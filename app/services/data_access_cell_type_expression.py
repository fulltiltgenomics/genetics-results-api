"""Cell-type expression (CELLxGENE) data access: a release that is swapped under a running server.

The release directory is located by a small JSON pointer object. A background loop re-reads
the pointer; when it names a different release, that release is warmed COMPLETELY (headers,
tabix indexes, a real range read, the measured-genes table) before it replaces the served
one. A pointer that is unreadable, or a release that fails to warm, leaves the old release
serving. Tabix indexes are cached by path, so serving a release whose index had not been
loaded would put the index download on a request.
"""

import asyncio
import gzip
import io
import json
import logging
from dataclasses import dataclass
from typing import AsyncGenerator

import app.config.cell_type_expression as config
from app.services.gcloud_tabix_base import GCloudTabixBase

logger = logging.getLogger(__name__)

# a real range read on each file during warm-up: the first gene of chr1 sits in the first
# bgzf blocks, so this fetches and inflates real data without reading a meaningful amount
_WARM_CHROM, _WARM_START, _WARM_END = 1, 1, 100_000


class ReleaseUnavailable(RuntimeError):
    """No release has been loaded yet (the pointer was never readable, or never warmed)."""


@dataclass(frozen=True)
class Pointer:
    release: str
    uri: str

    @property
    def base(self) -> str:
        return self.uri.rstrip("/")


def parse_pointer(raw: bytes) -> Pointer:
    """The `releases/CURRENT` object: only `release` and `uri` are read, the rest is for people."""
    doc = json.loads(raw)
    if not isinstance(doc, dict):
        raise ValueError("release pointer is not a JSON object")
    release, uri = doc.get("release"), doc.get("uri")
    if not isinstance(release, str) or not release:
        raise ValueError("release pointer has no 'release'")
    if not isinstance(uri, str) or not uri.startswith("gs://"):
        raise ValueError(f"release pointer 'uri' is not a gs:// location: {uri!r}")
    return Pointer(release=release, uri=uri)


def release_paths(pointer: Pointer) -> dict[str, str]:
    return {
        name: f"{pointer.base}/{name}"
        for name in (
            config.SUMMARY_FILE,
            config.PER_DATASET_FILE,
            config.MEASURED_GENES_FILE,
            config.DATASETS_FILE,
            config.GROUPS_FILE,
            config.MANIFEST_FILE,
        )
    }


@dataclass(frozen=True)
class Release:
    pointer: Pointer
    summary: str
    per_dataset: str
    # unversioned ENSG id -> bitmask over `dataset_ids` of the datasets that measured it
    measured: dict[str, int]
    dataset_ids: tuple[str, ...]
    # datasets with at least one group that passes the release's suppression thresholds
    kept_datasets: frozenset[str]

    @property
    def name(self) -> str:
        return self.pointer.release

    def measuring_datasets(self, ensg: str) -> list[str]:
        mask = self.measured.get(ensg, 0)
        return [
            d for i, d in enumerate(self.dataset_ids) if mask >> i & 1 and d in self.kept_datasets
        ]


def parse_measured(measured_raw: bytes, datasets_raw: bytes) -> tuple[dict[str, int], tuple[str, ...]]:
    """measured_genes.tsv.gz keys datasets by dataset_version_id, the per-dataset file by
    dataset_id; datasets.tsv is the bridge. A gene or dataset the bridge cannot place means
    the release is internally inconsistent, which is refused rather than guessed at."""
    rows = [l.split("\t") for l in datasets_raw.decode().splitlines()]
    head = rows[0]
    id_col, version_col = head.index("dataset_id"), head.index("dataset_version_id")
    version_to_id: dict[str, str] = {}
    for r in rows[1:]:
        if len(r) <= max(id_col, version_col) or not r[id_col] or not r[version_col]:
            raise ValueError(f"datasets.tsv row without dataset_id/dataset_version_id: {r!r}")
        version_to_id[r[version_col]] = r[id_col]
    dataset_ids = tuple(dict.fromkeys(version_to_id.values()))
    bit = {d: 1 << i for i, d in enumerate(dataset_ids)}

    # line by line: the file is tens of millions of rows at full scale, and decompressing and
    # splitting it whole would hold several GB at once
    measured: dict[str, int] = {}
    with gzip.open(io.BytesIO(measured_raw), "rt", newline="") as lines:
        if next(lines, "").rstrip("\r\n").split("\t") != ["dataset_version_id", "gene_id"]:
            raise ValueError("measured_genes.tsv.gz has an unexpected header")
        for line in lines:
            version, gene = line.rstrip("\r\n").split("\t")
            try:
                b = bit[version_to_id[version]]
            except KeyError:
                raise ValueError(f"measured_genes.tsv.gz names dataset {version!r} absent from datasets.tsv")
            ensg = gene.split(".", 1)[0]
            measured[ensg] = measured.get(ensg, 0) | b
    return measured, dataset_ids


def parse_kept_datasets(groups_raw: bytes, manifest_raw: bytes) -> frozenset[str]:
    """Datasets with at least one group that publish.py would emit rows for.

    groups.tsv.gz lists EVERY group, suppressed or not, so the rule publish.py applies to
    per-dataset rows is re-applied here with the thresholds the release recorded:
    `emit = n_cells >= min_cells and n_donors >= min_donors`. A release that does not record
    them is refused, because guessing them would silently misclassify datasets.
    """
    thresholds = json.loads(manifest_raw).get("thresholds")
    if not isinstance(thresholds, dict) or not all(
        isinstance(thresholds.get(k), int) for k in ("min_cells", "min_donors")
    ):
        raise ValueError("manifest.json has no usable 'thresholds' (min_cells, min_donors)")
    min_cells, min_donors = thresholds["min_cells"], thresholds["min_donors"]
    kept: set[str] = set()
    with gzip.open(io.BytesIO(groups_raw), "rt", newline="") as lines:
        head = next(lines, "").rstrip("\r\n").split("\t")
        try:
            dataset, cells, donors = (head.index(c) for c in ("dataset_id", "n_cells", "n_donors"))
        except ValueError:
            raise ValueError("groups.tsv.gz lacks dataset_id/n_cells/n_donors columns")
        for line in lines:
            f = line.rstrip("\r\n").split("\t")
            if int(f[cells]) >= min_cells and int(f[donors]) >= min_donors:
                kept.add(f[dataset])
    return frozenset(kept)


class CellTypeExpressionAccess(GCloudTabixBase):
    """Serves the current release; `warm_all` loads it and starts the pointer refresh loop."""

    def __init__(self) -> None:
        super().__init__()
        self.pointer_uri: str | None = config.release_pointer
        self.refresh_seconds: int = config.refresh_seconds
        self._release: Release | None = None
        self._task: asyncio.Task | None = None

    @property
    def configured(self) -> bool:
        return self.pointer_uri is not None

    @property
    def release(self) -> Release:
        release = self._release
        if release is None:
            raise ReleaseUnavailable("no cell-type expression release is loaded")
        return release

    async def warm_all(self) -> None:
        """Load the current release, then keep re-reading the pointer.

        A failure is logged and swallowed like every warm_all, and the loop starts regardless.
        It retries every `refresh_seconds`, so a pointer that is absent or unreadable at boot
        (the route answers 503 meanwhile) is picked up once it appears and its release warms.
        """
        if not self.configured:
            return
        try:
            await self.refresh()
        except Exception as e:
            logger.error(f"cell-type expression: initial release load failed: {e}")
        self._task = asyncio.create_task(self._refresh_loop(), name="cell-type-expression-refresh")

    async def _refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(self.refresh_seconds)
            try:
                await self.refresh()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                kept = self._release.name if self._release else "none"
                logger.error(f"cell-type expression: refresh failed, still serving {kept}: {e}")

    async def refresh(self) -> bool:
        """Switch to the release the pointer names if it is new. True when the served release changed.

        Raises when the pointer is unreadable or the new release does not warm; in both cases
        `_release` is untouched.
        """
        pointer = parse_pointer(await self._fetch_full(self.pointer_uri))
        current = self._release
        if current is not None and current.pointer == pointer:
            return False
        candidate = await self._warm_release(pointer)
        self._release = candidate
        logger.info(f"cell-type expression: now serving release {pointer.release} ({pointer.uri})")
        return True

    async def _warm_release(self, pointer: Pointer) -> Release:
        paths = release_paths(pointer)
        tables = {
            paths[config.SUMMARY_FILE]: config.summary_columns,
            paths[config.PER_DATASET_FILE]: config.per_dataset_columns,
        }

        async def warm_table(path: str, columns: list[str]) -> None:
            header = [h.decode() for h in await self._get_header_async(path)]
            if header != columns:
                raise ValueError(f"{path}: header {header} is not the expected {columns}")
            await self._get_index(path)
            stream = await self._stream_range(path, [_WARM_CHROM], [_WARM_START], [_WARM_END], 0)
            async for _ in stream:
                pass

        async def load_measured() -> tuple[dict[str, int], tuple[str, ...]]:
            measured_raw, datasets_raw = await asyncio.gather(
                self._fetch_full(paths[config.MEASURED_GENES_FILE]),
                self._fetch_full(paths[config.DATASETS_FILE]),
            )
            return await asyncio.to_thread(parse_measured, measured_raw, datasets_raw)

        async def load_kept() -> frozenset[str]:
            groups_raw, manifest_raw = await asyncio.gather(
                self._fetch_full(paths[config.GROUPS_FILE]),
                self._fetch_full(paths[config.MANIFEST_FILE]),
            )
            return await asyncio.to_thread(parse_kept_datasets, groups_raw, manifest_raw)

        _, (measured, dataset_ids), kept = await asyncio.gather(
            asyncio.gather(*(warm_table(p, c) for p, c in tables.items())),
            load_measured(),
            load_kept(),
        )
        return Release(
            pointer=pointer,
            summary=paths[config.SUMMARY_FILE],
            per_dataset=paths[config.PER_DATASET_FILE],
            measured=measured,
            dataset_ids=dataset_ids,
            kept_datasets=kept,
        )

    async def cleanup(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        await super().cleanup()

    async def _rows(self, path: str, genes: list[dict]) -> AsyncGenerator[list[bytes], None]:
        """Rows of `path` belonging to the given genes, split into fields.

        The files are point-indexed on gene_start, so a lookup returns every gene that STARTS
        at that position; the gene_id column picks out the requested one.
        """
        wanted = {g["ensg"] for g in genes}
        starts = sorted({(g["chrom"], g["gene_start"]) for g in genes})
        stream = await self._stream_range(
            path, [c for c, _ in starts], [s for _, s in starts], [s for _, s in starts], 0
        )
        pending = b""
        async for chunk in stream:
            pending += chunk
            *lines, pending = pending.split(b"\n")
            for line in lines:
                fields = line.split(b"\t")
                if fields[0].split(b".", 1)[0].decode() in wanted:
                    yield fields
        if pending:
            fields = pending.split(b"\t")
            if fields[0].split(b".", 1)[0].decode() in wanted:
                yield fields

    async def stream_summary(self, release: Release, genes: list[dict]) -> AsyncGenerator[bytes, None]:
        header = ("\t".join(config.summary_columns) + "\n").encode()

        async def gen() -> AsyncGenerator[bytes, None]:
            yield header
            async for fields in self._rows(release.summary, genes):
                yield b"\t".join(fields) + b"\n"

        return gen()

    async def stream_per_dataset(self, release: Release, genes: list[dict]) -> AsyncGenerator[bytes, None]:
        """Stored rows, then one placeholder row per (gene, dataset) that measured the gene but has none.

        The file is sparse: only (group, gene) pairs with an expressing cell have a row, and only
        for groups that pass the suppression thresholds. So for a dataset with at least one such
        group, a measured gene without a row has no expressing cell in any of them: mean and
        fraction are 0. The placeholder stands for that dataset, not for a group, so the
        pseudobulk (a per-group quantity with its own donor rule) is NA. A dataset whose every
        group is suppressed says nothing about the gene, and a dataset that did not measure it
        does not either; neither gets a row.
        """
        columns = config.per_dataset_columns
        dataset_col = columns.index("dataset_id")
        header = ("\t".join(columns + [config.STATUS_COLUMN]) + "\n").encode()
        present = config.STATUS_PRESENT.encode()

        async def gen() -> AsyncGenerator[bytes, None]:
            yield header
            seen: dict[str, set[str]] = {}
            async for fields in self._rows(release.per_dataset, genes):
                ensg = fields[0].split(b".", 1)[0].decode()
                seen.setdefault(ensg, set()).add(fields[dataset_col].decode())
                yield b"\t".join(fields + [present]) + b"\n"
            for g in genes:
                for dataset_id in release.measuring_datasets(g["ensg"]):
                    if dataset_id in seen.get(g["ensg"], ()):
                        continue
                    yield (self._placeholder(g, dataset_id) + "\n").encode()

        return gen()

    @staticmethod
    def _placeholder(gene: dict, dataset_id: str) -> str:
        """A measured gene with no stored row in a dataset that has kept groups: mean and fraction are 0.

        The pseudobulk is NA (its donor rule is per group) as is every group attribute.
        """
        values = {
            "gene_id": gene["gene_id"], "chrom": str(gene["chrom"]),
            "gene_start": str(gene["gene_start"]), "gene_end": str(gene["gene_end"]),
            "gene_name": gene["gene_name"], "dataset_id": dataset_id,
            "mean_log1p_cp10k": "0", "frac_expressing": "0",
            config.STATUS_COLUMN: config.STATUS_MEASURED_NO_ROW,
        }
        return "\t".join(values.get(c) or "NA" for c in config.per_dataset_columns + [config.STATUS_COLUMN])

