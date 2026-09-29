"""Catalog of the sandbox custom GWAS runs under one release's library-green prefix.

The userresults pipeline writes each run to `<prefix><folder>/`. In R14 the folder is the
phenotype name; in R12 and R13 it is the run title and can hold several phenotypes' files,
so a phenotype name can only be resolved to its objects by listing the prefix. That
listing, refreshed in the background, is the whole of the catalog: nothing is copied,
converted or staged, and every read goes to the pipeline's own files.

Per folder the layout this module understands is

    <name>.gz, <name>.gz.tbi                       REGENIE summary stats (tabix-indexed)
    <name>_cases_controls.json                     R12/R13 per-phenotype sizes
    metadata.json                                  run metadata (one phenotype's, by `name`)
    hla/<name>.gz                                  classical HLA allele results (R14+, unindexed)
    finemap/susie/<name>.SUSIE.snp.filter.tsv      95% credible-set members
    finemap/susie/<name>.SUSIE.cred.summary.tsv    one row per credible set with its lead

Anything else in the folder (FINEMAP outputs, plots, autoreporting, coding_variants) is
ignored.
"""

import asyncio
import json
import logging
import threading
import time
from dataclasses import dataclass, replace
from typing import Any, Awaitable, Callable, Iterable

from app.config.custom_gwas import base_path, release_by_id, releases
from app.core.exceptions import NotFoundException
from app.services.gcloud_tabix_base import GCloudTabixBase

logger = logging.getLogger(__name__)

SUSIE_MEMBERS_SUFFIX = ".SUSIE.snp.filter.tsv"
SUSIE_SUMMARY_SUFFIX = ".SUSIE.cred.summary.tsv"
CASES_CONTROLS_SUFFIX = "_cases_controls.json"

# GCS lists at most 1000 objects per page
_LIST_PAGE_SIZE = "1000"
_METADATA_FETCH_CONCURRENCY = 32


@dataclass(frozen=True)
class CustomGwasPhenotype:
    name: str
    folder: str
    sumstats: str | None = None
    hla: str | None = None
    susie_members: str | None = None
    susie_summary: str | None = None
    metadata_path: str | None = None
    cases_controls_path: str | None = None
    # RFC 3339 `updated` of the summary-stats object: when the pipeline wrote the run
    written: str = ""


def parse_listing(
    base: str, objects: Iterable[str | tuple[str, str]]
) -> dict[str, CustomGwasPhenotype]:
    """Map the objects under `base` (names relative to it, optionally with their
    `updated` timestamp) to phenotypes.

    A phenotype is anything with a tabix-indexed `<name>.gz` at the top of a folder; an
    `.gz` without its `.tbi` sibling (a partial upload, or `coding_variants.txt.gz`) is not
    one. A phenotype name is unique per folder, not per release: a few percent of the R12
    and R13 names were run more than once, under different run titles. Of those the run
    titled after the phenotype wins, else the most recently written one — the same answer
    a user gets from the userresults browser, which shows the last upload.
    """
    per_folder: dict[str, dict[str, str]] = {}
    for obj in objects:
        name, updated = obj if isinstance(obj, tuple) else (obj, "")
        folder, sep, rest = name.partition("/")
        if not sep or not rest:
            continue
        per_folder.setdefault(folder, {})[rest] = updated

    phenotypes: dict[str, CustomGwasPhenotype] = {}
    written: dict[str, str] = {}

    def _preferred(name: str, folder: str, updated: str) -> bool:
        current = phenotypes.get(name)
        if current is None:
            return True
        if current.folder == name:
            return False
        return folder == name or updated > written[name]

    for folder in sorted(per_folder):
        files = per_folder[folder]
        folder_base = f"{base}{folder}/"
        folder_meta = f"{folder_base}metadata.json" if "metadata.json" in files else None
        found: dict[str, dict[str, Any]] = {}
        for f in files:
            if f.endswith(".gz") and "/" not in f and f"{f}.tbi" in files:
                found.setdefault(f[: -len(".gz")], {})["sumstats"] = folder_base + f
            elif f.startswith("hla/") and f.endswith(".gz") and f.count("/") == 1:
                found.setdefault(f[len("hla/") : -len(".gz")], {})["hla"] = folder_base + f
            elif f.startswith("finemap/susie/") and f.endswith(SUSIE_MEMBERS_SUFFIX):
                pheno = f[len("finemap/susie/") : -len(SUSIE_MEMBERS_SUFFIX)]
                found.setdefault(pheno, {})["susie_members"] = folder_base + f
            elif f.startswith("finemap/susie/") and f.endswith(SUSIE_SUMMARY_SUFFIX):
                pheno = f[len("finemap/susie/") : -len(SUSIE_SUMMARY_SUFFIX)]
                found.setdefault(pheno, {})["susie_summary"] = folder_base + f
            elif f.endswith(CASES_CONTROLS_SUFFIX) and "/" not in f:
                pheno = f[: -len(CASES_CONTROLS_SUFFIX)]
                found.setdefault(pheno, {})["cases_controls_path"] = folder_base + f
        for pheno, parts in found.items():
            if "sumstats" not in parts:
                continue
            updated = files.get(f"{pheno}.gz", "")
            if not _preferred(pheno, folder, updated):
                logger.info(
                    f"custom GWAS {pheno!r} also under {folder!r}; serving the run in "
                    f"{phenotypes[pheno].folder!r}"
                )
                continue
            if pheno in phenotypes:
                logger.info(
                    f"custom GWAS {pheno!r} also under {phenotypes[pheno].folder!r}; "
                    f"serving the run in {folder!r}"
                )
            written[pheno] = updated
            # the folder's metadata.json describes one phenotype, named inside it; the
            # name is not known before the file is read, so it is attached to every
            # phenotype of the folder here and the reader keeps it only where it matches
            phenotypes[pheno] = replace(
                CustomGwasPhenotype(name=pheno, folder=folder, written=updated),
                metadata_path=folder_meta,
                **parts,
            )

    return phenotypes


def metadata_row(
    phenotype: CustomGwasPhenotype,
    folder_metadata: dict | None,
    cases_controls: dict | None,
) -> dict[str, Any]:
    """The pheweb-shaped row the `custom_gwas` harmonizer reads for one phenotype.

    `date` is the day the pipeline wrote the run's summary statistics — the only
    provenance served: the pipeline's `submitter` fields are left out on purpose.
    """
    row: dict[str, Any] = {
        "phenocode": phenotype.name,
        "phenostring": phenotype.name,
        "date": phenotype.written[:10] or None,
        "num_cases": None,
        "num_controls": None,
        "pheno_coding": None,
        "has_credible_sets": phenotype.susie_members is not None
        and phenotype.susie_summary is not None,
        "has_hla": phenotype.hla is not None,
    }
    if folder_metadata and folder_metadata.get("name") == phenotype.name:
        description = (folder_metadata.get("description") or "").strip()
        row["phenostring"] = description or phenotype.name
        row["num_cases"] = folder_metadata.get("num_cases")
        row["num_controls"] = folder_metadata.get("num_controls")
        row["pheno_coding"] = folder_metadata.get("pheno_coding") or None
        row["analysis_type"] = folder_metadata.get("analysis_type")
    elif cases_controls:
        cases = cases_controls.get("cases")
        controls = cases_controls.get("controls")
        # a continuous trait's sidecar says cases 0 / controls 1, which is no sample size
        if isinstance(cases, int) and isinstance(controls, int) and cases + controls > 1:
            row["num_cases"] = cases
            row["num_controls"] = controls
    return row


class CustomGwasCatalog(GCloudTabixBase):
    """One release's listing, its per-phenotype metadata, and the paths they resolve to."""

    def __init__(self, release: dict):
        # GCloudTabixBase is initialised on first load(): its constructor fetches GCS
        # credentials, and the service container builds this object on the event loop
        self._initialized = False
        self.id: str = release["id"]
        self.resource: str = release["resource"]
        self.bucket: str = release["bucket"]
        self.prefix: str = release["prefix"]
        self.base: str = base_path(release)
        self.dataset_label: str = release["dataset_label"]
        self.gwas_dataset_id: str = release["gwas_dataset_id"]
        self.hla_dataset_id: str | None = release.get("hla_dataset_id")
        self.convention_fallback: bool = bool(release.get("convention_fallback"))
        self.refresh_seconds: int = int(release.get("refresh_seconds", 900))
        self._phenotypes: dict[str, CustomGwasPhenotype] = {}
        self._metadata_rows: dict[str, dict[str, Any]] = {}
        self._raw_json: dict[str, dict | None] = {}
        self.listed_at: float | None = None
        # set once the first listing attempt has finished, whichever way: the search index
        # is built on a worker thread while this loads on the loop, and it waits on this
        # rather than indexing an empty catalog
        self.ready = threading.Event()

    # ---- resolution -------------------------------------------------------------

    @property
    def loaded(self) -> bool:
        return self.listed_at is not None

    def phenotype(self, name: str) -> CustomGwasPhenotype | None:
        return self._phenotypes.get(name)

    def phenotype_names(self) -> list[str]:
        return sorted(self._phenotypes)

    def _convention(self, name: str, rel: str) -> str | None:
        return f"{self.base}{name}/{rel}" if self.convention_fallback else None

    def sumstats_path(self, name: str) -> str | None:
        """The summary-stats object for `name`, or the convention path a not-yet-listed
        run would have (None when neither applies). The caller HEAD-checks it."""
        p = self._phenotypes.get(name)
        if p is not None:
            return p.sumstats
        return self._convention(name, f"{name}.gz")

    def hla_path(self, name: str) -> str | None:
        p = self._phenotypes.get(name)
        if p is not None:
            return p.hla
        return self._convention(name, f"hla/{name}.gz")

    def susie_paths(self, name: str) -> tuple[str, str] | None:
        """(members, summary) for `name`; None when the run has no SuSiE output."""
        p = self._phenotypes.get(name)
        if p is not None:
            if p.susie_members and p.susie_summary:
                return p.susie_members, p.susie_summary
            return None
        members = self._convention(name, f"finemap/susie/{name}{SUSIE_MEMBERS_SUFFIX}")
        if members is None:
            return None
        return members, self._convention(name, f"finemap/susie/{name}{SUSIE_SUMMARY_SUFFIX}")

    def metadata_rows(self) -> list[dict[str, Any]]:
        """Every phenotype's metadata row; blocks (on a worker thread) until the first
        listing has finished, so startup callers never see the catalog half-built."""
        if not self.ready.wait(timeout=300):
            logger.error(f"custom GWAS catalog {self.id}: first listing not finished in 300s")
        return [self._metadata_rows[n] for n in sorted(self._metadata_rows)]

    # ---- loading ----------------------------------------------------------------

    async def _list_objects(self) -> list[tuple[str, str]]:
        """(name relative to the prefix, RFC 3339 `updated`) of every object under it."""
        objects: list[tuple[str, str]] = []
        params = {
            "prefix": self.prefix,
            "maxResults": _LIST_PAGE_SIZE,
            "fields": "nextPageToken,items(name,updated)",
        }
        page_token: str | None = None
        while True:
            if page_token:
                params["pageToken"] = page_token
            data = await self.storage.list_objects(self.bucket, params=params)
            if "error" in data:
                raise RuntimeError(f"listing gs://{self.bucket}/{self.prefix}: {data['error']}")
            for item in data.get("items", []):
                objects.append((item["name"][len(self.prefix) :], item.get("updated", "")))
            page_token = data.get("nextPageToken")
            if not page_token:
                return objects

    async def _read_json(self, path: str) -> dict | None:
        try:
            return json.loads(await self._fetch_full(path))
        except NotFoundException:
            return None
        except Exception as e:
            logger.warning(f"custom GWAS metadata {path} unreadable: {e}")
            return None

    def _ensure_initialized(self) -> None:
        if not self._initialized:
            GCloudTabixBase.__init__(self)
            self._initialized = True

    async def load(self) -> bool:
        """(Re)list the prefix and read the metadata of phenotypes not seen before.

        Returns whether the phenotype set changed. Metadata already read is kept, so a
        refresh costs one listing plus one small GET per new run.
        """
        self._ensure_initialized()
        started = time.time()
        phenotypes = parse_listing(self.base, await self._list_objects())

        wanted = {
            path
            for p in phenotypes.values()
            for path in (p.metadata_path, p.cases_controls_path)
            if path and path not in self._raw_json
        }
        sem = asyncio.Semaphore(_METADATA_FETCH_CONCURRENCY)

        async def _fetch(path: str) -> tuple[str, dict | None]:
            async with sem:
                return path, await self._read_json(path)

        for path, data in await asyncio.gather(*(_fetch(p) for p in sorted(wanted))):
            self._raw_json[path] = data

        rows = {
            name: metadata_row(
                p,
                self._raw_json.get(p.metadata_path) if p.metadata_path else None,
                self._raw_json.get(p.cases_controls_path) if p.cases_controls_path else None,
            )
            for name, p in phenotypes.items()
        }
        changed = set(rows) != set(self._phenotypes)
        # swapped as whole references: sync readers on other threads see either listing
        self._phenotypes = phenotypes
        self._metadata_rows = rows
        self.listed_at = time.time()
        self.ready.set()
        logger.info(
            f"custom GWAS catalog {self.id}: {len(phenotypes)} phenotypes "
            f"({len(wanted)} new metadata reads) in {self.listed_at - started:.1f}s"
        )
        return changed


ChangeCallback = Callable[[CustomGwasCatalog], Awaitable[None]]


class CustomGwasCatalogs:
    """Every configured release's catalog, loaded at startup and refreshed in the background."""

    def __init__(self):
        self.catalogs: dict[str, CustomGwasCatalog] = {
            r["id"]: CustomGwasCatalog(r) for r in releases
        }
        self._tasks: list[asyncio.Task] = []
        self._on_change: list[ChangeCallback] = []

    def get(self, catalog_id: str) -> CustomGwasCatalog:
        try:
            return self.catalogs[catalog_id]
        except KeyError:
            raise KeyError(
                f"unknown custom GWAS catalog {catalog_id!r}; "
                f"configured: {sorted(release_by_id)}"
            ) from None

    def on_change(self, callback: ChangeCallback) -> None:
        self._on_change.append(callback)

    async def warm_all(self) -> None:
        """List every release, then keep each listing fresh on its own interval.

        A release whose listing fails at startup is logged and served empty: its data
        endpoints still resolve names through the convention fallback where the release
        has one, and the next refresh retries the listing.
        """
        results = await asyncio.gather(
            *(c.load() for c in self.catalogs.values()), return_exceptions=True
        )
        for catalog, result in zip(self.catalogs.values(), results):
            if isinstance(result, BaseException):
                logger.error(f"custom GWAS catalog {catalog.id} failed to load: {result}")
                catalog.ready.set()
        self._tasks = [
            asyncio.create_task(self._refresh_loop(c), name=f"custom-gwas-refresh-{c.id}")
            for c in self.catalogs.values()
        ]

    async def _refresh_loop(self, catalog: CustomGwasCatalog) -> None:
        while True:
            await asyncio.sleep(catalog.refresh_seconds)
            try:
                if await catalog.load():
                    for callback in self._on_change:
                        await callback(catalog)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"custom GWAS catalog {catalog.id} refresh failed: {e}")

    async def cleanup(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._tasks = []
        for catalog in self.catalogs.values():
            await catalog.cleanup()


def get_catalog(catalog_id: str) -> CustomGwasCatalog:
    """The catalog a product config's `catalog: <id>` names, via the service container."""
    from app.core.service_container import container

    return container.get("custom_gwas_catalogs").get(catalog_id)
