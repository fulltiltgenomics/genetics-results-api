"""Protein structure release: the pointer, the manifest and the in-memory model index.

Every consumer check of the contract that can be made without touching the model objects
runs here, and a release that fails any of them is not served at all: `release` raises
ReleaseUnavailable and the routes answer 503.

A LOADED release is never swapped in-process; a new release needs a pod restart. Clients
(view-specs in chat history, the SDK) hold model file ids across requests, and a release is
validated where an operator sees the verdict: the startup check's warning at boot. A swap
mid-life would change what those ids resolve to with nobody looking. An UNLOADED service is
different: a load that failed at boot (a transient GCS error, or a pointer written later) is
retried every `retry_seconds` until one succeeds, and then never again.

The size of each structure and PAE object is checked against the index where it is served:
a range response carries the object's total size, so the comparison costs nothing there,
whereas checking every object here would cost one request per model file at every boot.

The index keeps plain tuples. It drops the four sha256 columns (results-api checks sizes and
leaves hashes to the build) and the two object paths (they are derived from the id and
checked equal at load), and shares repeated strings. Measured on 22,000 synthetic rows: about
16 MB retained, 24 MB at the parse's peak; another kept string column adds roughly 2 MB.
"""

import asyncio
import gzip
import hashlib
import io
import json
import logging
import zlib
from dataclasses import dataclass
from typing import Awaitable, Callable, NamedTuple

import app.config.protein_structure as config
import app.config.protein_structure_contract as contract
from app.core.exceptions import NotFoundException
from app.services.gcloud_tabix_base import GCloudTabixBase, _get_fetch_semaphore

logger = logging.getLogger(__name__)

_SHA256_HEX_LEN = 64

# a real release has ~22,000 model rows; over twice that is headroom, not a plausible release.
# The models and manifest byte caps are derived from it, so raising it moves them together
MAX_MODEL_ROWS = 50_000
# a real row is ~600 bytes, most of it four sha256 hex strings; the header line is ~350
MAX_LINE_BYTES = 4096
# the four hashes are incompressible (~128 bytes of entropy), the rest compresses well
MAX_MODELS_GZ_BYTES = MAX_MODEL_ROWS * 512
# the manifest records bytes and sha256 per object, and there are at most two objects per row
MAX_MANIFEST_BYTES = MAX_MODEL_ROWS * 2 * 256
# a pointer is five short JSON fields, ~250 bytes
MAX_POINTER_BYTES = 4096
# release content quoted in a message or a log line
_CLIP = 120


def _clip(value: object) -> str:
    text = repr(value)
    return text if len(text) <= _CLIP else text[:_CLIP] + f"... ({len(text)} chars)"


class ReleaseUnavailable(RuntimeError):
    """No release is loaded: the pointer is unset, unreadable, or names a release that failed a check."""


class ContractCheckFailed(ValueError):
    """A release refused by one named consumer check."""

    def __init__(self, check: str, detail: str):
        super().__init__(f"{check}: {detail}")
        self.check = check


@dataclass(frozen=True)
class Pointer:
    release: str
    uri: str
    contract_version: int
    manifest_sha256: str

    @property
    def base(self) -> str:
        return self.uri.rstrip("/")


class Model(NamedTuple):
    model_file_id: str
    accession: str
    uniprot_isoform: str
    gene_id: str | None
    gene_name: str | None
    source_gene_symbol: str | None
    source: str
    source_label: str
    model_version: str
    licence: str
    window_index: int
    n_windows: int
    protein_start: int
    protein_end: int
    n_residues: int
    owned_start: int
    owned_end: int
    has_pae: bool
    structure_bytes: int
    pae_bytes: int | None

    @property
    def gene_ids(self) -> tuple[str, ...]:
        """gene_id is one id, or a comma-joined list when the accession sits under several genes."""
        return tuple(self.gene_id.split(",")) if self.gene_id else ()

    @property
    def structure_path(self) -> str:
        return contract.structure_path(self.model_file_id)

    @property
    def pae_path(self) -> str | None:
        return contract.pae_path(self.model_file_id) if self.has_pae else None


@dataclass(frozen=True)
class Release:
    pointer: Pointer
    manifest: dict
    models: dict[str, Model]
    # model file ids in window order, which is protein order
    by_accession: dict[str, tuple[str, ...]]
    # unversioned Ensembl gene id -> accessions, a model under several genes being listed under
    # each; rows without a gene_id are not reachable by gene
    by_gene: dict[str, tuple[str, ...]]

    @property
    def name(self) -> str:
        return self.pointer.release

    def object_path(self, relative: str) -> str:
        return f"{self.pointer.base}/{relative}"

    def models_of(self, accession: str) -> tuple[Model, ...]:
        return tuple(self.models[mid] for mid in self.by_accession.get(accession, ()))


def parse_pointer(raw: bytes) -> Pointer:
    try:
        doc = json.loads(raw)
    except ValueError as e:
        raise ContractCheckFailed("pointer", f"not JSON: {e}")
    if not isinstance(doc, dict):
        raise ContractCheckFailed("pointer", "not a JSON object")
    release, uri, version, sha = (
        doc.get("release"), doc.get("uri"), doc.get("contract_version"), doc.get("manifest_sha256")
    )
    if not isinstance(release, str) or not release:
        raise ContractCheckFailed("pointer", "no 'release'")
    if not isinstance(uri, str) or not uri.startswith("gs://"):
        raise ContractCheckFailed("pointer", f"'uri' is not a gs:// location: {_clip(uri)}")
    if not isinstance(sha, str) or len(sha) != _SHA256_HEX_LEN:
        raise ContractCheckFailed("pointer", "no 'manifest_sha256'")
    # bool is an int subclass; `true` is not version 1
    if type(version) is not int or version != contract.CONTRACT_VERSION:
        raise ContractCheckFailed(
            "contract_version",
            f"pointer names contract {_clip(version)}, this server implements {contract.CONTRACT_VERSION}",
        )
    return Pointer(release=release, uri=uri, contract_version=version, manifest_sha256=sha.lower())


def parse_manifest(pointer: Pointer, raw: bytes) -> dict:
    actual = hashlib.sha256(raw).hexdigest()
    if actual != pointer.manifest_sha256:
        raise ContractCheckFailed(
            "manifest_sha256", f"manifest hashes to {actual}, the pointer says {pointer.manifest_sha256}"
        )
    try:
        doc = json.loads(raw)
    except ValueError as e:
        raise ContractCheckFailed("manifest", f"not JSON: {e}")
    version = doc.get("contract_version") if isinstance(doc, dict) else None
    if type(version) is not int or version != contract.CONTRACT_VERSION:
        raise ContractCheckFailed(
            "contract_version",
            f"manifest names contract {_clip(version)}, this server implements {contract.CONTRACT_VERSION}",
        )
    return doc


_COLUMN_INDEX = {name: i for i, (name, _) in enumerate(contract.MODEL_COLUMNS)}


def _field(f: list[str], name: str) -> str:
    return f[_COLUMN_INDEX[name]]


def _int(f: list[str], name: str) -> int:
    return int(_field(f, name))


def _model(f: list[str], shared: dict[str, str]) -> Model:
    def s(name: str) -> str:
        v = _field(f, name)
        return shared.setdefault(v, v)

    def opt(name: str) -> str | None:
        return s(name) or None

    mid = _field(f, "model_file_id")
    # the only guard between a release row and an object path or URL: fullmatch, because `$`
    # also matches before a trailing newline
    if not contract.MODEL_FILE_ID_RE.fullmatch(mid):
        raise ValueError(f"model file id outside the allowed charset: {_clip(mid)}")
    if _field(f, "structure_path") != contract.structure_path(mid):
        raise ValueError(f"{mid}: structure_path is not {contract.structure_path(mid)}")
    has_pae = {"true": True, "false": False}[_field(f, "has_pae")]
    n_residues = _int(f, "n_residues")
    try:
        genes = contract.split_model_genes(_field(f, "gene_id"), _field(f, "gene_name"))
    except ValueError as e:
        raise ValueError(f"{mid}: {e}")
    # an empty id inside a list would be indexed as a gene
    if not all(gene_id for gene_id, _ in genes):
        raise ValueError(f"{mid}: an empty gene id in {_clip(_field(f, 'gene_id'))}")
    if has_pae:
        if _field(f, "pae_path") != contract.pae_path(mid):
            raise ValueError(f"{mid}: pae_path is not {contract.pae_path(mid)}")
        pae_bytes = _int(f, "pae_bytes")
        # one uint8 per residue pair: a size that is not n^2 cannot be sliced into rows
        if pae_bytes != n_residues * n_residues:
            raise ValueError(f"{mid}: pae_bytes {pae_bytes} is not n_residues^2 ({n_residues}^2)")
    else:
        if any(_field(f, c) for c in ("pae_path", "pae_sha256", "pae_bytes")):
            raise ValueError(f"{mid}: has_pae is false but a pae column is set")
        pae_bytes = None
    return Model(
        model_file_id=mid,
        accession=s("accession"),
        uniprot_isoform=s("uniprot_isoform"),
        gene_id=opt("gene_id"),
        gene_name=opt("gene_name"),
        source_gene_symbol=opt("source_gene_symbol"),
        source=s("source"),
        source_label=s("source_label"),
        model_version=s("model_version"),
        licence=s("licence"),
        window_index=_int(f, "window_index"),
        n_windows=_int(f, "n_windows"),
        protein_start=_int(f, "protein_start"),
        protein_end=_int(f, "protein_end"),
        n_residues=n_residues,
        owned_start=_int(f, "owned_start"),
        owned_end=_int(f, "owned_end"),
        has_pae=has_pae,
        structure_bytes=_int(f, "structure_bytes"),
        pae_bytes=pae_bytes,
    )


def parse_models(
    raw: bytes, served_licences: frozenset[str] | None
) -> tuple[dict[str, Model], dict[str, tuple[str, ...]], dict[str, tuple[str, ...]]]:
    expected_header = contract.header_line(contract.MODEL_COLUMNS)
    n_columns = len(contract.MODEL_COLUMNS)
    models: dict[str, Model] = {}
    shared: dict[str, str] = {}
    stream = gzip.GzipFile(fileobj=io.BytesIO(raw))

    def readline(n: int) -> str:
        # bounded: an unbounded readline holds a whole decompressed line, however long
        line = stream.readline(MAX_LINE_BYTES + 1)
        if len(line) > MAX_LINE_BYTES:
            raise ValueError(f"line {n} is longer than {MAX_LINE_BYTES} bytes")
        return line.decode("utf-8")

    try:
        header = readline(1)
    except (OSError, EOFError, ValueError, zlib.error) as e:
        raise ContractCheckFailed("models_header", f"{contract.MODELS_FILE}: {_clip(str(e))}")
    if header != expected_header:
        raise ContractCheckFailed(
            "models_header", f"{contract.MODELS_FILE} header {_clip(header)} is not the contract's"
        )
    try:
        n = 1
        while line := readline(n + 1):
            n += 1
            if n - 1 > MAX_MODEL_ROWS:
                raise ValueError(f"more than {MAX_MODEL_ROWS} rows")
            f = line.rstrip("\r\n").split("\t")
            if len(f) != n_columns:
                raise ValueError(f"line {n} has {len(f)} fields, not {n_columns}")
            m = _model(f, shared)
            if m.model_file_id in models:
                raise ValueError(f"model file id {m.model_file_id} appears twice")
            models[m.model_file_id] = m
    except (ValueError, KeyError, OSError, EOFError, zlib.error) as e:
        raise ContractCheckFailed("models_rows", f"{contract.MODELS_FILE}: {_clip(str(e))}")

    windows: dict[str, list[Model]] = {}
    for m in models.values():
        windows.setdefault(m.accession, []).append(m)
    refused = {
        a for a, ms in windows.items()
        if served_licences is not None and any(m.licence not in served_licences for m in ms)
    }
    by_accession: dict[str, tuple[str, ...]] = {}
    genes: dict[str, dict[str, None]] = {}
    for accession, ms in windows.items():
        ms.sort(key=lambda m: m.window_index)
        if [m.window_index for m in ms] != list(range(1, len(ms) + 1)) or any(
            m.n_windows != len(ms) for m in ms
        ):
            raise ContractCheckFailed(
                "models_rows", f"{accession}: window_index/n_windows do not number its {len(ms)} files 1..n"
            )
        if accession in refused:
            for m in ms:
                del models[m.model_file_id]
            continue
        by_accession[accession] = tuple(m.model_file_id for m in ms)
        for m in ms:
            for gene_id in m.gene_ids:
                genes.setdefault(gene_id, {})[accession] = None
    if refused:
        logger.info(f"protein structure: {len(refused)} accessions left out for an unserved licence")
    return models, by_accession, {g: tuple(a) for g, a in genes.items()}


def release_paths(pointer: Pointer) -> dict[str, str]:
    return {
        name: f"{pointer.base}/{name}"
        for name in (contract.MANIFEST_FILE, contract.MODELS_FILE)
    }


# (path, max_bytes): the whole object when it is within max_bytes, else its first max_bytes + 1
# bytes, which is enough to refuse it. Never fewer bytes than that for any other reason
Fetch = Callable[[str, int], Awaitable[bytes]]

_READ_CHUNK = 64 * 1024


async def read_capped(content, max_bytes: int) -> bytes:
    """An aiohttp body read to its end, or to max_bytes + 1 bytes when it is longer.

    StreamReader.read(n) returns whatever has arrived, up to n: one call is a prefix of the
    body, not the body, so the stream is drained until it ends or passes the cap.
    """
    buf = bytearray()
    async for chunk in content.iter_chunked(_READ_CHUNK):
        buf += chunk
        if len(buf) > max_bytes:
            return bytes(buf[: max_bytes + 1])
    return bytes(buf)


def check_models_object(manifest: dict, raw: bytes) -> None:
    """The fetched models.tsv.gz against the manifest's entry for it: a short or altered read is refused."""
    objects = manifest.get(contract.MANIFEST_OBJECTS)
    entry = objects.get(contract.MODELS_FILE) if isinstance(objects, dict) else None
    if not isinstance(entry, dict):
        raise ContractCheckFailed("models_object", f"the manifest has no entry for {contract.MODELS_FILE}")
    size, sha = entry.get(contract.OBJECT_BYTES), entry.get(contract.OBJECT_SHA256)
    if type(size) is not int or not isinstance(sha, str):
        raise ContractCheckFailed("models_object", f"the manifest's entry for {contract.MODELS_FILE} is malformed")
    if len(raw) != size:
        raise ContractCheckFailed(
            "models_object", f"{contract.MODELS_FILE}: read {len(raw)} bytes, the manifest says {size}"
        )
    actual = hashlib.sha256(raw).hexdigest()
    if actual != sha.lower():
        raise ContractCheckFailed(
            "models_object", f"{contract.MODELS_FILE} hashes to {actual}, the manifest says {_clip(sha)}"
        )


# a bgzf block is at most this long, so this many bytes hold a tabix file's whole first block
_BGZF_MAX_BLOCK = 64 * 1024

# (file, its columns, the check a differing header fails)
TABIX_HEADERS = (
    (contract.RESIDUES_FILE, contract.RESIDUE_COLUMNS, "residues_header"),
    (contract.FEATURES_FILE, contract.FEATURE_COLUMNS, "features_header"),
    (contract.VARIANTS_FILE, contract.VARIANT_COLUMNS, "variants_header"),
)


def check_tabix_header(check: str, name: str, columns: list[tuple[str, type]], head: bytes) -> None:
    """The first line of a bgzf file, given its leading bytes, against the contract's header."""
    try:
        # 47: gzip framing, largest window. Only the first member is inflated, and only as far
        # as one bounded line
        first = zlib.decompressobj(47).decompress(head, MAX_LINE_BYTES + 1)
    except zlib.error as e:
        raise ContractCheckFailed(check, f"{name}: {_clip(str(e))}")
    # the newline is appended here, not read: a first block that ends at the header without its
    # newline passes whatever the next block holds
    line = first.split(b"\n", 1)[0] + b"\n"
    if line != contract.header_line(columns).encode():
        raise ContractCheckFailed(check, f"{name} header {_clip(line[:MAX_LINE_BYTES])} is not the contract's")


async def load_release(pointer_uri: str, fetch: Fetch, served_licences: frozenset[str] | None) -> Release:
    """Read and check the release `pointer_uri` names. Raises ContractCheckFailed naming the check."""

    async def read(path: str, max_bytes: int) -> bytes:
        try:
            raw = await fetch(path, max_bytes)
        except Exception as e:
            raise ContractCheckFailed("object_missing", f"cannot read {path}: {_clip(str(e))}")
        if len(raw) > max_bytes:
            raise ContractCheckFailed("object_too_large", f"{path} is over {max_bytes} bytes")
        return raw

    pointer = parse_pointer(await read(pointer_uri, MAX_POINTER_BYTES))
    paths = release_paths(pointer)
    manifest = parse_manifest(pointer, await read(paths[contract.MANIFEST_FILE], MAX_MANIFEST_BYTES))
    models_raw = await read(paths[contract.MODELS_FILE], MAX_MODELS_GZ_BYTES)
    check_models_object(manifest, models_raw)
    models, by_accession, by_gene = await asyncio.to_thread(parse_models, models_raw, served_licences)
    for name, columns, check in TABIX_HEADERS:
        path = f"{pointer.base}/{name}"
        try:
            # deliberately not `read`: the file is far over this many bytes and only its head is wanted
            head = await fetch(path, _BGZF_MAX_BLOCK - 1)
        except Exception as e:
            raise ContractCheckFailed("object_missing", f"cannot read {path}: {_clip(str(e))}")
        check_tabix_header(check, name, columns, head)
    return Release(pointer=pointer, manifest=manifest, models=models, by_accession=by_accession, by_gene=by_gene)


class ProteinStructureAccess(GCloudTabixBase):
    """Holds the release the pointer named, or none while no load has succeeded."""

    # the CELLxGENE service's refresh interval: a missed boot costs at most this much uptime
    retry_seconds: float = 900

    def __init__(self) -> None:
        super().__init__()
        self.pointer_uri: str | None = config.release_pointer
        self._release: Release | None = None
        self._task: asyncio.Task | None = None

    @property
    def configured(self) -> bool:
        return self.pointer_uri is not None

    @property
    def release(self) -> Release:
        release = self._release
        if release is None:
            raise ReleaseUnavailable("no protein structure release is loaded")
        return release

    async def _fetch_capped(self, gs_path: str, max_bytes: int) -> bytes:
        """At most max_bytes + 1 bytes of an object: enough to refuse one over the cap unread."""
        async with _get_fetch_semaphore():
            headers = {**await self.storage._headers(), "Range": f"bytes=0-{max_bytes}"}
            async with self.session.get(self._gcs_url(gs_path), headers=headers) as resp:
                if resp.status == 404:
                    raise NotFoundException(f"File not found: {gs_path}")
                # an empty object cannot satisfy any range
                if resp.status == 416:
                    return b""
                resp.raise_for_status()
                return await read_capped(resp.content, max_bytes)

    async def _load(self) -> bool:
        try:
            self._release = await load_release(self.pointer_uri, self._fetch_capped, config.served_licences)
        except Exception as e:
            logger.error(
                f"protein structure: release from {self.pointer_uri} not served, "
                f"retrying every {self.retry_seconds}s: {e}"
            )
            return False
        r = self._release
        logger.info(
            f"protein structure: serving release {r.name} ({r.pointer.uri}): {len(r.models)} model files, "
            f"{len(r.by_accession)} accessions, {len(r.by_gene)} genes"
        )
        return True

    async def warm_all(self) -> None:
        """Load the release; on failure log loudly and keep retrying in the background. Never raises."""
        if not self.configured:
            return
        if not await self._load():
            self._task = asyncio.create_task(self._retry_until_loaded(), name="protein-structure-retry")

    async def _retry_until_loaded(self) -> None:
        while True:
            await asyncio.sleep(self.retry_seconds)
            if await self._load():
                self._task = None
                return

    async def cleanup(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        await super().cleanup()
