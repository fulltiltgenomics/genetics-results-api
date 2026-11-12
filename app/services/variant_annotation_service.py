import logging
from typing import AsyncGenerator

import app.config.common as config
import app.config.protein_structure_contract as protein_contract
from app.core.variant import Variant
from app.services.gcloud_tabix_base import GCloudTabixBase
from app.services.protein_structure import ProteinStructureAccess

logger = logging.getLogger(__name__)

# a queried variant carries its chromosome as a number, while a source file may spell
# the sex chromosomes either way; rows are matched on the numeric form
_SEX_CHROM_CODES = {b"X": b"23", b"Y": b"24"}

# the variant-to-residue map of the protein structure release. Its file is a member of whichever
# release is loaded, so no profile names a path for it
PROTEIN_SOURCE = "protein"
_PROTEIN_COLUMNS = [c for c, _ in protein_contract.VARIANT_COLUMNS]
_PROTEIN_ACCESSION_COL = _PROTEIN_COLUMNS.index("accession")


class VariantAnnotationService(GCloudTabixBase):
    # default cpra (chr/pos/ref/alt) column indices when a source omits "cpra_cols"
    _DEFAULT_CPRA_COLS = (1, 2, 3, 4)

    def __init__(self, protein_structure: ProteinStructureAccess | None = None) -> None:
        super().__init__()
        self._sources = dict(config.variant_annotation_sources)
        self._headers: dict[str, list[bytes]] = {}
        # the source exists wherever a release pointer is configured, loaded or not, so that a
        # release that failed to load answers "unavailable" rather than "unknown source"
        self._protein_structure = (
            protein_structure if protein_structure is not None and protein_structure.configured else None
        )
        if self._protein_structure is not None:
            self._sources[PROTEIN_SOURCE] = {
                "cpra_cols": [_PROTEIN_COLUMNS.index(c) for c in ("chrom", "pos", "ref", "alt")]
            }

    def _from_release(self, source: str) -> bool:
        return source == PROTEIN_SOURCE and self._protein_structure is not None

    def require_available(self, source: str) -> None:
        """Raises ReleaseUnavailable when the source reads a release and none is loaded."""
        if self._from_release(source):
            self._protein_structure.release

    def _file(self, source: str) -> str:
        if self._from_release(source):
            return self._protein_structure.release.object_path(protein_contract.VARIANTS_FILE)
        return self._sources[source]["file"]

    def _cpra_cols(self, source: str) -> tuple[int, int, int, int]:
        return tuple(self._sources[source].get("cpra_cols", self._DEFAULT_CPRA_COLS))

    def get_available_sources(self) -> list[str]:
        return list(self._sources.keys())

    def get_version(self, source: str) -> str | None:
        if self._from_release(source):
            return self._protein_structure.release.name
        return self._sources[source].get("version")

    def get_header(self, source: str) -> list[bytes]:
        # the release's header was compared with these columns when it loaded
        if self._from_release(source):
            return [c.encode() for c in _PROTEIN_COLUMNS]
        if source not in self._headers:
            self._headers[source] = self._cache_header(
                f"_va_header_{source}", self._sources[source]["file"]
            )
        return self._headers[source]

    async def stream_by_range(
        self, source: str, chr: int, start: int, end: int
    ) -> AsyncGenerator[bytes, None]:
        raw_stream = await self._stream_range(
            self._file(source),
            [chr],
            [start],
            [end],
            config.read_chunk_size,
        )
        return self._source_rows(source, raw_stream)

    async def stream_by_variants(
        self, source: str, variants: list[Variant]
    ) -> AsyncGenerator[bytes, None]:
        chrs = [v.chr for v in variants]
        starts = [v.pos for v in variants]
        ends = [v.pos for v in variants]
        raw_stream = await self._stream_range(
            self._file(source),
            chrs,
            starts,
            ends,
            config.read_chunk_size,
        )
        return self._filter_by_variants(
            self._source_rows(source, raw_stream), variants, self._cpra_cols(source)
        )

    def _source_rows(
        self, source: str, stream: AsyncGenerator[bytes, None]
    ) -> AsyncGenerator[bytes, None]:
        if self._from_release(source):
            return self._served_protein_rows(stream, self._protein_structure.release.by_accession)
        return stream

    @staticmethod
    async def _served_protein_rows(
        stream: AsyncGenerator[bytes, None], served: dict[str, tuple[str, ...]]
    ) -> AsyncGenerator[bytes, None]:
        """Rows of the accessions this deployment serves, empty cells written as NA.

        The file holds every accession of the release, the loaded index only those whose
        licence is served here. NA is how every TSV of this API writes a null, and the only
        spelling the JSON conversion turns into one.
        """
        n_columns = len(_PROTEIN_COLUMNS)
        malformed = 0

        def served_row(line: bytes) -> bytes:
            nonlocal malformed
            if not line:
                return b""
            fields = line.split(b"\t")
            # the status line has gone out by the time a row is read, so a row of the wrong
            # width is left out: raising here would cut a 200 short
            if len(fields) != n_columns:
                if not malformed:
                    logger.error(
                        f"protein variant map: a row of {len(fields)} fields, not {n_columns}, left out; "
                        "further ones in this response are not logged"
                    )
                malformed += 1
                return b""
            if fields[_PROTEIN_ACCESSION_COL].decode() not in served:
                return b""
            return b"\t".join(f or b"NA" for f in fields) + b"\n"

        pending = b""
        async for chunk in stream:
            *lines, pending = (pending + chunk).split(b"\n")
            out = b"".join(served_row(line) for line in lines)
            if out:
                yield out
        out = served_row(pending)
        if out:
            yield out

    async def _filter_by_variants(
        self,
        stream: AsyncGenerator[bytes, None],
        variants: list[Variant],
        cpra_cols: tuple[int, int, int, int],
    ) -> AsyncGenerator[bytes, None]:
        chr_col, pos_col, ref_col, alt_col = cpra_cols
        variant_keys = {
            (v.chr_bytes, v.pos_bytes, v.ref_bytes, v.alt_bytes) for v in variants
        }

        def wanted(line: bytes) -> bool:
            fields = line.split(b"\t")
            chrom = fields[chr_col].removeprefix(b"chr")
            key = (
                _SEX_CHROM_CODES.get(chrom, chrom),
                fields[pos_col],
                fields[ref_col],
                fields[alt_col],
            )
            return key in variant_keys

        buffer = b""

        async for chunk in stream:
            data = buffer + chunk
            lines = data.split(b"\n")

            for line in lines[:-1]:
                if line.strip() == b"":
                    continue
                if wanted(line):
                    yield line + b"\n"

            buffer = lines[-1]

        if buffer.strip() != b"" and wanted(buffer):
            yield buffer + b"\n"
