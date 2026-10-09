"""LD served from the suite's own matrices, beside the upstream proxy in ld_service.py.

WHY A SECOND SOURCE. The proxy relays r² and D' from the FinnGen LD server, which cannot give
the signed r a correlated-instrument estimator needs, and every query there is a third-party
call. The panels here are tabix-indexed long-format matrices on the suite's own bucket, so they
carry r with its sign, are read by the same range engine as every other data product, and make
no outbound call. They are in-sample LD of a release, not a reference panel; the panel's
description says so and the name (`finngen_r12`) is deliberately not `sisu42`.

WHAT THE FILES ARE. One file per chromosome, one row per ORDERED pair (variant1, variant2)
within the panel's distance cap and at or above its r² floor, both orderings present, indexed
on variant1's position. "Every partner of a variant" is therefore one point query at its
position, and "this pair" is the same query filtered to one row. A pair absent from the file
is below the floor — never unknown — provided both variants are in the panel, which is why a
pair query reads both variants' blocks rather than one. A variant with no row at all is either
not in the panel or has no partner above the floor within the cap; the files cannot tell those
apart, and neither can this service.

The files spell ids chr22_19963748_G_A; callers spell them 22:19963748:G:A. An id is matched
exactly as the caller oriented it, as the upstream does: a swapped ref/alt is 404 naming the
spelling the panel has, rather than a silently negated r.
"""

import asyncio
import logging

from app.config import ld as ld_config
from app.core.variant import Variant
from app.services.gcloud_tabix_base import GCloudTabixBase
from app.services.ld_service import LDVariantNotInPanel

logger = logging.getLogger(__name__)

# column offsets of the layout the startup header check proves (ld_config.panel_header):
# chr pos variant1 variant2 <statistics in the panel's `fields` order>
_COL_VARIANT1 = 2
_COL_VARIANT2 = 3
_FIRST_STATISTIC = 4

# _stream_range takes a chunk size for interface compatibility and ignores it
_CHUNK = 64 * 1024

# how many of a position's other ids a 404 names: enough to show the swapped orientation and
# a multi-allelic neighbour, bounded so a dense position cannot bloat the detail
_MAX_IDS_NAMED = 5


def _chrom_label(chrom: int) -> str:
    """The spelling the panel's chromosome list and its ids use (Variant codes X as 23)."""
    return "X" if chrom == 23 else str(chrom)


def file_id(v: Variant) -> bytes:
    return f"chr{_chrom_label(v.chr)}_{v.pos}_{v.ref}_{v.alt}".encode()


def api_id(fid: bytes) -> str:
    """chr22_19963748_G_A -> 22:19963748:G:A, the spelling the upstream answers with."""
    chrom, pos, ref, alt = fid.decode().split("_", 3)
    return f"{chrom.removeprefix('chr')}:{pos}:{ref}:{alt}"


def _pos_of(fid: bytes) -> int:
    return int(fid.split(b"_", 2)[1])


class _LDPanel(GCloudTabixBase):
    """One panel: its per-chromosome files and the reads against them."""

    def __init__(self, cfg: dict) -> None:
        super().__init__()
        self.name: str = cfg["panel"]
        self.cfg = cfg
        self.fields: tuple[str, ...] = tuple(cfg["fields"])
        self._files = {
            label: ld_config.panel_file(cfg, label) for label in cfg["chromosomes"]
        }
        self._stat_col = {
            f: _FIRST_STATISTIC + i for i, f in enumerate(self.fields)
        }

    async def warm(self) -> None:
        """Prefetch and parse every chromosome's .tbi so the first query pays no index load."""
        await asyncio.gather(*(self._get_index(path) for path in self._files.values()))

    def _file_for(self, v: Variant) -> str | None:
        return self._files.get(_chrom_label(v.chr))

    async def _rows_at(self, v: Variant) -> list[list[bytes]]:
        """Every row whose variant1 sits at the variant's position, split into fields.

        The point-query fast path of the range engine: the rows of every allele at that
        position come back, and the caller picks the id it wants.
        """
        path = self._file_for(v)
        if path is None:
            return []
        stream = await self._stream_range(path, [v.chr], [v.pos], [v.pos], _CHUNK)
        buf = bytearray()
        async for chunk in stream:
            buf += chunk
        return [line.split(b"\t") for line in bytes(buf).split(b"\n") if line]

    async def block(self, v: Variant, spelled: str) -> list[list[bytes]]:
        """The variant's rows (one per partner), or LDVariantNotInPanel naming what the
        panel does have at that position — the swapped orientation, when that is the
        caller's mistake, is the common case."""
        rows = await self._rows_at(v)
        key = file_id(v)
        mine = [r for r in rows if r[_COL_VARIANT1] == key]
        if mine:
            return mine
        others = sorted({api_id(r[_COL_VARIANT1]) for r in rows})
        detail = f"{spelled} is not in LD panel {self.name}"
        if others:
            named = ", ".join(others[:_MAX_IDS_NAMED])
            more = f" and {len(others) - _MAX_IDS_NAMED} more" if len(others) > _MAX_IDS_NAMED else ""
            detail += f"; at that position the panel has {named}{more}"
        else:
            detail += (
                f" (or has no partner at r2 >= {self.cfg['r2_floor']:g} within "
                f"{self.cfg['max_distance']:,} bp, which the files cannot tell apart)"
            )
        raise LDVariantNotInPanel(detail)

    def entries(
        self,
        rows: list[list[bytes]],
        spelled: str,
        *,
        pos: int,
        half_window: int | None,
        r2_threshold: float,
    ) -> list[dict]:
        """Rows -> upstream-shaped entries, in file order (partner position ascending),
        carrying every statistic the panel has; the router trims to the fields asked for."""
        r2_col = self._stat_col["r2"]
        out: list[dict] = []
        for r in rows:
            other = r[_COL_VARIANT2]
            if half_window is not None and abs(_pos_of(other) - pos) > half_window:
                continue
            if float(r[r2_col]) < r2_threshold:
                continue
            entry = {"variation1": spelled, "variation2": api_id(other)}
            for f, c in self._stat_col.items():
                entry[f] = float(r[c])
            out.append(entry)
        return out

    def identity(self, spelled: str) -> dict:
        """A variant against itself: r = r² = D' = 1 by definition, and the files carry no
        self pairs, so it is answered rather than read."""
        return {"variation1": spelled, "variation2": spelled, **{f: 1.0 for f in self.fields}}


class LDPanelService:
    """The panels the active profile configures, built and warmed lazily like every other
    per-dataset data access object: construction touches nothing, `warm_all` prefetches."""

    def __init__(self) -> None:
        self._panels: dict[str, _LDPanel] = {}

    def panel_names(self) -> list[str]:
        return [cfg["panel"] for cfg in ld_config.ld_panels]

    def has_panel(self, panel: str) -> bool:
        return ld_config.panel_config(panel) is not None

    def panel_config(self, panel: str) -> dict:
        cfg = ld_config.panel_config(panel)
        if cfg is None:
            raise KeyError(f"no local LD panel {panel!r}; the router dispatches those to the upstream")
        return cfg

    def _get_panel(self, panel: str) -> _LDPanel:
        if panel not in self._panels:
            self._panels[panel] = _LDPanel(self.panel_config(panel))
        return self._panels[panel]

    async def warm_all(self) -> None:
        async def warm(name: str) -> None:
            try:
                await self._get_panel(name).warm()
            # a prefetch, not a gate: verify_all_data_files() has already proved the files
            except Exception as e:
                logger.warning(f"LD panel {name}: warm failed: {e}")

        await asyncio.gather(*(warm(name) for name in self.panel_names()))

    async def cleanup(self) -> None:
        for panel in self._panels.values():
            await panel.cleanup()

    async def variants_in_ld(
        self, variant: str, *, window: int, r2_threshold: float, panel: str
    ) -> list[dict]:
        """Every partner of `variant` within `window` (a total span centred on it, as the
        upstream defines it) at r² >= `r2_threshold`, partner position ascending."""
        p = self._get_panel(panel)
        v = Variant(variant)
        rows = await p.block(v, variant)
        return p.entries(
            rows, variant, pos=v.pos, half_window=window // 2, r2_threshold=r2_threshold
        )

    async def pair(
        self, variant: str, other: str, *, r2_threshold: float, panel: str
    ) -> list[dict]:
        """The one entry for (variant, other), or [] when the pair is below the floor or the
        threshold. Both variants must be in the panel: either absent is LDVariantNotInPanel.
        The distance cap and same-chromosome rule are checked by the router from config
        before any read happens."""
        p = self._get_panel(panel)
        v, o = Variant(variant), Variant(other)
        if file_id(v) == file_id(o):
            await p.block(v, variant)
            return [p.identity(variant)]
        # both blocks concurrently; the other's is read only to tell "not in panel" from
        # "below the floor". `return_exceptions` so neither read is left running detached
        # when the other raises, and the variant's own error is the one reported
        results = await asyncio.gather(
            p.block(v, variant), p.block(o, other), return_exceptions=True
        )
        for result in results:
            if isinstance(result, BaseException):
                raise result
        mine, _theirs = results
        key = file_id(o)
        hit = [r for r in mine if r[_COL_VARIANT2] == key]
        return p.entries(hit, variant, pos=v.pos, half_window=None, r2_threshold=r2_threshold)
