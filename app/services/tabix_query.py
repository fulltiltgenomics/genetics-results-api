"""In-process tabix range queries against bgzf+tbi files on GCS.

htslib's `tabix` subprocess fetches one byte range per query region serially over a
single connection (~20 ms/region in-region), so a batch of N regions costs N round
trips. This module replaces that with: parse the (small) `.tbi` index once, turn the
query regions into a set of compressed byte ranges, and let the caller fetch those
ranges concurrently over a pooled HTTP session. Decompression uses stdlib `zlib`.

The index/format logic mirrors htslib's tbi reader and reg2bin/get_intv so that the
returned records are exactly those `tabix -R` would emit for the same regions.
"""

import struct
import zlib
from dataclasses import dataclass, field

# bgzf blocks are at most 64 KiB; padding a range end by this guarantees the final
# (partial-virtual-offset) block is fetched whole and can be fully decompressed.
_MAX_BLOCK = 65536

# tbi format field: low 16 bits = preset, bit 16 = 0-based (UCSC/BED) coordinates
_TBX_VCF = 2
_FLAG_ZERO_BASED = 0x10000


@dataclass
class _Ref:
    bins: dict[int, list[tuple[int, int]]] = field(default_factory=dict)
    linear: list[int] = field(default_factory=list)


@dataclass
class TabixIndex:
    preset: int
    zero_based: bool
    col_seq: int
    col_beg: int
    col_end: int
    meta: int  # comment/meta character (e.g. ord('#'))
    name_to_tid: dict[bytes, int]
    names: list[bytes]  # tid -> sequence name
    refs: list[_Ref]

    def tid_for_chrom(self, chrom: int) -> int | None:
        """Resolve a numeric chromosome (X=23, Y=24, MT=25) to a tid, tolerating
        common seqname spellings the file might use."""
        candidates = [str(chrom).encode()]
        special = {23: b"X", 24: b"Y", 25: b"MT"}.get(chrom)
        if special:
            candidates += [special, b"chr" + special]
        candidates.append(b"chr" + str(chrom).encode())
        for name in candidates:
            tid = self.name_to_tid.get(name)
            if tid is not None:
                return tid
        return None

    def record_interval(self, fields: list[bytes]) -> tuple[int, int]:
        """0-based half-open [beg, end) of a record, computed like htslib get_intv."""
        beg = int(fields[self.col_beg - 1])
        beg0 = beg if self.zero_based else beg - 1
        if self.col_end != 0:
            end = int(fields[self.col_end - 1])
        elif self.preset == _TBX_VCF:
            # VCF: end spans the REF allele (column 4, 0-based index 3)
            end = beg0 + max(1, len(fields[3]))
        else:
            end = beg0 + 1
        return beg0, end

    def byte_ranges(
        self, regions: list[tuple[int, int, int]]
    ) -> list[tuple[int, int, int]]:
        """Map query regions (tid, beg0, end) to merged compressed byte ranges to
        fetch, as (start, last_block_off, skip) tuples.

        - ``start`` is the compressed offset of the first bgzf block to read.
        - ``last_block_off`` is the compressed offset of the *last* block that must
          be read fully (it holds the chunk's final records); the caller streams
          from ``start`` and stops once that block has been read whole, so only the
          relevant blocks are downloaded (no fixed 64 KiB over-read per range).
        - ``skip`` is the within-block offset of the first record: a range begins at
          a block boundary but its first record may continue from the previous
          block, so decompressed bytes before ``skip`` are a partial fragment.

        Ranges whose final block could overlap (within one max block) are merged so
        the boundary block is fetched once; over-read records are dropped by overlap
        filtering, and reading each block once avoids duplicates.
        """
        spans: list[tuple[int, int, int]] = []
        for tid, beg0, end in regions:
            if tid < 0 or tid >= len(self.refs):
                continue
            ref = self.refs[tid]
            min_off = 0
            li = beg0 >> 14
            if ref.linear and li < len(ref.linear):
                min_off = ref.linear[li]
            for b in _reg2bins(beg0, end):
                for cbeg, cend in ref.bins.get(b, ()):
                    if cend <= min_off:
                        continue
                    spans.append((cbeg >> 16, cend >> 16, cbeg & 0xFFFF))
        if not spans:
            return []
        spans.sort()
        merged = [spans[0]]
        for s, e, u in spans[1:]:
            ls, le, lu = merged[-1]
            # the block at `le` may extend up to le + _MAX_BLOCK; merge ranges that
            # start within that window so the boundary block is read only once
            if s <= le + _MAX_BLOCK:
                if e > le:
                    merged[-1] = (ls, e, lu)
            else:
                merged.append((s, e, u))
        return merged


def parse_tabix_index(raw_tbi: bytes) -> TabixIndex:
    """Parse a (bgzf-compressed) .tbi index into a TabixIndex."""
    data = bgzf_decompress(raw_tbi)
    if data[:4] != b"TBI\x01":
        raise ValueError("not a tabix index (bad magic)")
    (n_ref, fmt, col_seq, col_beg, col_end, meta, _skip, l_nm) = struct.unpack_from(
        "<8i", data, 4
    )
    off = 36
    names_blob = data[off : off + l_nm]
    off += l_nm
    names = [n for n in names_blob.split(b"\x00") if n]
    name_to_tid = {name: tid for tid, name in enumerate(names)}

    refs: list[_Ref] = []
    for _ in range(n_ref):
        ref = _Ref()
        (n_bin,) = struct.unpack_from("<i", data, off)
        off += 4
        for _ in range(n_bin):
            bin_id, n_chunk = struct.unpack_from("<Ii", data, off)
            off += 8
            chunks = []
            for _ in range(n_chunk):
                cbeg, cend = struct.unpack_from("<QQ", data, off)
                off += 16
                chunks.append((cbeg, cend))
            ref.bins[bin_id] = chunks
        (n_intv,) = struct.unpack_from("<i", data, off)
        off += 4
        if n_intv:
            ref.linear = list(struct.unpack_from(f"<{n_intv}Q", data, off))
            off += 8 * n_intv
        refs.append(ref)

    return TabixIndex(
        preset=fmt & 0xFFFF,
        zero_based=bool(fmt & _FLAG_ZERO_BASED),
        col_seq=col_seq,
        col_beg=col_beg,
        col_end=col_end,
        meta=meta,
        name_to_tid=name_to_tid,
        names=names,
        refs=refs,
    )


def filter_records(buf: bytes, skip: int, spec: dict) -> bytes:
    """Decompress a fetched byte range and return the newline-joined records that
    overlap the query, matching `tabix -R` output. ``spec`` is a small picklable
    description of the query so this can run in a worker process.

    Lines are split only up to the columns needed. The fast path (point queries on
    point records) compares (seqname, pos) bytes against a precomputed key set with
    no integer parsing; the general path computes record intervals and tests overlap.
    """
    text = bgzf_decompress(buf)
    if not text:
        return b""
    text = text[skip:]
    lines = text.split(b"\n")
    # drop a trailing partial line from the padded over-read
    if lines and lines[-1] != b"":
        lines = lines[:-1]
    meta = spec["meta"]
    seq_col = spec["seq_col"]
    beg_col = spec["beg_col"]
    ncols = spec["ncols"]
    kept = []
    if spec["point_query"]:
        query_keys = spec["query_keys"]
        for line in lines:
            if not line or line[0] == meta:
                continue
            fields = line.split(b"\t", ncols)
            if len(fields) < ncols:
                continue
            if (fields[seq_col], fields[beg_col]) in query_keys:
                kept.append(line)
    else:
        name_to_tid = spec["name_to_tid"]
        region_intervals = spec["region_intervals"]
        end_col = spec["end_col"]
        preset = spec["preset"]
        zero_based = spec["zero_based"]
        for line in lines:
            if not line or line[0] == meta:
                continue
            fields = line.split(b"\t", ncols)
            if len(fields) < ncols:
                continue
            tid = name_to_tid.get(fields[seq_col])
            if tid is None:
                continue
            intervals = region_intervals.get(tid)
            if not intervals:
                continue
            beg = int(fields[beg_col])
            beg0 = beg if zero_based else beg - 1
            if end_col >= 0:
                rend = int(fields[end_col])
            elif preset == _TBX_VCF:
                rend = beg0 + max(1, len(fields[3]))
            else:
                rend = beg0 + 1
            for qbeg, qend in intervals:
                if beg0 < qend and qbeg < rend:
                    kept.append(line)
                    break
    return b"\n".join(kept) + b"\n" if kept else b""


def filter_batch(items: list[tuple[bytes, int]], spec: dict) -> list[bytes]:
    """Filter a batch of (buf, skip) ranges with one query spec. Runs as a single
    unit of work in a worker process so the spec is pickled once per batch."""
    return [filter_records(buf, skip, spec) for buf, skip in items]


def _reg2bins(beg: int, end: int) -> list[int]:
    """Bins overlapping the 0-based half-open [beg, end), per the BAM/TBI scheme
    (min_shift 14, depth 5)."""
    if end <= beg:
        end = beg + 1
    e = end - 1
    out = [0]
    for shift, base in ((26, 1), (23, 9), (20, 73), (17, 585), (14, 4681)):
        for k in range(base + (beg >> shift), base + (e >> shift) + 1):
            out.append(k)
    return out


def bgzf_block_size(buf: bytes, off: int) -> int | None:
    """Total byte length of the bgzf block starting at ``off`` in ``buf`` (from its
    BC extra subfield), or None if the header isn't fully present yet."""
    if off + 18 > len(buf) or buf[off] != 0x1F or buf[off + 1] != 0x8B:
        return None
    xlen = buf[off + 10] | (buf[off + 11] << 8)
    j = off + 12
    extra_end = j + xlen
    while j + 4 <= extra_end and j + 4 <= len(buf):
        si1, si2, slen = buf[j], buf[j + 1], buf[j + 2] | (buf[j + 3] << 8)
        if si1 == 66 and si2 == 67 and j + 6 <= len(buf):
            return (buf[j + 4] | (buf[j + 5] << 8)) + 1
        j += 4 + slen
    return None


def bgzf_decompress(buf: bytes) -> bytes:
    """Decompress a concatenation of bgzf blocks. A trailing partial block (from a
    padded over-read) is ignored."""
    out = bytearray()
    i = 0
    n = len(buf)
    while i + 18 <= n:
        if buf[i] != 0x1F or buf[i + 1] != 0x8B:
            break
        xlen = buf[i + 10] | (buf[i + 11] << 8)
        bsize = None
        j = i + 12
        extra_end = j + xlen
        while j + 4 <= extra_end:
            si1, si2, slen = buf[j], buf[j + 1], buf[j + 2] | (buf[j + 3] << 8)
            if si1 == 66 and si2 == 67:  # 'BC' subfield carries BSIZE
                bsize = buf[j + 4] | (buf[j + 5] << 8)
            j += 4 + slen
        if bsize is None:
            break
        blen = bsize + 1
        if i + blen > n:  # partial trailing block from over-read; stop
            break
        block = buf[i : i + blen]
        if blen > 28:  # 28-byte block is the empty EOF marker
            out += zlib.decompress(block, 31)
        i += blen
    return bytes(out)
