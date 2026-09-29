"""Widen a REGENIE HLA-allele run into the gene/allele shape the /hla endpoints serve.

The pipeline writes the allele as a variant: `ref='<absent>'`, `alt='DQB1*02:01'`, one row
per allele at its gene's anchor position, as a plain gzip with no tabix index. The core
`finngen_hla` files were munged offline into `gene`/`allele` columns; this does the same
rewrite in memory so the same column mapping and sort key apply to both.
"""

import gzip


def hla_gene_of(allele: bytes) -> bytes:
    """`DQB1*02:01` -> `HLA-DQB1`; an allele with no `*` keeps its whole name as the gene."""
    return b"HLA-" + allele.split(b"*", 1)[0]


def widen_hla_alleles(header: list[bytes], rows: list[list[bytes]]) -> tuple[list[bytes], list[list[bytes]]]:
    """Replace the `ref`/`alt` columns by `gene`/`allele`; the other columns are untouched.

    Raises ValueError when the header carries no `alt` column, since a file without the
    allele column cannot be an HLA run at all.
    """
    if b"alt" not in header or b"ref" not in header:
        raise ValueError(f"not an HLA allele file: header {header}")
    ref_idx = header.index(b"ref")
    alt_idx = header.index(b"alt")
    new_header = list(header)
    new_header[ref_idx] = b"gene"
    new_header[alt_idx] = b"allele"
    new_rows = []
    for row in rows:
        if len(row) <= alt_idx:
            continue
        widened = list(row)
        widened[ref_idx] = hla_gene_of(row[alt_idx])
        new_rows.append(widened)
    return new_header, new_rows


def parse_gzip_tsv(payload: bytes) -> tuple[list[bytes], list[list[bytes]]]:
    """Header (leading `#` stripped) and rows of a gzipped TSV object read whole."""
    text = gzip.decompress(payload)
    lines = [line for line in text.split(b"\n") if line.strip()]
    if not lines:
        raise ValueError("empty file")
    header = lines[0].split(b"\t")
    if header and header[0].startswith(b"#"):
        header[0] = header[0][1:]
    return header, [line.split(b"\t") for line in lines[1:]]


# name -> (widening function) a summary_stats config selects with `unindexed: <name>`
UNINDEXED_TRANSFORMS = {"hla_allele": widen_hla_alleles}
