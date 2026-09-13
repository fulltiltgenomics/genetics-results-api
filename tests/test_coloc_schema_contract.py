"""Offline pins for the coloc simple projection and the by_variant response docs.

The simple format renames side-2 columns by stripping their suffix, and `nsnps2` stripped is
`nsnps`, the pair-level count that the format also keeps. Emitting both put `nsnps` in the
TSV header twice, and a JSON row keyed by name then held only the trait-2 value in place of
the pair-level one. The projection is checked here against the served schema so a header
change cannot reintroduce a collision unnoticed.
"""

from app.config.coloc import (
    coloc_credset_header_schema,
    coloc_header_schema,
    coloc_header_schema_simple,
)
from app.core.streams import coloc_simple_projection
from app.routers.colocalization import _BY_VARIANT_PROPERTIES, _BY_VARIANT_TSV_EXAMPLE

# the data file header: the schema minus the resource/version columns the iterators prepend
FILE_HEADER = [
    name.encode()
    for name in coloc_header_schema
    if name not in ("resource1", "version1", "resource2", "version2")
]


def test_simple_header_is_unique_and_matches_the_documented_schema():
    names = [b"resource", b"version"] + [
        name for _, name in coloc_simple_projection(FILE_HEADER)
    ]
    assert len(names) == len(set(names))
    assert [n.decode() for n in names] == list(coloc_header_schema_simple)


def test_simple_nsnps_is_the_pair_level_column():
    projected = {name: idx for idx, name in coloc_simple_projection(FILE_HEADER)}
    assert projected[b"nsnps"] == FILE_HEADER.index(b"nsnps")
    row = [str(i).encode() for i in range(len(FILE_HEADER))]
    values = [row[idx] for idx, _ in coloc_simple_projection(FILE_HEADER)]
    assert values[list(projected).index(b"nsnps")] == row[FILE_HEADER.index(b"nsnps")]


def test_by_variant_docs_follow_the_schemas():
    header, row = _BY_VARIANT_TSV_EXAMPLE.split("\n")
    assert header.split("\t") == list(coloc_header_schema)
    assert len(row.split("\t")) == len(coloc_header_schema)
    expected = list(coloc_header_schema) + [
        f"variant_{name}" for name in coloc_credset_header_schema
    ]
    assert list(_BY_VARIANT_PROPERTIES) == expected
