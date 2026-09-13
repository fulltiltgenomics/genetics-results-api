"""`/gene_based/{gene}` appends a `resource` column to the merged burden TSV.

The burden files have no resource column, so without this a merged result across
datasets cannot be filtered by resource — which is what `gene_burden_results_v`'s schema
docs tell the sandbox SDK's callers to do.
"""

import asyncio

from app.routers.gene_based import _merge_results

HEADER = b"#dataset\ttrait\tgene\tmlog10p_burden\n"


async def _collect(data_files, results):
    return b"".join([chunk async for chunk in _merge_results(data_files, results)])


def test_resource_is_appended_to_header_and_every_row():
    files = [{"id": "schema_gene_based", "resource": "schema2"}, {"id": "bipex_gene_based", "resource": "bipex2"}]
    results = [
        HEADER + b"SCHEMA2\tschizophrenia\tRB1CC1\t10.8\n",
        HEADER + b"BipEx2\tbipolar_disorder\tRB1CC1\t11.1\n",
    ]

    out = asyncio.run(_collect(files, results)).decode().splitlines()

    assert out == [
        "dataset\ttrait\tgene\tmlog10p_burden\tresource",
        "SCHEMA2\tschizophrenia\tRB1CC1\t10.8\tschema2",
        "BipEx2\tbipolar_disorder\tRB1CC1\t11.1\tbipex2",
    ]


def test_header_comes_from_the_first_file_with_output():
    files = [{"id": "a", "resource": "genebass"}, {"id": "b", "resource": "ibd_exome_2026"}]
    results = [b"", HEADER + b"IBD_exome\tinflammatory_bowel_disease\tNOD2\t9.2\n"]

    out = asyncio.run(_collect(files, results)).decode().splitlines()

    assert out[0].endswith("\tresource")
    assert out[1].endswith("\tibd_exome_2026")
    assert len(out) == 2
