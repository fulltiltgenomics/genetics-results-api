"""`/gene_based/{gene}` queries the burden files on the gene's start position alone.

The files are point-indexed on gene_start, so a start..end query also returns every gene
that starts inside the requested gene's span: DDX19B starts 44 bp before AARS1 ends.
"""

import asyncio
import os
import shutil
import subprocess

import pytest

from app.routers.gene_based import _run_tabix

HEADER = "#dataset\ttrait\tgene\tgene_id\tgene_chr\tgene_start_pos\tgene_end_pos\tannotation\tmlog10p_burden\n"
ROWS = [
    "SCHEMA2\tSchizophrenia\tAARS1\tENSG00000090861\t16\t70251983\t70289707\tPTV\t0.0092\n",
    "SCHEMA2\tSchizophrenia\tDDX19B\tENSG00000157349\t16\t70289663\t70335305\tPTV\t0.0556\n",
]


@pytest.fixture
def burden_file(tmp_path):
    if not (shutil.which("bgzip") and shutil.which("tabix")):
        pytest.skip("bgzip/tabix not installed")
    # _run_tabix runs with this as its working directory
    os.makedirs("/tmp/tbi_cache", exist_ok=True)
    plain = tmp_path / "burden.tsv"
    plain.write_text(HEADER + "".join(ROWS))
    subprocess.run(["bgzip", str(plain)], check=True)
    path = f"{plain}.gz"
    subprocess.run(["tabix", "-s5", "-b6", "-e6", path], check=True)
    return path


def _genes(output: bytes) -> list[str]:
    return [line.split("\t")[2] for line in output.decode().splitlines() if not line.startswith("#")]


def test_a_gene_starting_inside_the_span_is_not_returned(burden_file):
    aars1 = [{"chrom": 16, "gene_start": 70251983, "gene_end": 70289707}]
    out = asyncio.run(_run_tabix({"id": "t", "gene_based": {"file": burden_file}}, aars1))
    assert _genes(out) == ["AARS1"]


def test_the_overlapping_neighbour_is_found_by_its_own_start(burden_file):
    ddx19b = [{"chrom": 16, "gene_start": 70289663, "gene_end": 70335305}]
    out = asyncio.run(_run_tabix({"id": "t", "gene_based": {"file": burden_file}}, ddx19b))
    assert _genes(out) == ["DDX19B"]
