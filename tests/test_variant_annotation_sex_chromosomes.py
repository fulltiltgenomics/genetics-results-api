"""Sex-chromosome lookups on /variant_annotation/{source}, against a local bgzip+tabix file.

A caller may write X as X, chrX or 23, and a source file may name the contig either way.
The gnomAD file names its contigs 23 and 24 and holds one row per variant; the same
service also serves files that spell X as a letter, so both spellings are driven here.
Offline: the router runs in-process and the "GCS" range reads are local file reads.
"""

import importlib
import json
import pathlib
import subprocess
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlencode

import pytest
from fastapi import FastAPI
from helpers.asgi import call_asgi
from helpers.local_tabix import LocalTabixReads

from app.core.exceptions import ParseException
from app.core.responses import DATASET_VERSION_HEADER
from app.core.variant import Variant
from app.dependencies import get_gene_name_mapping, get_variant_annotation_service
from app.routers import variant_annotation
from app.services import gcloud_tabix_base
from app.services.variant_annotation_service import VariantAnnotationService

HEADER = (
    "chr pos ref alt rsids filters AN AF AF_afr AF_amr AF_asj AF_eas AF_fin AF_mid "
    "AF_nfe AF_remaining AF_sas most_severe gene_most_severe consequences genome_or_exome"
).split()
CPRA_COLS = [0, 1, 2, 3]

# (chr, pos, ref, alt, genome_or_exome); 1:1000 carries two alleles so that a point
# lookup has a same-position neighbour to leave out
VARIANTS = [
    ("1", 1000, "A", "G", "genome"),
    ("1", 1000, "A", "T", "exome"),
    ("1", 2000, "C", "T", "both"),
    ("23", 5000, "G", "A", "genome"),
    ("23", 6000, "T", "C", "exome"),
    ("24", 7000, "C", "G", "genome"),
]


def _write_tabix(path, contig_names: dict[str, str]) -> str:
    lines = ["#" + "\t".join(HEADER)]
    for chrom, pos, ref, alt, source in VARIANTS:
        row = dict.fromkeys(HEADER, "NA")
        row.update(
            chr=contig_names.get(chrom, chrom), pos=str(pos), ref=ref, alt=alt,
            genome_or_exome=source,
        )
        lines.append("\t".join(row[h] for h in HEADER))
    path.write_text("\n".join(lines) + "\n")
    subprocess.run(["bgzip", str(path)], check=True)
    subprocess.run(["tabix", "-s1", "-b2", "-e2", f"{path}.gz"], check=True)
    return f"{path}.gz"


class _LocalFileService(LocalTabixReads, VariantAnnotationService):
    """The real lookup path, with the index and block reads taken from local disk."""

    def __init__(self, sources: dict) -> None:
        self._sources = sources
        self._headers = {name: [h.encode() for h in HEADER] for name in sources}


@pytest.fixture
def api(tmp_path, monkeypatch):
    """Two sources over the same variants: `gnomad` names the sex chromosomes 23 and 24,
    `lettered` names them X and Y."""
    pool = ThreadPoolExecutor(1)
    monkeypatch.setattr(gcloud_tabix_base, "_get_filter_pool", lambda: pool)
    monkeypatch.setattr(gcloud_tabix_base, "_files_semaphore", None)
    service = _LocalFileService(
        {
            "gnomad": {
                "file": _write_tabix(tmp_path / "numeric.tsv", {}),
                "version": "numeric",
                "cpra_cols": CPRA_COLS,
            },
            "lettered": {
                "file": _write_tabix(tmp_path / "lettered.tsv", {"23": "X", "24": "Y"}),
                "version": "lettered",
                "cpra_cols": CPRA_COLS,
            },
        }
    )
    app = FastAPI()
    app.include_router(variant_annotation.router)
    app.dependency_overrides[get_variant_annotation_service] = lambda: service
    app.dependency_overrides[get_gene_name_mapping] = lambda: None
    try:
        yield app, service
    finally:
        pool.shutdown()


def _request(app, method: str, path: str, params: dict, body: dict | None = None):
    """Drive the ASGI app directly: httpx (and so TestClient) is not installed here."""
    response = call_asgi(
        app,
        method,
        path,
        query=urlencode(params),
        headers=[("content-type", "application/json")],
        body=json.dumps(body).encode() if body is not None else b"",
    )
    headers = {k.decode().lower(): v.decode() for k, v in response.headers}
    return response.status_code, headers, response.content


def _rows(app, source: str, **params) -> list[dict]:
    status, _, content = _request(
        app, "GET", f"/variant_annotation/{source}", {**params, "format": "json"}
    )
    assert status == 200, content
    return json.loads(content)


def _cpra(rows: list[dict]) -> list[tuple]:
    return [(r["chr"], int(r["pos"]), r["ref"], r["alt"]) for r in rows]


@pytest.mark.parametrize("source,contig", [("gnomad", "23"), ("lettered", "X")])
@pytest.mark.parametrize("spelling", ["X", "chrX", "x", "23", "chr23"])
def test_x_point_lookup_returns_the_row_however_x_is_spelled(api, source, contig, spelling):
    app, _ = api
    rows = _rows(app, source, variant=f"{spelling}:5000:G:A")
    assert _cpra(rows) == [(contig, 5000, "G", "A")]


@pytest.mark.parametrize("source,contig", [("gnomad", "24"), ("lettered", "Y")])
@pytest.mark.parametrize("spelling", ["Y", "chrY", "24"])
def test_y_point_lookup_returns_the_row(api, source, contig, spelling):
    app, _ = api
    rows = _rows(app, source, variant=f"{spelling}-7000-C-G")
    assert _cpra(rows) == [(contig, 7000, "C", "G")]


@pytest.mark.parametrize("source,contig", [("gnomad", "23"), ("lettered", "X")])
@pytest.mark.parametrize("spelling", ["X", "chrX", "23"])
def test_x_region_lookup(api, source, contig, spelling):
    app, _ = api
    rows = _rows(app, source, region=f"{spelling}:4000-6500")
    assert _cpra(rows) == [(contig, 5000, "G", "A"), (contig, 6000, "T", "C")]


@pytest.mark.parametrize("spelling", ["Y", "chrY", "24"])
def test_y_region_lookup(api, spelling):
    app, _ = api
    rows = _rows(app, "gnomad", region=f"{spelling}:1-10000")
    assert _cpra(rows) == [("24", 7000, "C", "G")]


def test_a_variant_present_once_returns_exactly_one_row(api):
    app, _ = api
    rows = _rows(app, "gnomad", variant="1:2000:C:T")
    assert len(rows) == 1
    assert rows[0]["genome_or_exome"] == "both"


def test_a_point_lookup_leaves_out_the_other_allele_at_the_position(api):
    app, _ = api
    rows = _rows(app, "gnomad", variant="1:1000:A:T")
    assert _cpra(rows) == [("1", 1000, "A", "T")]


@pytest.mark.parametrize("source,x,y", [("gnomad", "23", "24"), ("lettered", "X", "Y")])
def test_batch_lookup_spans_autosomes_and_sex_chromosomes(api, source, x, y):
    app, _ = api
    status, _, content = _request(
        app,
        "POST",
        f"/variant_annotation/{source}",
        {"format": "json"},
        {"variants": ["chrX:6000:T:C", "Y:7000:C:G", "1:1000:A:G", "23:5000:G:A"]},
    )
    assert status == 200, content
    assert _cpra(json.loads(content)) == [
        ("1", 1000, "A", "G"),
        (x, 5000, "G", "A"),
        (x, 6000, "T", "C"),
        (y, 7000, "C", "G"),
    ]


def test_an_unknown_chromosome_is_refused(api):
    app, _ = api
    for params in ({"variant": "25:1:A:T"}, {"region": "Z:1-100"}):
        status, _, _ = _request(app, "GET", "/variant_annotation/gnomad", params)
        assert status == 422


@pytest.mark.parametrize("variant", ["Y:7000:C:G", "chrY:7000:C:G", "24:7000:C:G"])
def test_the_shared_parser_still_refuses_y(variant):
    """Every other endpoint parses its variants with the parser's defaults."""
    with pytest.raises(ParseException, match="supported chromosomes: 1-23,X$"):
        Variant(variant)


def test_only_the_variant_annotation_router_opts_in_to_y():
    app_dir = pathlib.Path(variant_annotation.__file__).parents[1]
    opted_in = {
        path.relative_to(app_dir).as_posix()
        for path in app_dir.rglob("*.py")
        if "allow_y=True" in path.read_text()
    }
    assert opted_in == {"routers/variant_annotation.py"}


@pytest.mark.parametrize("profile", ["finngen", "daly"])
@pytest.mark.parametrize("format", ["tsv", "json"])
def test_gnomad_source_reports_its_release(api, profile, format):
    """The header carries the profile's own `version`, so the release is asserted on
    the shipped config rather than on a value this test supplies."""
    app, service = api
    configured = importlib.import_module(
        f"app.config.profiles.{profile}.common"
    ).variant_annotation_sources["gnomad"]
    service._sources["gnomad"] = {**configured, "file": service._sources["gnomad"]["file"]}

    get = _request(
        app, "GET", "/variant_annotation/gnomad", {"variant": "X:5000:G:A", "format": format}
    )
    post = _request(
        app,
        "POST",
        "/variant_annotation/gnomad",
        {"format": format},
        {"variants": ["X:5000:G:A"]},
    )
    for status, headers, content in (get, post):
        assert status == 200, content
        assert headers[DATASET_VERSION_HEADER.lower()] == "4.1.1"
