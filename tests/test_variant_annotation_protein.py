"""Offline tests of the `protein` variant-annotation source.

The release index is built from the contract mirror and the tabix reader is replaced by the
rows it would return, as in test_protein_structure_routes.py.
"""

import asyncio
import logging
import sys
from json import dumps, loads
from urllib.parse import urlencode

import pytest
from fastapi import FastAPI
from test_protein_structure import model_row
from test_protein_structure_routes import BASE, RELEASE, make_release

import app.config.common as common
import app.config.protein_structure as config
import app.config.protein_structure_contract as contract
from app.core.responses import DATASET_VERSION_HEADER
from app.dependencies import get_gene_name_mapping, get_variant_annotation_service
from app.routers import variant_annotation as routes
from app.services import gcloud_tabix_base
from app.services import protein_structure as svc
from app.services.protein_structure import ProteinStructureAccess
from app.services.variant_annotation_service import (
    PROTEIN_SOURCE,
    VariantAnnotationService,
)

POINTER = "gs://test-bucket/protein_structure/releases/CURRENT"
URL = f"/api/v1/variant_annotation/{PROTEIN_SOURCE}"
COLUMNS = [c for c, _ in contract.VARIANT_COLUMNS]

BRAF = "7:140753336:A:T"
TWO_PROTEINS = "21:46326429:A:C"
UNSERVED = "1:1000:A:G"
ON_X = "23:5000:C:T"


def variant_line(variant, accession, aa_pos, aa_ref, aa_alt, gene_id="ENSG00000000001", gene_name="G", mu="1.5e-08"):
    chrom, pos, ref, alt = variant.split(":")
    row = dict(zip(COLUMNS, [chrom, pos, ref, alt, variant, accession, gene_id, gene_name, str(aa_pos), aa_ref, aa_alt,
                             f"AF-{accession}-F1", str(aa_pos), mu], strict=True))
    return "\t".join(row[c] for c in COLUMNS) + "\n"


TABLE = [
    variant_line(UNSERVED, "A0A000", 5, "M", "V"),
    variant_line(BRAF, "P15056", 600, "V", "E"),
    variant_line("7:140753336:A:C", "P15056", 600, "V", "G"),
    variant_line(TWO_PROTEINS, "A0A494C0I6", 10, "K", "N", gene_id="", gene_name="", mu=""),
    variant_line(TWO_PROTEINS, "O95613", 20, "K", "N"),
    variant_line(ON_X, "O95613", 30, "R", "C"),
]


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    monkeypatch.setattr(gcloud_tabix_base, "ensure_gcs_token", lambda: None)


def access(pointer, loaded=True) -> ProteinStructureAccess:
    a = ProteinStructureAccess()
    a.pointer_uri = pointer
    if loaded:
        a._release = make_release([
            model_row(f"AF-{acc}-F1", acc, 1, 1, 1, 700) for acc in ("P15056", "A0A494C0I6", "O95613")
        ])
    return a


class Reply:
    def __init__(self, status_code: int, headers: dict[str, str], body: bytes):
        self.status_code, self.headers, self.text = status_code, headers, body.decode()

    def json(self):
        return loads(self.text)


class Client:
    """The app called over ASGI in process: the test client proper needs httpx, which is not installed."""

    def __init__(self, app: FastAPI, service: VariantAnnotationService):
        self.app, self.service = app, service

    def get(self, path, params=None) -> Reply:
        return asyncio.run(self._call("GET", path, params, b""))

    def post(self, path, json=None, params=None) -> Reply:
        return asyncio.run(self._call("POST", path, params, dumps(json).encode()))

    async def _call(self, method, path, params, body) -> Reply:
        messages, asked = [], []

        async def receive():
            if not asked:
                asked.append(True)
                return {"type": "http.request", "body": body, "more_body": False}
            await asyncio.Event().wait()

        async def send(message):
            messages.append(message)

        scope = {
            "type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"}, "http_version": "1.1",
            "method": method, "scheme": "http", "path": path, "raw_path": path.encode(),
            "query_string": urlencode(params or {}).encode(), "root_path": "",
            "headers": [(b"content-type", b"application/json")],
            "client": ("testclient", 50000), "server": ("testserver", 80), "state": {},
        }
        await self.app(scope, receive, send)
        start = next(m for m in messages if m["type"] == "http.response.start")
        return Reply(
            start["status"],
            {k.decode().lower(): v.decode() for k, v in start["headers"]},
            b"".join(bytes(m.get("body", b"")) for m in messages if m["type"] == "http.response.body"),
        )


def client(service: VariantAnnotationService) -> Client:
    service.asked = []

    async def stream_range(path, chroms, starts, ends, chunk_size):
        service.asked.append(path)
        blob = "".join(
            line for line in TABLE
            if any(int(line.split("\t")[0]) == c and s <= int(line.split("\t")[1]) <= e
                   for c, s, e in zip(chroms, starts, ends))
        ).encode()

        async def gen():
            # chunk boundaries fall inside lines
            for i in range(0, len(blob), 37):
                yield blob[i:i + 37]

        return gen()

    service._stream_range = stream_range
    app = FastAPI()
    app.include_router(routes.router, prefix="/api/v1")
    app.dependency_overrides[get_variant_annotation_service] = lambda: service
    app.dependency_overrides[get_gene_name_mapping] = lambda: None
    return Client(app, service)


@pytest.fixture
def loaded():
    return client(VariantAnnotationService(access(POINTER)))


class TestContract:
    def test_the_variants_header_is_a_named_load_check(self):
        assert (contract.VARIANTS_FILE, contract.VARIANT_COLUMNS, "variants_header") in svc.TABIX_HEADERS


class TestStates:
    @pytest.mark.parametrize("protein_structure", [None, access(None, loaded=False)])
    def test_no_pointer_and_the_source_is_unknown(self, protein_structure):
        service = VariantAnnotationService(protein_structure)
        assert service.get_available_sources() == list(common.variant_annotation_sources)
        api = client(service)
        for r in (api.get(URL, params={"variant": BRAF}), api.post(URL, json={"variants": [BRAF]})):
            unknown = api.get("/api/v1/variant_annotation/nonesuch", params={"variant": BRAF})
            assert r.status_code == unknown.status_code == 404
            assert r.json()["detail"] == unknown.json()["detail"].replace("nonesuch", PROTEIN_SOURCE)
        assert service.asked == []

    def test_a_pointer_without_a_release_is_a_503(self):
        service = VariantAnnotationService(access(POINTER, loaded=False))
        assert PROTEIN_SOURCE in service.get_available_sources()
        api = client(service)
        for r in (
            api.get(URL, params={"variant": BRAF}),
            api.get(URL, params={"region": "7:140753000-140754000"}),
            # refused before the gene is looked up: the mapping here is None
            api.get(URL, params={"gene": "BRAF"}),
            api.post(URL, json={"variants": [BRAF]}),
        ):
            assert r.status_code == 503 and "no protein structure release is loaded" in r.json()["detail"]
        assert service.asked == []

    def test_the_source_appears_once_the_retried_load_succeeds(self):
        protein_structure = access(POINTER, loaded=False)
        api = client(VariantAnnotationService(protein_structure))
        assert api.post(URL, json={"variants": [BRAF]}).status_code == 503
        protein_structure._release = access(POINTER)._release
        assert api.post(URL, json={"variants": [BRAF]}).status_code == 200

    def test_the_configured_sources_are_left_as_they_were(self, monkeypatch):
        monkeypatch.setattr(config, "release_pointer", POINTER)
        service = VariantAnnotationService(access(POINTER))
        for source, entry in common.variant_annotation_sources.items():
            assert service._file(source) == entry["file"]
            assert service.get_version(source) == entry.get("version")
            stream = object()
            assert service._source_rows(source, stream) is stream
        assert PROTEIN_SOURCE not in common.variant_annotation_sources


class TestRows:
    def test_the_file_is_the_loaded_releases(self, loaded):
        loaded.post(URL, json={"variants": [BRAF]})
        assert loaded.service.asked == [
            f"{BASE}/{contract.VARIANTS_FILE}"
        ]

    def test_post_returns_every_protein_of_each_variant(self, loaded):
        r = loaded.post(URL, json={"variants": [BRAF, TWO_PROTEINS, "2:5:A:C"]})
        assert r.status_code == 200 and r.headers["content-type"].startswith("text/tab-separated-values")
        assert r.headers[DATASET_VERSION_HEADER.lower()] == RELEASE
        header, *rows = r.text.splitlines()
        assert header.split("\t") == COLUMNS
        cells = [dict(zip(COLUMNS, row.split("\t"), strict=True)) for row in rows]
        # the other allele at BRAF's position is in the file and was not asked for
        assert [(c["variant"], c["accession"], c["aa_ref"] + c["aa_pos"] + c["aa_alt"]) for c in cells] == [
            (BRAF, "P15056", "V600E"), (TWO_PROTEINS, "A0A494C0I6", "K10N"), (TWO_PROTEINS, "O95613", "K20N"),
        ]
        # the release writes null as an empty cell, this API as NA
        assert (cells[1]["gene_id"], cells[1]["gene_name"], cells[1]["mu_snp"]) == ("NA", "NA", "NA")

    def test_json_rows_carry_null_for_an_empty_cell(self, loaded):
        r = loaded.post(URL, params={"format": "json"}, json={"variants": [TWO_PROTEINS]})
        assert r.status_code == 200 and r.headers[DATASET_VERSION_HEADER.lower()] == RELEASE
        assert r.headers["x-columns"].split(",") == COLUMNS
        first, second = r.json()
        assert list(first) == COLUMNS
        assert first["gene_id"] is None and first["mu_snp"] is None
        assert (second["accession"], second["aa_pos"], second["mu_snp"]) == ("O95613", "20", "1.5e-08")

    def test_an_accession_outside_the_loaded_index_is_not_served(self, loaded):
        for r in (
            loaded.post(URL, json={"variants": [UNSERVED]}),
            loaded.get(URL, params={"variant": UNSERVED}),
            loaded.get(URL, params={"region": "1:1-2000"}),
        ):
            assert r.status_code == 200 and r.text.splitlines() == ["\t".join(COLUMNS)]

    def test_a_variant_absent_from_the_map_is_an_empty_result(self, loaded):
        r = loaded.get(URL, params={"variant": "7:140753336:A:G", "format": "json"})
        assert r.status_code == 200 and r.json() == []
        assert r.headers["x-columns"].split(",") == COLUMNS

    def test_get_by_variant_and_by_region(self, loaded):
        r = loaded.get(URL, params={"variant": BRAF})
        assert [x.split("\t")[4] for x in r.text.splitlines()[1:]] == [BRAF]
        r = loaded.get(URL, params={"region": "7:140753000-140754000"})
        assert [x.split("\t")[4] for x in r.text.splitlines()[1:]] == [BRAF, "7:140753336:A:C"]

    def test_get_by_gene_reads_the_genes_span(self, loaded):
        class Mapping:
            def get_coordinates_by_gene_name(self, gene):
                assert gene == "BRAF"
                return {"49": [{"chrom": 7, "gene_start": 140_753_000, "gene_end": 140_754_000}]}

        loaded.app.dependency_overrides[get_gene_name_mapping] = Mapping
        for fmt in ("tsv", "json"):
            r = loaded.get(URL, params={"gene": "BRAF", "format": fmt})
            assert r.status_code == 200 and r.headers[DATASET_VERSION_HEADER.lower()] == RELEASE
            variants = [x["variant"] for x in r.json()] if fmt == "json" else [x.split("\t")[4] for x in r.text.splitlines()[1:]]
            assert variants == [BRAF, "7:140753336:A:C"]

    def test_a_region_holding_only_unserved_rows_is_empty_json(self, loaded):
        r = loaded.get(URL, params={"region": "1:1-2000", "format": "json"})
        assert r.status_code == 200 and r.json() == [] and r.headers["x-columns"].split(",") == COLUMNS

    def test_alleles_are_matched_in_upper_case(self, loaded):
        r = loaded.get(URL, params={"variant": BRAF.lower()})
        assert r.status_code == 200 and [x.split("\t")[4] for x in r.text.splitlines()[1:]] == [BRAF]

    @pytest.mark.parametrize("indel", ["7:140753336:AT:A", "7:140753336:A:AT"])
    def test_an_indel_is_a_200_without_rows(self, loaded, indel):
        for r in (loaded.get(URL, params={"variant": indel}), loaded.post(URL, json={"variants": [indel]})):
            assert r.status_code == 200 and r.text.splitlines() == ["\t".join(COLUMNS)]

    def test_a_row_of_the_wrong_width_is_left_out_and_logged_once(self, monkeypatch, loaded, caplog):
        short = "\t".join(TABLE[1].split("\t")[:3]) + "\n"
        long = TABLE[1].rstrip("\n") + "\textra\n"
        monkeypatch.setattr(sys.modules[__name__], "TABLE", [short, TABLE[1], long, TABLE[2], short.rstrip("\n")])
        with caplog.at_level(logging.ERROR, logger="app.services.variant_annotation_service"):
            r = loaded.get(URL, params={"region": "7:140753000-140754000"})
        assert r.status_code == 200
        assert [x.split("\t")[4] for x in r.text.splitlines()[1:]] == [BRAF, "7:140753336:A:C"]
        assert [rec.getMessage() for rec in caplog.records].count(caplog.records[0].getMessage()) == 1
        assert len(caplog.records) == 1 and "3 fields, not 14" in caplog.text

    @pytest.mark.parametrize("spelling", ["X:5000:C:T", "chrX:5000:C:T", "23:5000:C:T"])
    def test_x_is_matched_however_it_is_written(self, loaded, spelling):
        r = loaded.get(URL, params={"variant": spelling})
        assert [x.split("\t")[4] for x in r.text.splitlines()[1:]] == [ON_X]

    def test_the_variant_limit_and_a_malformed_id_are_refused(self, loaded, monkeypatch):
        monkeypatch.setattr(common, "max_query_variants", 2)
        assert loaded.post(URL, json={"variants": [BRAF, TWO_PROTEINS]}).status_code == 200
        assert loaded.post(URL, json={"variants": [BRAF, TWO_PROTEINS, ON_X]}).status_code == 422
        r = loaded.post(URL, json={"variants": [BRAF, "rs113488022"]})
        assert r.status_code == 422 and "rs113488022" in r.json()["detail"]
        assert loaded.post(URL, json={"variants": []}).status_code == 422
