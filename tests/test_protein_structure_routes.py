"""Offline tests of the protein structure routes and the reads behind them.

The release is built from the contract mirror; GCS is an in-memory object store that answers
whole and ranged GETs as GCS does, and the tabix reader is replaced by the rows it would return.
"""

import asyncio
import gzip
import json
import os

import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from test_protein_structure import model_row, models_bytes

import app.config.protein_structure as config
import app.config.protein_structure_contract as contract
from app.core import limits
from app.core.exceptions import GeneNotFoundException, ParseException
from app.dependencies import get_gene_name_mapping, get_protein_structure
from app.routers import protein_structure as routes
from app.server import PROTEIN_STRUCTURE_ROUTERS, include_protein_structure
from app.services import data_access_protein_structure as data
from app.services import gcloud_tabix_base
from app.services.protein_structure import Pointer, Release, parse_models

BASE = "gs://test-bucket/protein_structure/releases/20261112-120000"
RELEASE = "20261112-120000"
# the build's own bound on one gene's stored residue rows (munge_protein_structure.py)
BUILD_MAX_GENE_RESIDUE_BYTES = 8 * 1024 * 1024

CELSR2 = {"ensg": "ENSG00000082074", "chrom": 1, "gene_start": 109_249_539, "gene_end": 109_275_751}
TP53 = {"ensg": "ENSG00000141510", "chrom": 17, "gene_start": 7_661_779, "gene_end": 7_687_546}
# starts where CELSR2 does: a point lookup returns it too
DECOY = {"ensg": "ENSG00000999999", "chrom": 1, "gene_start": 109_249_539, "gene_end": 109_249_999}

HBA1 = {"ensg": "ENSG00000206172", "chrom": 16, "gene_start": 176_680, "gene_end": 177_522}
HBA2 = {"ensg": "ENSG00000188536", "chrom": 16, "gene_start": 172_876, "gene_end": 173_710}
TWO_GENES = "AF-P69905-F1"

F1, F2, SMALL, NOPAE, SHARED = "AF-Q9HCU4-F1-model_v4", "AF-Q9HCU4-F2-model_v4", "AF-P04637-F1", "AF-Q00001-F1", "AF-Q00002-F1"


def release_rows():
    return [
        model_row(F2, "Q9HCU4", 2, 2, 201, 400, gene_id=CELSR2["ensg"], gene_name="CELSR2"),
        model_row(F1, "Q9HCU4", 1, 2, 1, 300, gene_id=CELSR2["ensg"], gene_name="CELSR2"),
        model_row(SMALL, "P04637", 1, 1, 1, 40, structure_bytes=0),
        model_row(NOPAE, "Q00001", 1, 1, 1, 25, gene_id="ENSG00000000001", gene_name="NOPAE1", has_pae=False),
        model_row(SHARED, "Q00002", 1, 1, 1, 7, gene_id="", gene_name=""),
        model_row(TWO_GENES, "P69905", 1, 1, 1, 9, gene_id=f"{HBA1['ensg']},{HBA2['ensg']}", gene_name="HBA1,HBA2"),
    ]


def pae_matrix(n: int) -> bytes:
    return bytes((r * 7 + c * 13) % 251 for r in range(n) for c in range(n))


def submatrix(matrix: bytes, n: int, positions: list[int]) -> bytes:
    return bytes(matrix[(a - 1) * n + (b - 1)] for a in positions for b in positions)


def anchor(gene: dict, name: str) -> list[str]:
    return [gene["ensg"], str(gene["chrom"]), str(gene["gene_start"]), str(gene["gene_end"]), name]


def residue_line(gene, name, accession, mid, pos, mu="1.5e-08") -> str:
    fields = anchor(gene, name) + [accession, mid, str(pos), str(pos), "M", "true", "-5.716", "34.290", "4.957",
                                   "39.25", mu, "9" if mu else "", "9" if mu else ""]
    return "\t".join(fields) + "\n"


def feature_line(gene, name, accession, start, end, evidence="ECO:0000269") -> str:
    tier = "experimental" if evidence else "unknown"
    return "\t".join(anchor(gene, name) + [accession, "uniprot", "Domain", str(start), str(end), "Cadherin", evidence, tier]) + "\n"


class Content:
    def __init__(self, body: bytes, gcs=None, rng=None):
        self.body, self.gcs, self.rng = body, gcs, rng

    async def iter_chunked(self, n: int):
        gcs = self.gcs
        gcs.active += 1
        gcs.peak = max(gcs.peak, gcs.active)
        try:
            if self.rng in gcs.blocked:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    gcs.cancelled.append(self.rng)
                    raise
            # deliberately not a divisor of any row length used here
            for i in range(0, len(self.body), 1013):
                # a real body arrives over several turns of the loop
                await asyncio.sleep(0)
                yield self.body[i:i + 1013]
        finally:
            gcs.active -= 1


class Resp:
    def __init__(self, status, body=b"", headers=None, content_length=None, gcs=None, rng=None):
        self.status, self.content, self.headers = status, Content(body, gcs, rng), headers or {}
        self.content_length = content_length

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def raise_for_status(self):
        assert self.status < 400, self.status


class Gcs:
    """Objects by gs:// path; answers a Range with 206 and the object's total, as GCS does."""

    closed = False

    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.requests: list[tuple[str, str | None]] = []
        self.active = self.peak = 0
        # bytes dropped from the end of every body while the headers still state the full length
        self.short = 0
        # Range values whose body never arrives, and those of them that were cancelled
        self.blocked: set[str] = set()
        self.cancelled: list[str] = []
        # Range values answered as if the object were one byte longer
        self.wrong_total: set[str] = set()

    def get(self, url, headers=None):
        path = url.replace("https://storage.googleapis.com/", "gs://")
        rng = (headers or {}).get("Range")
        self.requests.append((path, rng))
        obj = self.objects.get(path)
        if obj is None:
            return Resp(404)
        if rng is None:
            return Resp(200, obj[:len(obj) - self.short], content_length=len(obj), gcs=self)
        first, last = (int(x) for x in rng.removeprefix("bytes=").split("-"))
        if first >= len(obj):
            return Resp(416)
        part = obj[first:last + 1]
        total = len(obj) + (rng in self.wrong_total)
        return Resp(206, part[:len(part) - self.short], {"Content-Range": f"bytes {first}-{first + len(part) - 1}/{total}"},
                    len(part), gcs=self, rng=rng)

    async def _headers(self):
        return {}

    async def close(self):
        pass


class Mapping:
    def __init__(self, genes: dict[str, dict]):
        self.by_name = {name.upper(): g["ensg"] for name, g in genes.items()}
        self.gene_positions = {contract.GENCODE_VERSION: pl.DataFrame(list(genes.values()))}

    def get_ensg_ids_by_gene_name(self, name: str) -> set[str]:
        upper = name.upper()
        if upper.startswith("ENSG"):
            return {upper}
        if upper not in self.by_name:
            raise GeneNotFoundException(f"Gene {name} not found")
        return {self.by_name[upper]}


class Response:
    def __init__(self, status: int, headers: dict[str, str], body: bytes):
        self.status, self.headers, self.body = status, headers, body

    @property
    def text(self) -> str:
        return self.body.decode()

    def json(self):
        return json.loads(self.body)


def call(api, path: str, headers: dict[str, str] | None = None) -> Response:
    return asyncio.run(acall(api, path, headers))


async def acall(api, path: str, headers: dict[str, str] | None = None) -> Response:
    path, _, query = path.partition("?")
    messages, asked = [], []

    async def receive():
        if not asked:
            asked.append(True)
            return {"type": "http.request", "body": b"", "more_body": False}
        await asyncio.Event().wait()

    async def send(message):
        messages.append(message)

    scope = {
        "type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"}, "http_version": "1.1",
        "method": "GET", "scheme": "http", "path": "/api/v1/protein_structure/" + path,
        "raw_path": ("/api/v1/protein_structure/" + path).encode(), "query_string": query.encode(),
        "root_path": "", "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()],
        "client": ("testclient", 50000), "server": ("testserver", 80), "state": {},
    }
    await api(scope, receive, send)
    start = next(m for m in messages if m["type"] == "http.response.start")
    body = b"".join(bytes(m.get("body", b"")) for m in messages if m["type"] == "http.response.body")
    return Response(start["status"], {k.decode().lower(): v.decode() for k, v in start["headers"]}, body)


@pytest.fixture(autouse=True)
def _fresh_module_state(monkeypatch):
    monkeypatch.setattr(gcloud_tabix_base, "ensure_gcs_token", lambda: None)
    monkeypatch.setattr(data, "_pae_cache", type(data._pae_cache)())
    monkeypatch.setattr(data, "_pae_cache_bytes", 0)
    monkeypatch.setattr(data, "_slice_semaphore", None)
    monkeypatch.setattr(data, "_json_semaphore", None)
    monkeypatch.setattr(data, "_TABIX_RETRY_BASE_DELAY", 0)


@pytest.fixture
def gcs():
    return Gcs()


def make_release(rows, served=None) -> Release:
    models, by_accession, by_gene = parse_models(models_bytes(rows), served)
    return Release(
        pointer=Pointer(release=RELEASE, uri=BASE, contract_version=contract.CONTRACT_VERSION, manifest_sha256="0" * 64),
        manifest={}, models=models, by_accession=by_accession, by_gene=by_gene,
    )


@pytest.fixture
def service(gcs):
    release = make_release(release_rows())
    models = release.models
    s = data.ProteinStructureData()
    s._session = s._storage = gcs
    s._release = release
    s.tables = {}
    s.reading = s.peak_reading = 0
    # set to an Event to hold every read open until the test releases it
    s.gate = None

    async def stream_range(path, chroms, starts, ends, chunk_size):
        assert starts == ends
        lines = [line for line in s.tables[path] if (int(line.split("\t")[1]), int(line.split("\t")[2])) in set(zip(chroms, starts))]
        blob = "".join(lines).encode()

        async def gen():
            s.reading += 1
            s.peak_reading = max(s.peak_reading, s.reading)
            try:
                if s.gate is not None:
                    await s.gate.wait()
                # chunk boundaries fall inside lines
                for i in range(0, len(blob), 97):
                    yield blob[i:i + 97]
                await asyncio.sleep(0)
            finally:
                s.reading -= 1

        return gen()

    s._stream_range = stream_range
    for m in models.values():
        if m.has_pae:
            gcs.objects[release.object_path(m.pae_path)] = pae_matrix(m.n_residues)
    return s


@pytest.fixture
def api(service):
    app = FastAPI()
    app.include_router(routes.router, prefix="/api/v1")
    app.dependency_overrides[get_protein_structure] = lambda: service
    app.dependency_overrides[get_gene_name_mapping] = lambda: Mapping(
        {"CELSR2": CELSR2, "TP53": TP53, "DECOY": DECOY, "HBA1": HBA1, "HBA2": HBA2,
         "NOPAE1": {**TP53, "ensg": "ENSG00000000001"}}
    )
    return app


def put_structure(service, gcs, mid: str, text: bytes) -> bytes:
    stored = gzip.compress(text)
    model = service.release.models[mid]
    service.release.models[mid] = model._replace(structure_bytes=len(stored))
    gcs.objects[service.release.object_path(model.structure_path)] = stored
    return stored


ROUTES = [
    "models/Q9HCU4", "residues/Q9HCU4", "features/Q9HCU4", f"structure/{F1}", f"pae/{F1}", f"pae/{F1}/slice?positions=1",
    f"pae/{F1}/rows?positions=1",
]


class TestRegistration:
    def test_every_route_goes_through_the_pointer_switch(self, monkeypatch):
        assert routes.router in PROTEIN_STRUCTURE_ROUTERS
        monkeypatch.setattr(config, "release_pointer", "gs://b/releases/CURRENT")
        app = FastAPI()
        assert include_protein_structure(app)
        paths = {getattr(r, "path", "") for r in app.routes}
        for route in ROUTES:
            template = "/api/v1/protein_structure/" + route.split("?")[0].replace(F1, "{model_file_id}").replace("Q9HCU4", "{protein}")
            assert template in paths

    @pytest.mark.parametrize("route", ROUTES)
    def test_no_release_is_a_503(self, api, service, route):
        service._release = None
        r = call(api, route)
        assert r.status == 503 and "no protein structure release is loaded" in r.json()["detail"]


class TestModels:
    @pytest.mark.parametrize("protein", ["Q9HCU4", "q9hcu4", "CELSR2", "ENSG00000082074"])
    def test_a_protein_lists_its_files_in_window_order(self, api, protein):
        r = call(api, f"models/{protein}")
        assert r.status == 200 and r.headers["x-dataset-version"] == RELEASE
        rows = r.json()
        assert [m["model_file_id"] for m in rows] == [F1, F2]
        assert r.headers["x-columns"].split(",") == list(rows[0])
        assert (rows[1]["protein_start"], rows[1]["n_residues"], rows[1]["has_pae"], rows[1]["pae_bytes"]) == (201, 200, True, 40000)

    def test_a_model_without_pae_says_so(self, api):
        (m,) = call(api, "models/Q00001").json()
        assert (m["has_pae"], m["pae_bytes"]) == (False, None)

    def test_unknown_names_are_404(self, api):
        assert call(api, "models/NOSUCH").status == 404
        r = call(api, "models/DECOY")
        assert r.status == 404 and "No model file is listed under gene" in r.json()["detail"]

    @pytest.mark.parametrize("protein", ["HBA1", "HBA2", "P69905"])
    def test_a_protein_under_several_genes_is_listed_under_each(self, api, protein):
        (m,) = call(api, f"models/{protein}").json()
        assert (m["model_file_id"], m["gene_id"], m["gene_name"]) == (TWO_GENES, f"{HBA1['ensg']},{HBA2['ensg']}", "HBA1,HBA2")


class TestRows:
    @pytest.fixture(autouse=True)
    def _tables(self, service):
        service.tables[f"{BASE}/{contract.RESIDUES_FILE}"] = [
            residue_line(CELSR2, "CELSR2", "Q9HCU4", F1, 1, mu=""),
            residue_line(CELSR2, "CELSR2", "Q9HCU4", F1, 2),
            residue_line(CELSR2, "CELSR2", "A0A000", "AF-A0A000-F1", 1),
            residue_line(DECOY, "DECOY", "Q9HCU4", F1, 1),
        ]
        service.tables[f"{BASE}/{contract.FEATURES_FILE}"] = [
            feature_line(CELSR2, "CELSR2", "Q9HCU4", 10, 90),
            feature_line(CELSR2, "CELSR2", "Q9HCU4", 100, 190, evidence=""),
        ]

    def test_a_gene_gets_its_residue_rows_and_no_other_gene_starting_there(self, api):
        r = call(api, "residues/CELSR2")
        assert r.status == 200 and r.headers["content-type"].startswith("text/tab-separated-values")
        assert r.headers["x-dataset-version"] == RELEASE
        header, *rows = r.text.splitlines()
        assert "#" + header + "\n" == contract.header_line(contract.RESIDUE_COLUMNS)
        # A0A000 has rows in the file and no model in the index: not served
        assert [(x.split("\t")[0], x.split("\t")[5]) for x in rows] == [(CELSR2["ensg"], "Q9HCU4")] * 2
        # the release writes null as an empty cell, this API as NA
        assert rows[0].split("\t")[-3:] == ["NA", "NA", "NA"] and rows[1].split("\t")[-3:] == ["1.5e-08", "9", "9"]

    def test_an_accession_gets_only_its_own_rows(self, api):
        rows = call(api, "residues/Q9HCU4").text.splitlines()[1:]
        assert len(rows) == 2 and {x.split("\t")[5] for x in rows} == {"Q9HCU4"}

    def test_an_accession_under_several_genes_gets_its_rows_under_each(self, api, service):
        service.tables[f"{BASE}/{contract.RESIDUES_FILE}"] = [
            residue_line(HBA2, "HBA2", "P69905", TWO_GENES, 1),
            residue_line(HBA1, "HBA1", "P69905", TWO_GENES, 1),
        ]
        by_accession = call(api, "residues/P69905").text.splitlines()[1:]
        assert sorted(x.split("\t")[4] for x in by_accession) == ["HBA1", "HBA2"]
        by_gene = call(api, "residues/HBA1").text.splitlines()[1:]
        assert [x.split("\t")[4] for x in by_gene] == ["HBA1"]

    def test_json_rows_are_typed_and_carry_their_columns(self, api):
        r = call(api, "residues/Q9HCU4?format=json")
        assert r.status == 200 and r.headers["content-type"] == "application/json"
        assert r.headers["x-columns"].split(",") == [c for c, _ in contract.RESIDUE_COLUMNS]
        first, second = r.json()
        assert list(first) == [c for c, _ in contract.RESIDUE_COLUMNS]
        assert (first["chrom"], first["file_position"], first["is_owner"], first["ca_x"], first["mu"], first["n_possible"]) == (1, 1, True, -5.716, None, None)
        assert (second["mu"], second["n_with_mu"]) == (1.5e-08, 9)

    def test_features(self, api):
        r = call(api, "features/CELSR2")
        header, *rows = r.text.splitlines()
        assert "#" + header + "\n" == contract.header_line(contract.FEATURE_COLUMNS)
        assert [x.split("\t")[8:10] for x in rows] == [["10", "90"], ["100", "190"]]
        assert rows[1].split("\t")[-2:] == ["NA", "unknown"]
        assert call(api, "features/Q9HCU4?format=json").json()[1]["evidence"] is None

    @pytest.mark.parametrize("kind", ["residues", "features"])
    def test_what_has_no_rows_is_a_404(self, api, kind):
        for protein, why in (("TP53", "No "), ("Q00002", "listed under no gene"), ("NOSUCH", "neither"), ("ENSG00000123456", "GENCODE")):
            r = call(api, f"{kind}/{protein}")
            assert r.status == 404 and why in r.json()["detail"], protein

    def test_a_row_of_another_width_is_not_served(self, api, service):
        service.tables[f"{BASE}/{contract.RESIDUES_FILE}"] = [residue_line(CELSR2, "CELSR2", "Q9HCU4", F1, 1).replace("\tM\t", "\t")]
        assert call(api, "residues/CELSR2").status == 500

    def test_the_largest_gene_the_build_allows_fits_one_response(self, api, service):
        # worst case for the NA rewrite: every nullable cell of every row empty
        line = residue_line(CELSR2, "CELSR2", "Q9HCU4", F1, 30000, mu="")
        n = BUILD_MAX_GENE_RESIDUE_BYTES // len(line)
        service.tables[f"{BASE}/{contract.RESIDUES_FILE}"] = [line] * n
        r = call(api, "residues/CELSR2")
        assert r.status == 200 and r.body.count(b"\n") == n + 1
        assert BUILD_MAX_GENE_RESIDUE_BYTES < len(r.body) < contract.MAX_RESPONSE_BYTES
        # as JSON the same rows pass the limit, and are refused rather than sent
        r = call(api, "residues/CELSR2?format=json")
        assert r.status == 413 and "format=tsv" in r.json()["detail"]

    def test_rows_of_an_accession_outside_the_index_are_never_served(self, api, service):
        extra = model_row("AF3-A0A000-F1", "A0A000", 1, 1, 1, 30, gene_id=CELSR2["ensg"], gene_name="CELSR2",
                          licence="AlphaFold Server output terms")
        service.tables[f"{BASE}/{contract.FEATURES_FILE}"].append(feature_line(CELSR2, "CELSR2", "A0A000", 1, 9))

        def accessions(route):
            r = call(api, route)
            return (r.status, sorted({x.split("\t")[5] for x in r.text.splitlines()[1:]})) if r.status == 200 else (r.status, [])

        gene_routes = ["residues/CELSR2", f"residues/{CELSR2['ensg']}", "features/CELSR2", f"features/{CELSR2['ensg']}"]
        service._release = make_release(release_rows() + [extra])
        for route in gene_routes:
            assert accessions(route) == (200, ["A0A000", "Q9HCU4"]), route
        assert accessions("residues/A0A000") == (200, ["A0A000"])

        service._release = make_release(release_rows() + [extra], frozenset({"CC-BY-4.0"}))
        for route in gene_routes:
            assert accessions(route) == (200, ["Q9HCU4"]), route
            assert [r["accession"] for r in call(api, route + "?format=json").json()].count("A0A000") == 0
        for route in ("models/A0A000", "residues/A0A000", "features/A0A000", "structure/AF3-A0A000-F1", "pae/AF3-A0A000-F1"):
            assert call(api, route).status == 404, route

    def test_a_gene_whose_every_row_is_outside_the_index_is_a_404(self, api, service):
        service.tables[f"{BASE}/{contract.RESIDUES_FILE}"] = [residue_line(CELSR2, "CELSR2", "A0A000", "AF-A0A000-F1", 1)]
        assert call(api, "residues/CELSR2").status == 404

    def test_a_float_json_cannot_state_is_null(self, api, service):
        line = residue_line(CELSR2, "CELSR2", "Q9HCU4", F1, 1, mu="nan").replace("\t39.25\t", "\tinf\t").replace("\t-5.716\t", "\t-Infinity\t")
        service.tables[f"{BASE}/{contract.RESIDUES_FILE}"] = [line]
        r = call(api, "residues/CELSR2?format=json")
        assert r.status == 200 and b"NaN" not in r.body and b"Infinity" not in r.body
        (row,) = json.loads(r.body, parse_constant=lambda name: pytest.fail(name))
        assert (row["mu"], row["plddt"], row["ca_x"], row["ca_y"]) == (None, None, None, 34.29)

    @pytest.mark.parametrize("n_rows", [1, 2, 5])
    def test_a_json_body_exactly_at_the_limit_is_served_and_one_byte_over_is_not(self, n_rows):
        fields = [residue_line(CELSR2, "CELSR2", "Q9HCU4", F1, i).rstrip("\n").encode().split(b"\t") for i in range(n_rows)]

        async def build(max_bytes):
            async def rows():
                for f in fields:
                    yield f
            return await data.json_body(data.RESIDUES, rows(), max_bytes)

        body = asyncio.run(build(10**6))
        assert body == json.dumps(json.loads(body), separators=(",", ":")).encode() and len(json.loads(body)) == n_rows
        assert asyncio.run(build(len(body))) == body
        with pytest.raises(data.ResponseTooLarge):
            asyncio.run(build(len(body) - 1))

    def test_only_so_many_json_bodies_are_built_at_once(self, api, service):
        async def go():
            # reads stay open until every request is accounted for: how far a request has got
            # when another finishes depends on the threadpool that resolves its dependencies
            service.gate = asyncio.Event()
            tasks = [asyncio.ensure_future(acall(api, "residues/CELSR2?format=json")) for _ in range(5)]

            def waiting() -> int:
                slot = data._json_semaphore
                return len(slot._waiters or ()) if slot is not None else 0

            async with asyncio.timeout(10):
                while service.reading + waiting() < 5:
                    await asyncio.sleep(0.001)
            held = (service.reading, waiting())
            service.gate.set()
            return held, await asyncio.gather(*tasks)

        held, responses = asyncio.run(go())
        # the slot is taken before the first read, so the reads show the bound
        assert held == (2, 3)
        assert [r.status for r in responses] == [200] * 5 and len({r.body for r in responses}) == 1
        assert service.peak_reading == 2

    def test_the_limit_is_the_one_the_sandbox_cap_enforces(self):
        if "SANDBOX_MAX_RESPONSE_BYTES" in os.environ:
            pytest.skip("the cap is overridden in this environment")
        assert contract.MAX_RESPONSE_BYTES == limits.SANDBOX_MAX_RESPONSE_BYTES


class TestStructure:
    PDB = b"HEADER    TEST\nATOM      1  CA  MET A   1\n" * 50

    def test_the_stored_gzip_is_passed_through_to_a_client_that_takes_it(self, api, service, gcs):
        stored = put_structure(service, gcs, F1, self.PDB)
        r = call(api, f"structure/{F1}", {"Accept-Encoding": "gzip, deflate, br"})
        assert r.status == 200 and r.body == stored
        assert r.headers["content-encoding"] == "gzip" and r.headers["content-type"] == "chemical/x-pdb"
        assert r.headers["content-length"] == str(len(stored)) and r.headers["vary"] == "Accept-Encoding"
        assert r.headers["cache-control"] == "private, no-cache" and RELEASE in r.headers["etag"]
        assert r.headers["x-dataset-version"] == RELEASE

    def test_a_client_that_does_not_take_gzip_gets_the_text(self, api, service, gcs):
        put_structure(service, gcs, F1, self.PDB)
        r = call(api, f"structure/{F1}")
        assert r.status == 200 and r.body == self.PDB and r.headers["content-encoding"] == "identity"
        assert r.headers["etag"] != call(api, f"structure/{F1}", {"Accept-Encoding": "gzip"}).headers["etag"]

    @pytest.mark.parametrize("header,takes", [
        ("gzip", True), ("gzip, deflate, br", True), ("br, GZIP ; q=0.5", True), ("*", True), ("deflate, *;q=0.1", True),
        ("gzip;q=0", False), ("gzip; q=0.0, *", False), ("xgzipx", False), ("x-gzip-like, br", False), ("identity", False),
        ("*;q=0", False), ("", False), ("gzip;q=abc", False),
    ])
    def test_accept_encoding_is_read_as_a_list_of_codings(self, header, takes):
        assert routes._accepts_gzip(header) is takes

    @pytest.mark.parametrize("header", ["gzip;q=0", "xgzipx", "identity"])
    def test_a_client_that_refuses_gzip_gets_text_through_the_gzip_middleware_too(self, api, service, gcs, header):
        put_structure(service, gcs, F1, self.PDB)
        r = call(GZipMiddleware(api, minimum_size=100), f"structure/{F1}", {"Accept-Encoding": header})
        assert r.status == 200 and r.body == self.PDB and r.headers["content-encoding"] == "identity"
        assert r.headers["vary"] == "Accept-Encoding"

    def test_a_stored_object_that_is_not_gzip_is_an_index_mismatch(self, api, service, gcs):
        model = service.release.models[F1]
        service.release.models[F1] = model._replace(structure_bytes=len(self.PDB))
        gcs.objects[service.release.object_path(model.structure_path)] = self.PDB
        r = call(api, f"structure/{F1}")
        assert r.status == 500 and "does not match the release index" in r.json()["detail"]

    def test_a_model_file_that_inflates_past_the_bound_is_not_served(self, api, service, gcs, monkeypatch):
        put_structure(service, gcs, F1, self.PDB)
        monkeypatch.setattr(routes, "MAX_STRUCTURE_TEXT_BYTES", len(self.PDB))
        assert call(api, f"structure/{F1}").body == self.PDB
        monkeypatch.setattr(routes, "MAX_STRUCTURE_TEXT_BYTES", len(self.PDB) - 1)
        r = call(api, f"structure/{F1}")
        assert r.status == 500 and "does not match the release index" in r.json()["detail"]
        # truncated inside the gzip stream, and two members back to back
        assert data.inflate_structure(gzip.compress(b"a") + gzip.compress(b"b"), 2) == b"ab"
        with pytest.raises(data.ObjectMismatch):
            data.inflate_structure(gzip.compress(self.PDB)[:-9], 10**6)

    def test_a_body_shorter_than_its_headers_say_is_not_served(self, api, service, gcs):
        put_structure(service, gcs, F1, self.PDB)
        gcs.short = 1
        assert call(api, f"structure/{F1}", {"Accept-Encoding": "gzip"}).status == 500

    def test_a_current_copy_is_not_sent_again(self, api, service, gcs):
        put_structure(service, gcs, F1, self.PDB)
        etag = call(api, f"structure/{F1}", {"Accept-Encoding": "gzip"}).headers["etag"]
        gcs.requests.clear()
        r = call(api, f"structure/{F1}", {"Accept-Encoding": "gzip", "If-None-Match": etag})
        assert r.status == 304 and r.body == b"" and gcs.requests == []
        assert call(api, f"structure/{F1}", {"Accept-Encoding": "gzip", "If-None-Match": '"other"'}).status == 200

    @pytest.mark.parametrize("mid", ["nope", "..", "AF-Q9HCU4-F1-model_v4.pdb.gz", "%2e%2e%2fmanifest.json"])
    def test_an_id_outside_the_index_reads_nothing(self, api, gcs, mid):
        assert call(api, f"structure/{mid}").status == 404
        assert call(api, f"pae/{mid}").status == 404
        assert gcs.requests == []

    def test_an_object_of_another_size_is_not_served(self, api, service, gcs):
        put_structure(service, gcs, F1, self.PDB)
        path = service.release.object_path(service.release.models[F1].structure_path)
        gcs.objects[path] += b"x"
        r = call(api, f"structure/{F1}", {"Accept-Encoding": "gzip"})
        assert r.status == 500 and "does not match the release index" in r.json()["detail"]
        del gcs.objects[path]
        assert call(api, f"structure/{F1}", {"Accept-Encoding": "gzip"}).status == 500


class TestPae:
    def test_the_whole_object(self, api, gcs):
        r = call(api, f"pae/{F1}")
        assert r.status == 200 and r.body == pae_matrix(300)
        assert r.headers["content-type"] == "application/octet-stream" and r.headers["x-pae-size"] == "300"
        assert r.headers["content-length"] == "90000" and r.headers["cache-control"] == "private, no-cache"
        gcs.requests.clear()
        assert call(api, f"pae/{F1}", {"If-None-Match": r.headers["etag"]}).status == 304 and gcs.requests == []

    @pytest.mark.parametrize("route", [f"pae/{NOPAE}", f"pae/{NOPAE}/slice?positions=1-3", f"pae/{NOPAE}/rows?positions=1-3"])
    def test_a_model_without_pae_is_a_404_that_says_why(self, api, gcs, route):
        r = call(api, route)
        assert r.status == 404 and "has no PAE" in r.json()["detail"] and "Q00001" in r.json()["detail"]
        assert gcs.requests == []

    @pytest.mark.parametrize("route", [f"pae/{F1}", f"pae/{F1}/slice?positions=1-3", f"pae/{F1}/rows?positions=1-3"])
    @pytest.mark.parametrize("cached_path", [True, False])
    def test_an_object_of_another_size_is_not_served(self, api, service, gcs, monkeypatch, route, cached_path):
        if not cached_path:
            monkeypatch.setattr(data, "_PAE_CACHE_OBJECT_MAX", 0)
        path = service.release.object_path(service.release.models[F1].pae_path)
        gcs.objects[path] = gcs.objects[path][:-300]
        r = call(api, route)
        assert r.status == 500 and "does not match the release index" in r.json()["detail"]
        assert data._pae_cache == {}

    def test_a_slice_is_the_submatrix_of_the_positions_in_ascending_order(self, api, gcs):
        r = call(api, f"pae/{F1}/slice?positions=250,7,100-104,7")
        positions = [7, 100, 101, 102, 103, 104, 250]
        assert r.status == 200 and r.headers["x-pae-size"] == "7"
        assert r.body == submatrix(pae_matrix(300), 300, positions)
        # the object is small enough to keep: one whole read, then none
        assert gcs.requests == [(f"{BASE}/pae/{F1}.u8", None)]
        assert call(api, f"pae/{F1}/slice?positions=1-300").body == pae_matrix(300)
        assert len(gcs.requests) == 1

    def test_a_large_object_is_sliced_by_range_reads_and_not_kept(self, api, gcs, monkeypatch):
        monkeypatch.setattr(data, "_PAE_CACHE_OBJECT_MAX", 1000)
        positions = [1, 2, 3, 50, 51, 299, 300]
        r = call(api, f"pae/{F1}/slice?positions=1-3,50-51,299-300")
        assert r.status == 200 and r.body == submatrix(pae_matrix(300), 300, positions)
        assert sorted(rng for _, rng in gcs.requests) == ["bytes=0-899", "bytes=14700-15299", "bytes=89400-89999"]
        assert data._pae_cache == {}

    def test_scattered_positions_cost_a_bounded_number_of_reads(self, api, gcs, monkeypatch):
        monkeypatch.setattr(data, "_PAE_CACHE_OBJECT_MAX", 1000)
        positions = list(range(1, 300, 3))
        r = call(api, f"pae/{F1}/slice?positions=" + ",".join(map(str, positions)))
        assert r.body == submatrix(pae_matrix(300), 300, positions)
        assert len(gcs.requests) == 16

    @pytest.mark.parametrize("positions", ["", "abc", "0", "301", "5-2", "1,,2", "-3", "1.5", "²", "9" * 5000, "1-" + "9" * 5000])
    @pytest.mark.parametrize("kind", ["slice", "rows"])
    def test_bad_positions_are_422(self, api, gcs, positions, kind):
        assert call(api, f"pae/{F1}/{kind}?positions={positions}").status == 422
        assert gcs.requests == []

    def test_positions_given_twice_count_once(self):
        most = contract.pae_rows_per_request(5000)
        assert data.parse_positions(f"1-{most},1", 5000) == list(range(1, most + 1))
        assert data.parse_positions(f"1-{most},{most - 10}-{most},7", 5000) == list(range(1, most + 1))
        assert data.parse_positions("9,3-5,4-6,3", 300) == [3, 4, 5, 6, 9]
        with pytest.raises(ParseException, match="more than"):
            data.parse_positions(f"1-{most},{most - 10}-{most + 1}", 5000)

    @pytest.mark.parametrize("cached_path", [True, False])
    def test_rows_are_the_whole_rows_of_the_positions_in_ascending_order(self, api, gcs, monkeypatch, cached_path):
        if not cached_path:
            monkeypatch.setattr(data, "_PAE_CACHE_OBJECT_MAX", 1000)
        matrix = pae_matrix(300)
        r = call(api, f"pae/{F1}/rows?positions=250,7,100-101,7")
        assert r.status == 200 and (r.headers["x-pae-size"], r.headers["x-pae-rows"]) == ("300", "4")
        assert r.body == b"".join(matrix[(p - 1) * 300:p * 300] for p in (7, 100, 101, 250))
        assert r.headers["content-type"] == "application/octet-stream" and r.headers["x-dataset-version"] == RELEASE
        expected = [None] if cached_path else ["bytes=1800-2099", "bytes=29700-30299", "bytes=74700-74999"]
        assert sorted((rng for _, rng in gcs.requests), key=str) == expected

    def test_a_failed_range_read_cancels_the_others(self, service, gcs, monkeypatch):
        monkeypatch.setattr(data, "_PAE_CACHE_OBJECT_MAX", 1000)
        gcs.blocked.add("bytes=0-899")
        gcs.wrong_total.add("bytes=89400-89999")
        release = service.release

        async def go():
            with pytest.raises(data.ObjectMismatch):
                await service.pae_slice(release, release.models[F1], [1, 2, 3, 299, 300])
            # read before the loop closes, which would cancel a stray task anyway
            return list(gcs.cancelled), gcs.active

        assert asyncio.run(go()) == (["bytes=0-899"], 0)

    def test_only_so_many_slices_read_at_once(self, service, gcs, monkeypatch):
        monkeypatch.setattr(data, "_PAE_CACHE_OBJECT_MAX", 1000)
        release = service.release

        async def go():
            return await asyncio.gather(*(service.pae_slice(release, release.models[F1], [1, 2, 3]) for _ in range(7)))

        bodies = asyncio.run(go())
        assert set(bodies) == {submatrix(pae_matrix(300), 300, [1, 2, 3])}
        assert len(gcs.requests) == 7 and gcs.peak == 4

    @pytest.mark.parametrize("cached_path", [True, False])
    def test_a_body_shorter_than_its_headers_say_is_not_served(self, api, service, gcs, monkeypatch, cached_path):
        if not cached_path:
            monkeypatch.setattr(data, "_PAE_CACHE_OBJECT_MAX", 1000)
        gcs.short = 1
        for kind in ("slice", "rows"):
            assert call(api, f"pae/{F1}/{kind}?positions=1-3").status == 500
        assert data._pae_cache == {}
        release = service.release

        async def whole():
            return [chunk async for chunk in await service.open_pae(release, release.models[F1])]

        with pytest.raises(data.ObjectMismatch, match="arrived"):
            asyncio.run(whole())

    @pytest.mark.parametrize("route", [f"pae/{F1}", f"pae/{F1}/slice?positions=1-300", f"pae/{F1}/rows?positions=1-300"])
    def test_pae_bodies_pass_the_gzip_middleware_untouched(self, api, route):
        direct = call(api, route)
        r = call(GZipMiddleware(api, minimum_size=1000), route, {"Accept-Encoding": "gzip"})
        assert r.status == 200 and r.headers["content-encoding"] == "identity"
        assert r.body == direct.body and len(r.body) == 90000 and r.headers["content-length"] == "90000"

    def test_more_positions_than_one_response_may_hold_are_refused(self):
        n = 5000
        most = contract.pae_rows_per_request(n)
        assert len(data.parse_positions(f"1-{most}", n)) == most
        # the full-width rows of the most positions allowed are what a slice holds
        assert most * n <= contract.MAX_RESPONSE_BYTES < (most + 1) * n
        for text in (f"1-{most + 1}", f"1-{most},{n}"):
            with pytest.raises(ParseException, match="more than"):
                data.parse_positions(text, n)


class TestPaeCache:
    def test_it_is_bounded_in_bytes_and_drops_the_least_recently_used(self, monkeypatch):
        monkeypatch.setattr(data, "_PAE_CACHE_BYTES", 250)
        for name in "abc":
            data._pae_cache_put(name, bytes(100))
        assert list(data._pae_cache) == ["b", "c"] and data._pae_cache_bytes == 200
        assert data._pae_cache_get("b") is not None
        data._pae_cache_put("d", bytes(100))
        assert list(data._pae_cache) == ["b", "d"] and data._pae_cache_bytes == 200
        data._pae_cache_put("big", bytes(251))
        assert "big" not in data._pae_cache and data._pae_cache_bytes == 200

    def test_exactly_the_bound_is_kept(self, monkeypatch):
        monkeypatch.setattr(data, "_PAE_CACHE_BYTES", 300)
        for name in "abc":
            data._pae_cache_put(name, bytes(100))
        assert list(data._pae_cache) == ["a", "b", "c"] and data._pae_cache_bytes == 300
        monkeypatch.setattr(data, "_pae_cache", type(data._pae_cache)())
        monkeypatch.setattr(data, "_pae_cache_bytes", 0)
        data._pae_cache_put("whole", bytes(300))
        assert list(data._pae_cache) == ["whole"] and data._pae_cache_bytes == 300

    def test_two_misses_of_one_object_count_it_once(self, service, gcs):
        release = service.release

        async def go():
            return await asyncio.gather(*(service.pae_slice(release, release.models[F1], [1, 2]) for _ in range(2)))

        a, b = asyncio.run(go())
        assert a == b == submatrix(pae_matrix(300), 300, [1, 2])
        # both missed and both read; the second put must not count the object again
        assert len(gcs.requests) == 2
        assert list(data._pae_cache) == [f"{BASE}/pae/{F1}.u8"] and data._pae_cache_bytes == 90000

    def test_zero_keeps_nothing(self, api, gcs, monkeypatch):
        monkeypatch.setattr(data, "_PAE_CACHE_BYTES", 0)
        assert call(api, f"pae/{SMALL}/slice?positions=2,4").body == submatrix(pae_matrix(40), 40, [2, 4])
        assert data._pae_cache == {}
        assert sorted(gcs.requests) == [(f"{BASE}/pae/{SMALL}.u8", "bytes=120-159"), (f"{BASE}/pae/{SMALL}.u8", "bytes=40-79")]


class TestPlanning:
    def test_runs_are_merged_across_the_smallest_gaps(self):
        rows = [0, 1, 2, 10, 11, 13, 50, 90, 91]
        assert data.plan_row_reads(rows, 16) == [(0, 3), (10, 12), (13, 14), (50, 51), (90, 92)]
        assert data.plan_row_reads(rows, 3) == [(0, 14), (50, 51), (90, 92)]
        assert data.plan_row_reads(rows, 1) == [(0, 92)]
