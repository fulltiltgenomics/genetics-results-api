"""Offline tests of the protein structure release pointer, loader and startup check.

The release is a small one built per test from the contract mirror: two accessions, one split
into two windows and one without PAE. GCS reads are replaced by local ones.
"""

import asyncio
import gzip
import hashlib
import importlib
import importlib.util
import json
import logging
import os
import zlib
from pathlib import Path

import pytest
from fastapi import APIRouter, FastAPI

import app.config.protein_structure as config
import app.config.protein_structure_contract as contract
from app.services import gcloud_tabix_base, startup_checks
from app.services import protein_structure as svc
from app.services.protein_structure import (
    ContractCheckFailed,
    ProteinStructureAccess,
    ReleaseUnavailable,
    load_release,
)

POINTER = "gs://test-bucket/protein_structure/releases/CURRENT"
RELEASE = "20261112-120000"
AF3_LICENCE = "LicenseRef-AlphaFold-Server-output-terms"


def model_row(mid, accession, window, n_windows, start, end, gene_id="ENSG00000141510",
              gene_name="TP53", licence="CC-BY-4.0", has_pae=True, **override):
    n = end - start + 1
    row = {
        "model_file_id": mid, "accession": accession, "uniprot_isoform": accession,
        "gene_id": gene_id, "gene_name": gene_name, "source_gene_symbol": gene_name,
        "source": "AF2DB", "source_label": "AlphaFold DB", "model_version": "v4",
        "licence": licence, "window_index": window, "n_windows": n_windows,
        "protein_start": start, "protein_end": end, "n_residues": n,
        "owned_start": start, "owned_end": end, "has_pae": "true" if has_pae else "false",
        "structure_path": contract.structure_path(mid), "structure_sha256": "a" * 64,
        "structure_bytes": 1234,
        "pae_path": contract.pae_path(mid) if has_pae else "",
        "pae_sha256": "b" * 64 if has_pae else "",
        "pae_bytes": n * n if has_pae else "",
        "source_pdb_sha256": "a" * 64, "source_pae_sha256": "c" * 64 if has_pae else "",
    }
    row.update(override)
    return row


def default_rows():
    return [
        model_row("AF-Q9HCU4-F2-model_v4", "Q9HCU4", 2, 2, 201, 1400, gene_id="ENSG00000082074", gene_name="CELSR2"),
        model_row("AF-Q9HCU4-F1-model_v4", "Q9HCU4", 1, 2, 1, 1400, gene_id="ENSG00000082074", gene_name="CELSR2"),
        model_row("AF3-P04637", "P04637", 1, 1, 1, 393, licence=AF3_LICENCE, has_pae=False),
    ]


def models_bytes(rows, header=None) -> bytes:
    lines = [header if header is not None else contract.header_line(contract.MODEL_COLUMNS)]
    for r in rows:
        lines.append("\t".join(str(r[c]) for c, _ in contract.MODEL_COLUMNS) + "\n")
    return gzip.compress("".join(lines).encode())


class FakeGcs:
    """gs://test-bucket/... -> root/...; a missing file raises as _fetch_full does."""

    def __init__(self, root: Path):
        self.root = root

    def local(self, gs_path: str) -> Path:
        return self.root / gs_path.removeprefix("gs://test-bucket/")

    def read(self, gs_path: str, max_bytes: int) -> bytes:
        with self.local(gs_path).open("rb") as f:
            return f.read(max_bytes + 1)

    async def fetch(self, gs_path: str, max_bytes: int) -> bytes:
        return self.read(gs_path, max_bytes)

    def put(self, gs_path: str, data: bytes) -> None:
        p = self.local(gs_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    def publish(self, rows=None, header=None, manifest=None, pointer_version=None, manifest_sha=None,
                release=RELEASE, models_raw=None):
        base = f"gs://test-bucket/protein_structure/releases/{release}"
        manifest_raw = json.dumps(
            manifest if manifest is not None else {"contract_version": contract.CONTRACT_VERSION, "release": release}
        ).encode()
        if models_raw is None:
            models_raw = models_bytes(rows if rows is not None else default_rows(), header)
        self.put(f"{base}/{contract.MODELS_FILE}", models_raw)
        self.put(f"{base}/{contract.MANIFEST_FILE}", manifest_raw)
        pointer = json.loads(_munge_pointer(release, base, hashlib.sha256(manifest_raw).hexdigest()))
        if pointer_version is not None:
            pointer["contract_version"] = pointer_version
        if manifest_sha is not None:
            pointer["manifest_sha256"] = manifest_sha
        self.put(POINTER, json.dumps(pointer).encode())
        return base


def _munge_pointer(release, uri, sha):
    # the shape of the munge contract's pointer(); the pin test below compares the two
    return json.dumps({"release": release, "uri": uri, "written_at": "2026-11-12T12:00:00Z",
                       "contract_version": contract.CONTRACT_VERSION, "manifest_sha256": sha}, indent=1)


@pytest.fixture
def gcs(tmp_path):
    return FakeGcs(tmp_path)


def load(gcs, served=None):
    return asyncio.run(load_release(POINTER, gcs.fetch, served))


def refused(gcs, served=None) -> ContractCheckFailed:
    with pytest.raises(ContractCheckFailed) as e:
        load(gcs, served)
    return e.value


class TestContractMirror:
    def test_pinned_values(self):
        # a change here is a contract change: bump CONTRACT_VERSION in munge first
        assert contract.CONTRACT_VERSION == 1
        assert contract.header_line(contract.MODEL_COLUMNS) == (
            "#model_file_id\taccession\tuniprot_isoform\tgene_id\tgene_name\tsource_gene_symbol"
            "\tsource\tsource_label\tmodel_version\tlicence\twindow_index\tn_windows\tprotein_start"
            "\tprotein_end\tn_residues\towned_start\towned_end\thas_pae\tstructure_path"
            "\tstructure_sha256\tstructure_bytes\tpae_path\tpae_sha256\tpae_bytes"
            "\tsource_pdb_sha256\tsource_pae_sha256\n"
        )
        assert (contract.MANIFEST_FILE, contract.MODELS_FILE) == ("manifest.json", "models.tsv.gz")
        assert contract.structure_path("X") == "structures/X.pdb.gz"
        assert contract.pae_path("X") == "pae/X.u8"

    def test_matches_the_munge_contract_when_a_checkout_is_found(self):
        candidates = [os.environ.get("PROTEIN_STRUCTURE_MUNGE_CONTRACT", "")] + [
            str(p / "genetics-results-munge" / "scripts" / "protein_structure_contract.py")
            for p in Path(__file__).resolve().parents
        ]
        found = next((c for c in candidates if c and Path(c).is_file()), None)
        if found is None:
            pytest.skip("no genetics-results-munge checkout; set PROTEIN_STRUCTURE_MUNGE_CONTRACT")
        spec = importlib.util.spec_from_file_location("_munge_protein_structure_contract", found)
        munge = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(munge)
        for name in ("CONTRACT_VERSION", "MANIFEST_FILE", "MODELS_FILE", "RESIDUES_FILE",
                     "FEATURES_FILE", "VARIANTS_FILE", "STRUCTURE_DIR", "PAE_DIR", "MODEL_COLUMNS"):
            assert getattr(contract, name) == getattr(munge, name), name
        assert contract.MODEL_FILE_ID_RE.pattern == munge.MODEL_FILE_ID_RE.pattern
        assert contract.header_line(contract.MODEL_COLUMNS) == munge.header_line(munge.MODEL_COLUMNS)
        assert json.loads(munge.pointer(RELEASE, "gs://x", "2026-11-12T12:00:00Z", "a" * 64)).keys() == json.loads(
            _munge_pointer(RELEASE, "gs://x", "a" * 64)
        ).keys()


class TestConfig:
    @pytest.mark.parametrize("profile,env,expected", [
        ("daly", "gs://b/protein_structure_staging/releases/CURRENT", "gs://b/protein_structure_staging/releases/CURRENT"),
        ("daly", "", None),
        ("daly", None, None),
        # finngen has no protein_structure module, so even a set env cannot switch it on
        ("finngen", "gs://b/protein_structure/releases/CURRENT", None),
        ("finngen", None, None),
    ])
    def test_pointer_resolution(self, monkeypatch, profile, env, expected):
        from app.config import profile as profile_loader

        monkeypatch.setattr(profile_loader, "CONFIG_PROFILE", profile)
        if env is None:
            monkeypatch.delenv("PROTEIN_STRUCTURE_RELEASE_POINTER", raising=False)
        else:
            monkeypatch.setenv("PROTEIN_STRUCTURE_RELEASE_POINTER", env)
        try:
            assert importlib.reload(config).release_pointer == expected
        finally:
            monkeypatch.undo()
            importlib.reload(config)

    def test_daly_names_no_pointer(self):
        from app.config.profiles.daly import protein_structure as daly

        assert daly.release_pointer is None

    @pytest.mark.parametrize("pointer,registered", [(None, False), (POINTER, True)])
    def test_routes_exist_only_where_a_release_is_named(self, monkeypatch, pointer, registered):
        from app.server import include_protein_structure

        router = APIRouter()

        @router.get("/protein_structure/probe")
        async def probe():
            return {}

        monkeypatch.setattr(config, "release_pointer", pointer)
        app = FastAPI()
        assert include_protein_structure(app, (router,)) is registered
        paths = {getattr(r, "path", None) for r in app.routes}
        assert ("/api/v1/protein_structure/probe" in paths) is registered


class TestLoader:
    def test_valid_release_is_indexed(self, gcs):
        base = gcs.publish()
        r = load(gcs)
        assert r.name == RELEASE and r.pointer.base == base
        assert set(r.models) == {"AF-Q9HCU4-F1-model_v4", "AF-Q9HCU4-F2-model_v4", "AF3-P04637"}
        assert r.by_accession["Q9HCU4"] == ("AF-Q9HCU4-F1-model_v4", "AF-Q9HCU4-F2-model_v4")
        assert r.by_gene == {"ENSG00000082074": ("Q9HCU4",), "ENSG00000141510": ("P04637",)}
        m = r.models["AF-Q9HCU4-F2-model_v4"]
        assert (m.protein_start, m.n_residues, m.has_pae, m.pae_bytes) == (201, 1200, True, 1200 * 1200)
        assert m.structure_path == "structures/AF-Q9HCU4-F2-model_v4.pdb.gz"
        nopae = r.models["AF3-P04637"]
        assert (nopae.has_pae, nopae.pae_bytes, nopae.pae_path) == (False, None, None)

    def test_an_empty_gene_id_is_not_reachable_by_gene(self, gcs):
        gcs.publish(rows=[model_row("AF-Q0-F1-model_v4", "Q0", 1, 1, 1, 10, gene_id="", gene_name="")])
        r = load(gcs)
        assert r.by_gene == {} and r.models["AF-Q0-F1-model_v4"].gene_id is None

    def test_an_unserved_licence_leaves_the_accession_out(self, gcs):
        gcs.publish()
        r = load(gcs, served=frozenset({"CC-BY-4.0"}))
        assert "P04637" not in r.by_accession and "AF3-P04637" not in r.models
        assert "ENSG00000141510" not in r.by_gene and "Q9HCU4" in r.by_accession

    @pytest.mark.parametrize("version", [2, 0, "1", True])
    def test_pointer_contract_version_mismatch(self, gcs, version):
        gcs.publish(pointer_version=version)
        assert refused(gcs).check == "contract_version"

    def test_manifest_contract_version_mismatch(self, gcs):
        gcs.publish(manifest={"contract_version": 2})
        assert refused(gcs).check == "contract_version"

    def test_manifest_hash_mismatch(self, gcs):
        gcs.publish(manifest_sha="0" * 64)
        assert refused(gcs).check == "manifest_sha256"

    def test_manifest_overwritten_after_the_pointer(self, gcs):
        base = gcs.publish()
        gcs.put(f"{base}/{contract.MANIFEST_FILE}", b'{"contract_version": 1, "edited": true}')
        assert refused(gcs).check == "manifest_sha256"

    @pytest.mark.parametrize("header", [
        contract.header_line(contract.MODEL_COLUMNS[:-1]),
        contract.header_line(list(reversed(contract.MODEL_COLUMNS))),
        contract.header_line(contract.MODEL_COLUMNS).lstrip("#"),
    ])
    def test_header_mismatch(self, gcs, header):
        gcs.publish(header=header)
        e = refused(gcs)
        assert e.check == "models_header" and contract.MODELS_FILE in str(e)

    @pytest.mark.parametrize("missing", ["pointer", contract.MANIFEST_FILE, contract.MODELS_FILE])
    def test_missing_object(self, gcs, missing):
        base = gcs.publish()
        gcs.local(POINTER if missing == "pointer" else f"{base}/{missing}").unlink()
        e = refused(gcs)
        assert e.check == "object_missing"

    @pytest.mark.parametrize("override", [
        {"model_file_id": "../etc"},
        {"structure_path": "structures/other.pdb.gz"},
        {"pae_bytes": 7},
        {"window_index": 3},
        {"has_pae": "yes"},
    ])
    def test_malformed_rows_refuse_the_release(self, gcs, override):
        rows = default_rows()
        rows[0].update(override)
        gcs.publish(rows=rows)
        assert refused(gcs).check == "models_rows"

    def test_a_trailing_newline_does_not_pass_the_id_check(self):
        # a line-splitting reader never hands one over, but `$` would accept it if one did
        row = model_row("AF-Q0\n", "Q0", 1, 1, 1, 10)
        with pytest.raises(ValueError, match="allowed charset"):
            svc._model([str(row[c]) for c, _ in contract.MODEL_COLUMNS], {})

    @pytest.mark.parametrize("mid", ["../../x", "a/b"])
    def test_a_traversing_id_is_refused_even_with_consistent_paths(self, gcs, mid):
        row = model_row(mid, "Q0", 1, 1, 1, 10)
        assert row["structure_path"] == contract.structure_path(mid)
        gcs.publish(rows=[row])
        assert refused(gcs).check == "models_rows"

    def test_an_oversized_object_is_refused_unread(self, gcs, monkeypatch):
        gcs.publish()
        monkeypatch.setattr(svc, "MAX_MODELS_GZ_BYTES", 10)
        reads = []

        async def fetch(path, max_bytes):
            data = await gcs.fetch(path, max_bytes)
            reads.append(len(data))
            return data

        with pytest.raises(ContractCheckFailed) as e:
            asyncio.run(load_release(POINTER, fetch, None))
        assert e.value.check == "object_too_large" and reads[-1] == 11

    def test_an_oversized_pointer_is_refused(self, gcs):
        gcs.publish()
        gcs.put(POINTER, b" " * (svc.MAX_POINTER_BYTES + 1))
        assert refused(gcs).check == "object_too_large"

    def test_an_over_long_line_is_refused_and_not_quoted_whole(self, gcs):
        header = contract.header_line(contract.MODEL_COLUMNS)
        gcs.publish(models_raw=gzip.compress((header + "x" * 10_000_000 + "\n").encode()))
        e = refused(gcs)
        assert e.check == "models_rows" and "longer than" in str(e) and len(str(e)) < 500

    def test_an_over_long_header_is_refused_and_not_quoted_whole(self, gcs):
        gcs.publish(models_raw=gzip.compress(b"#" + b"y" * 10_000_000 + b"\n"))
        e = refused(gcs)
        assert e.check == "models_header" and len(str(e)) < 500

    def test_a_long_wrong_header_is_clipped_in_the_message(self, gcs):
        gcs.publish(header="#" + "z" * 3000 + "\n")
        e = refused(gcs)
        assert e.check == "models_header" and len(str(e)) < 500

    def test_too_many_rows_are_refused(self, gcs, monkeypatch):
        monkeypatch.setattr(svc, "MAX_MODEL_ROWS", 2)
        gcs.publish()
        e = refused(gcs)
        assert e.check == "models_rows" and "more than 2 rows" in str(e)

    @staticmethod
    def _corrupt_after(text: bytes) -> bytes:
        """`text` as a valid gzip stream, then a deflate block of an invalid type: zlib.error, not OSError."""
        c = zlib.compressobj(wbits=31)
        return c.compress(text) + c.flush(zlib.Z_SYNC_FLUSH) + b"\xff" * 64

    def test_a_corrupt_deflate_stream_at_the_header_names_the_check(self, gcs):
        gcs.publish(models_raw=self._corrupt_after(contract.header_line(contract.MODEL_COLUMNS).encode()))
        assert refused(gcs).check == "models_header"

    def test_a_corrupt_deflate_stream_in_the_rows_names_the_check(self, gcs):
        # incompressible rows, so the valid part outlasts gzip's read buffer and the header is read first
        rows = [model_row(f"AF-Q{i}-F1-model_v4", f"Q{i}", 1, 1, 1, 10,
                          structure_sha256=os.urandom(32).hex(), source_pdb_sha256=os.urandom(32).hex(),
                          pae_sha256=os.urandom(32).hex(), source_pae_sha256=os.urandom(32).hex())
                for i in range(5000)]
        text = gzip.decompress(models_bytes(rows))
        gcs.publish(models_raw=self._corrupt_after(text))
        assert refused(gcs).check == "models_rows"

    def test_pae_columns_set_without_pae_refuse_the_release(self, gcs):
        gcs.publish(rows=[model_row("AF-Q0-F1-model_v4", "Q0", 1, 1, 1, 10, has_pae=False, pae_bytes=100)])
        assert refused(gcs).check == "models_rows"


class TestService:
    @pytest.fixture
    def access(self, monkeypatch, gcs):
        monkeypatch.setattr(gcloud_tabix_base, "ensure_gcs_token", lambda: None)
        monkeypatch.setattr(config, "release_pointer", POINTER)
        a = ProteinStructureAccess()
        monkeypatch.setattr(a, "_fetch_capped", gcs.fetch)
        return a

    def test_good_release_is_served(self, access, gcs):
        gcs.publish()
        asyncio.run(access.warm_all())
        assert access.release.name == RELEASE

    def test_bad_release_is_logged_and_never_raises(self, access, gcs, caplog):
        gcs.publish(manifest_sha="0" * 64)
        with caplog.at_level(logging.ERROR, logger=svc.logger.name):
            asyncio.run(access.warm_all())
        assert "manifest_sha256" in caplog.text
        with pytest.raises(ReleaseUnavailable):
            access.release

    def test_a_failed_first_load_is_retried_until_it_succeeds(self, access, gcs):
        access.retry_seconds = 0.01

        async def scenario():
            await access.warm_all()
            assert access._task is not None
            with pytest.raises(ReleaseUnavailable):
                access.release
            await asyncio.sleep(0.05)
            gcs.publish()
            await asyncio.wait_for(asyncio.shield(access._task), 2)

        asyncio.run(scenario())
        assert access.release.name == RELEASE and access._task is None

    def test_a_loaded_release_is_not_replaced_when_the_pointer_moves(self, access, gcs):
        access.retry_seconds = 0.01
        gcs.publish()

        async def scenario():
            await access.warm_all()
            assert access._task is None
            gcs.publish(release="20261201-000000")
            await asyncio.sleep(0.05)

        asyncio.run(scenario())
        assert access.release.name == RELEASE

    def test_cleanup_stops_the_retry(self, access):
        access.retry_seconds = 0.01

        async def scenario():
            await access.warm_all()
            task = access._task
            await access.cleanup()
            return task

        assert asyncio.run(scenario()).cancelled()

    def test_unconfigured_service_reads_nothing(self, monkeypatch):
        monkeypatch.setattr(gcloud_tabix_base, "ensure_gcs_token", lambda: None)
        monkeypatch.setattr(config, "release_pointer", None)
        a = ProteinStructureAccess()

        async def fail(path, max_bytes):
            raise AssertionError("read with no pointer configured")

        monkeypatch.setattr(a, "_fetch_capped", fail)
        asyncio.run(a.warm_all())
        with pytest.raises(ReleaseUnavailable):
            a.release


class TestStartupCheck:
    @pytest.fixture(autouse=True)
    def _local(self, monkeypatch, gcs):
        monkeypatch.setattr(startup_checks, "_read_object", gcs.read)
        monkeypatch.setattr(config, "release_pointer", POINTER)

    def test_pointer_unset_is_silent(self, monkeypatch, caplog):
        monkeypatch.setattr(config, "release_pointer", None)
        with caplog.at_level(logging.DEBUG):
            startup_checks._check_protein_structure_release()
        assert "protein_structure" not in caplog.text

    def test_good_release_passes_quietly(self, gcs, caplog):
        gcs.publish()
        with caplog.at_level(logging.WARNING, logger=startup_checks.logger.name):
            startup_checks._check_protein_structure_release()
        assert caplog.text == ""

    @pytest.mark.parametrize("publish,check", [
        (dict(pointer_version=2), "contract_version"),
        (dict(manifest_sha="0" * 64), "manifest_sha256"),
        (dict(header="#wrong\n"), "models_header"),
    ])
    def test_a_failed_check_is_a_warning_naming_it(self, gcs, caplog, publish, check):
        gcs.publish(**publish)
        with caplog.at_level(logging.WARNING, logger=startup_checks.logger.name):
            startup_checks._check_protein_structure_release()
        assert f"check '{check}' failed" in caplog.text and POINTER in caplog.text

    def test_absent_pointer_is_a_warning(self, caplog):
        with caplog.at_level(logging.WARNING, logger=startup_checks.logger.name):
            startup_checks._check_protein_structure_release()
        assert "check 'object_missing' failed" in caplog.text

    def test_the_server_still_starts_on_a_bad_release(self, monkeypatch, gcs):
        gcs.publish(manifest={"contract_version": 99})
        monkeypatch.setattr(startup_checks, "_collect_tabix_files", lambda: [])
        monkeypatch.setattr(startup_checks, "_collect_mapping_files", lambda: [])
        monkeypatch.setattr(startup_checks, "_check_cell_type_expression_release", lambda tabix: [])
        monkeypatch.setattr(gcloud_tabix_base, "ensure_gcs_token", lambda: None)
        startup_checks.verify_all_data_files()

    def test_an_unexpected_error_is_a_warning_too(self, monkeypatch, gcs, caplog):
        gcs.publish()

        def boom(*a, **k):
            raise MemoryError("simulated")

        monkeypatch.setattr(svc, "parse_models", boom)
        with caplog.at_level(logging.WARNING, logger=startup_checks.logger.name):
            startup_checks._check_protein_structure_release()
        assert "RELEASE REFUSED" in caplog.text
