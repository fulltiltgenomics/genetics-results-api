"""Offline tests of GET /api/v1/cell_type_expression/{gene} and its release handling.

The release is built from small TSV slices of a real publish (tests/fixtures/
cell_type_expression), bgzipped and tabix-indexed per test, and the GCS reads are replaced
by local ones. Two genes are in the slices on purpose: WASH7P has stored per-dataset rows in
two datasets and is measured by a third, DDX11L1 has summary rows but no per-dataset row at
all and is measured by two datasets.

The route handler is invoked directly (as in test_gene_groups_endpoint.py), so no server runs.
"""

import asyncio
import json
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import app.config.cell_type_expression as config
from app.routers.cell_type_expression import cell_type_expression
from app.services import data_access_cell_type_expression as svc
from app.services import gcloud_tabix_base, startup_checks
from app.services.data_access_cell_type_expression import (
    CellTypeExpressionAccess,
    ReleaseUnavailable,
)
from app.services.gcloud_tabix_base import GCloudTabixBase

FIXTURES = Path(__file__).parent / "fixtures" / "cell_type_expression"
POINTER = "gs://test-bucket/cellxgene/releases/CURRENT"
WASH7P, DDX11L1 = "ENSG00000227232", "ENSG00000223972"
DATASET_1, DATASET_2, DATASET_3 = (
    "31f657dc-1875-4c4b-a5ca-ce63b3ef3a82",  # measures both genes, no stored row for either
    "37a17b78-4864-4a42-b67b-31c00962795a",  # stored WASH7P rows
    "8c42cfd0-0b0a-46d5-910c-fc833d83c45e",  # stored WASH7P rows, measures DDX11L1 too
)

GENES = {
    "WASH7P": dict(ensg=WASH7P, gene_id=WASH7P + ".6", chrom=1, gene_start=14696, gene_end=29570, gene_name="WASH7P"),
    "DDX11L1": dict(ensg=DDX11L1, gene_id=DDX11L1 + ".6", chrom=1, gene_start=12010, gene_end=13670, gene_name="DDX11L1"),
}


def build_release(root: Path, name: str, suppress: tuple[str, ...] = (), manifest: str | None = None) -> str:
    """bgzip + tabix the fixture slices into `root/name`; returns the release's gs:// uri.

    `suppress` names datasets whose every group is rewritten to a single donor, so none passes
    the thresholds. `manifest` replaces the fixture manifest's text."""
    rel = root / name
    (rel / "bq").mkdir(parents=True)
    for table in ("summary", "per_dataset"):
        shutil.copy(FIXTURES / f"{table}.tsv", rel / f"{table}.tsv")
        subprocess.run(["bgzip", str(rel / f"{table}.tsv")], check=True)
        subprocess.run(["tabix", "-f", "-s2", "-b3", "-e3", str(rel / f"{table}.tsv.gz")], check=True)
    subprocess.run(
        f"gzip -c {FIXTURES / 'measured_genes.tsv'} > {rel / 'measured_genes.tsv.gz'}",
        shell=True, check=True,
    )
    shutil.copy(FIXTURES / "datasets.tsv", rel / "bq" / "datasets.tsv")
    groups = (FIXTURES / "groups.tsv").read_text().splitlines()
    cols = groups[0].split("\t")
    rows = []
    for line in groups[1:]:
        f = line.split("\t")
        if f[cols.index("dataset_id")] in suppress:
            f[cols.index("n_donors")] = "1"
        rows.append("\t".join(f))
    (rel / "groups.tsv").write_text("\n".join([groups[0], *rows]) + "\n")
    subprocess.run(["gzip", "-c", str(rel / "groups.tsv")], stdout=(rel / "bq" / "groups.tsv.gz").open("wb"), check=True)
    (rel / "manifest.json").write_text(manifest if manifest is not None else (FIXTURES / "manifest.json").read_text())
    return f"gs://test-bucket/cellxgene/releases/{name}"


def rename_in_header(gz: str, old: bytes, new: bytes) -> None:
    """Re-write a bgzipped table with its first `old` (in the header) renamed, and re-index it."""
    text = subprocess.run(["bgzip", "-dc", gz], capture_output=True, check=True).stdout
    Path(gz).unlink()
    Path(gz[:-3]).write_bytes(text.replace(old, new, 1))
    subprocess.run(["bgzip", gz[:-3]], check=True)
    subprocess.run(["tabix", "-f", "-s2", "-b3", "-e3", gz], check=True)


def pointer_bytes(release: str, uri: str) -> bytes:
    return json.dumps({"release": release, "uri": uri, "datasets": 8}).encode()


class FakeGcs:
    """gs://test-bucket/cellxgene/releases/<name>/... -> root/<name>/...; the pointer is settable."""

    def __init__(self, root: Path):
        self.root = root
        self.pointer: bytes | Exception = b""
        self.indexed: list[str] = []
        self.ranged: list[str] = []
        self.fail_range: set[str] = set()

    def local(self, gs_path: str) -> str:
        return str(self.root / gs_path.split("/releases/", 1)[1])

    async def fetch_full(self, gs_path: str) -> bytes:
        if gs_path == POINTER:
            if isinstance(self.pointer, Exception):
                raise self.pointer
            return self.pointer
        return Path(self.local(gs_path)).read_bytes()

    async def header(self, gs_path: str) -> list[bytes]:
        out = subprocess.run(["tabix", "-H", self.local(gs_path)], capture_output=True, check=True).stdout
        return [h.lstrip(b"#") for h in out.strip().split(b"\t")]

    async def index(self, gs_path: str):
        self.indexed.append(gs_path)

    async def stream_range(self, gs_path, chroms, starts, ends, chunk_size):
        if any(f in gs_path for f in self.fail_range):
            raise RuntimeError("range read failed")
        self.ranged.append(gs_path)
        regions = [f"{c}:{s}-{e}" for c, s, e in zip(chroms, starts, ends)]
        out = subprocess.run(["tabix", self.local(gs_path), *regions], capture_output=True, check=True).stdout

        async def gen():
            # arbitrary chunk boundaries: the consumer must not depend on them being line-aligned
            for i in range(0, len(out), 700):
                yield out[i : i + 700]

        return gen()


@pytest.fixture
def gcs(monkeypatch, tmp_path):
    monkeypatch.setattr(gcloud_tabix_base, "ensure_gcs_token", lambda: None)
    return FakeGcs(tmp_path)


@pytest.fixture
def access(monkeypatch, gcs):
    monkeypatch.setattr(config, "release_pointer", POINTER)
    a = CellTypeExpressionAccess()
    monkeypatch.setattr(a, "_fetch_full", gcs.fetch_full)
    monkeypatch.setattr(a, "_get_header_async", gcs.header)
    monkeypatch.setattr(a, "_get_index", gcs.index)
    monkeypatch.setattr(a, "_stream_range", gcs.stream_range)
    return a


def publish(gcs: FakeGcs, name: str, **kw) -> str:
    uri = build_release(gcs.root, name, **kw)
    gcs.pointer = pointer_bytes(name, uri)
    return uri


class FakeMapping:
    """The slice of GeneNameAndPositionMapping the route uses."""

    def __init__(self):
        import polars as pl

        self.gene_positions = {
            49: pl.DataFrame(
                [
                    {"ensg": g["ensg"], "gene_id": g["gene_id"], "chrom": g["chrom"],
                     "gene_start": g["gene_start"], "gene_end": g["gene_end"], "gene_name": g["gene_name"]}
                    for g in GENES.values()
                ]
            )
        }

    def get_ensg_ids_by_gene_name(self, name: str) -> set[str]:
        if name.upper().startswith("ENSG"):
            return {name.upper()}
        from app.core.exceptions import GeneNotFoundException

        for g in GENES.values():
            if g["gene_name"].lower() == name.lower():
                return {g["ensg"]}
        raise GeneNotFoundException(f"Gene {name} not found")


def call(access, gene, level="summary", format="tsv"):
    request = SimpleNamespace(url=f"http://test/api/v1/cell_type_expression/{gene}")
    return asyncio.run(
        cell_type_expression(
            request=request, gene=gene, level=level, format=format,
            data_access=access, gene_name_and_position_mapping=FakeMapping(),
        )
    )


def body(response) -> str:
    if hasattr(response, "body_iterator"):
        async def drain():
            return b"".join([c async for c in response.body_iterator])
        return asyncio.run(drain()).decode()
    return response.body.decode()


def tsv_rows(response) -> list[dict]:
    lines = body(response).splitlines()
    header = lines[0].split("\t")
    return [dict(zip(header, l.split("\t"))) for l in lines[1:]]


@pytest.fixture
def loaded(access, gcs):
    publish(gcs, "2026-10-13-120000")
    asyncio.run(access.refresh())
    return access


class TestLevels:
    def test_summary_tsv(self, loaded):
        response = call(loaded, "WASH7P")
        assert response.media_type == "text/tab-separated-values"
        assert response.headers["x-dataset-version"] == "2026-10-13-120000"
        rows = tsv_rows(response)
        assert rows and {r["gene_name"] for r in rows} == {"WASH7P"}
        assert list(rows[0]) == config.summary_columns
        # both metrics and the secondary one are carried through untouched
        assert {"mean_log1p_cp10k", "frac_expressing", "donor_pseudobulk_mean_cp10k"} <= set(rows[0])

    def test_summary_json_is_typed_and_na_is_null(self, loaded):
        rows = json.loads(body(call(loaded, "WASH7P", format="json")))
        assert isinstance(rows[0]["mean_log1p_cp10k"], float)
        assert isinstance(rows[0]["n_cells"], int)
        assert isinstance(rows[0]["tau"], float)
        # DDX11L1's tau is NA in the file
        assert json.loads(body(call(loaded, "DDX11L1", format="json")))[0]["tau"] is None

    def test_dataset_level_stored_rows_are_marked_present(self, loaded):
        rows = tsv_rows(call(loaded, "WASH7P", level="dataset"))
        present = [r for r in rows if r["status"] == "present"]
        assert len(present) == 8
        assert {r["dataset_id"] for r in present} == {DATASET_2, DATASET_3}
        assert list(present[0]) == config.per_dataset_columns + ["status"]

    def test_dataset_level_measured_without_a_row_is_an_explicit_zero(self, loaded):
        rows = tsv_rows(call(loaded, "WASH7P", level="dataset"))
        absent = [r for r in rows if r["status"] == "measured_no_row"]
        assert [r["dataset_id"] for r in absent] == [DATASET_1]
        assert absent[0]["mean_log1p_cp10k"] == "0"
        assert absent[0]["frac_expressing"] == "0"
        assert absent[0]["n_cells"] == "NA" and absent[0]["tissue"] == "NA"
        # the pseudobulk's donor rule is per group, so a dataset-level zero is not claimed
        assert absent[0]["donor_pseudobulk_mean_cp10k"] == "NA"
        assert absent[0]["gene_id"] == GENES["WASH7P"]["gene_id"]

    def test_dataset_level_json(self, loaded):
        rows = json.loads(body(call(loaded, "WASH7P", level="dataset", format="json")))
        absent = [r for r in rows if r["status"] == "measured_no_row"]
        assert len(absent) == 1 and absent[0]["n_cells"] is None
        assert absent[0]["mean_log1p_cp10k"] == 0.0

    def test_a_dataset_that_did_not_measure_the_gene_is_absent(self, loaded):
        rows = tsv_rows(call(loaded, "DDX11L1", level="dataset"))
        # five of the release's eight datasets never measured it; they are not rows at all
        assert {r["dataset_id"] for r in rows} == {DATASET_1, DATASET_3}

    def test_gene_with_no_per_dataset_rows_still_has_summary_rows(self, loaded):
        assert tsv_rows(call(loaded, "DDX11L1"))
        assert {r["status"] for r in tsv_rows(call(loaded, "DDX11L1", level="dataset"))} == {"measured_no_row"}

    def test_gene_in_neither_table_returns_just_the_header(self, loaded):
        other = dict(GENES["WASH7P"], ensg="ENSG00000000001", gene_id="ENSG00000000001.1")
        rows = asyncio.run(_collect(loaded, other))
        assert rows == ""

    def test_comma_separated_genes(self, loaded):
        names = {r["gene_name"] for r in tsv_rows(call(loaded, "WASH7P,ddx11l1"))}
        assert names == {"WASH7P", "DDX11L1"}

    def test_unknown_gene_is_404(self, loaded):
        with pytest.raises(HTTPException) as e:
            call(loaded, "NOSUCHGENE")
        assert e.value.status_code == 404

    def test_gene_absent_from_gencode_49_is_404(self, loaded):
        with pytest.raises(HTTPException) as e:
            call(loaded, "ENSG00000000001")
        assert e.value.status_code == 404 and "GENCODE v49" in e.value.detail


async def _collect(access, gene) -> str:
    stream = await access.stream_summary(access.release, [gene])
    lines = b"".join([c async for c in stream]).decode().splitlines()
    assert lines[0].split("\t") == config.summary_columns
    return "\n".join(lines[1:])


class TestProfiles:
    def test_unconfigured_deployment_answers_404(self, monkeypatch):
        monkeypatch.setattr(gcloud_tabix_base, "ensure_gcs_token", lambda: None)
        monkeypatch.setattr(config, "release_pointer", None)
        with pytest.raises(HTTPException) as e:
            call(CellTypeExpressionAccess(), "WASH7P")
        assert e.value.status_code == 404 and "not configured" in e.value.detail

    def test_unconfigured_deployment_neither_warms_nor_polls(self, monkeypatch):
        monkeypatch.setattr(gcloud_tabix_base, "ensure_gcs_token", lambda: None)
        monkeypatch.setattr(config, "release_pointer", None)
        a = CellTypeExpressionAccess()
        asyncio.run(a.warm_all())
        assert a._task is None

    def test_only_the_daly_profile_names_a_pointer(self):
        from app.config.profiles.daly import cell_type_expression as daly
        from app.config.profiles.finngen import cell_type_expression as finngen

        assert daly.release_pointer == "gs://daly-expression-data/cellxgene/releases/CURRENT"
        assert finngen.release_pointer is None

    def test_no_release_yet_is_503(self, access):
        with pytest.raises(HTTPException) as e:
            call(access, "WASH7P")
        assert e.value.status_code == 503
        with pytest.raises(ReleaseUnavailable):
            access.release


class TestKeptGroups:
    def test_a_fully_suppressed_dataset_gets_no_placeholder(self, access, gcs):
        publish(gcs, "2026-10-13-120000", suppress=(DATASET_1,))
        asyncio.run(access.refresh())
        for gene in ("WASH7P", "DDX11L1"):
            assert DATASET_1 not in {r["dataset_id"] for r in tsv_rows(call(access, gene, level="dataset"))}
        assert {r["dataset_id"] for r in tsv_rows(call(access, "DDX11L1", level="dataset"))} == {DATASET_3}

    def test_a_dataset_with_one_kept_group_still_gets_one(self, access, gcs):
        publish(gcs, "2026-10-13-120000")
        asyncio.run(access.refresh())
        rows = tsv_rows(call(access, "DDX11L1", level="dataset"))
        assert {r["dataset_id"] for r in rows} == {DATASET_1, DATASET_3}

    @pytest.mark.parametrize("manifest", ["{}", '{"thresholds": {"min_cells": 50}}', '{"thresholds": null}'])
    def test_a_manifest_without_thresholds_fails_the_warm(self, access, gcs, manifest):
        publish(gcs, "2026-10-13-120000", manifest=manifest)
        with pytest.raises(ValueError, match="thresholds"):
            asyncio.run(access.refresh())
        with pytest.raises(ReleaseUnavailable):
            access.release

    def test_the_emit_rule_is_inclusive_at_both_thresholds(self):
        groups = svc.gzip.compress(
            b"dataset_id\tn_cells\tn_donors\\nA\t50\t3\nB\t49\t3\nC\t50\t2\n".replace(b"\\n", b"\n")
        )
        manifest = b'{"thresholds": {"min_cells": 50, "min_donors": 3}}'
        assert svc.parse_kept_datasets(groups, manifest) == frozenset({"A"})


class TestRefresh:
    def test_unchanged_pointer_is_a_no_op(self, loaded, gcs):
        before = list(gcs.indexed)
        assert asyncio.run(loaded.refresh()) is False
        assert gcs.indexed == before

    def test_new_release_is_warmed_before_it_is_served(self, loaded, gcs):
        old = loaded.release
        new_uri = publish(gcs, "2026-10-14-090000")
        served_during_warm: list[str] = []
        real_stream_range = loaded._stream_range

        async def spying(*a, **k):
            served_during_warm.append(loaded.release.name)
            return await real_stream_range(*a, **k)

        loaded._stream_range = spying
        assert asyncio.run(loaded.refresh()) is True
        # every warm-up read happened while the old release was still the served one
        assert served_during_warm and set(served_during_warm) == {old.name}
        assert loaded.release.name == "2026-10-14-090000"
        # both of the new release's files had index and a real range read done on them
        for table in (config.SUMMARY_FILE, config.PER_DATASET_FILE):
            assert f"{new_uri}/{table}" in gcs.indexed
            assert f"{new_uri}/{table}" in gcs.ranged
        assert tsv_rows(call(loaded, "WASH7P"))

    def test_unreadable_pointer_keeps_the_old_release(self, loaded, gcs):
        gcs.pointer = RuntimeError("GCS unavailable")
        with pytest.raises(RuntimeError):
            asyncio.run(loaded.refresh())
        assert loaded.release.name == "2026-10-13-120000"
        assert tsv_rows(call(loaded, "WASH7P"))

    @pytest.mark.parametrize("bad", [b"not json", b"[]", b'{"release": "x"}', b'{"release": "x", "uri": "/local/dir"}'])
    def test_malformed_pointer_keeps_the_old_release(self, loaded, gcs, bad):
        gcs.pointer = bad
        with pytest.raises(Exception):
            asyncio.run(loaded.refresh())
        assert loaded.release.name == "2026-10-13-120000"

    def test_release_that_fails_to_warm_keeps_the_old_release(self, loaded, gcs):
        publish(gcs, "2026-10-14-090000")
        gcs.fail_range = {"2026-10-14-090000/per_dataset"}
        with pytest.raises(RuntimeError):
            asyncio.run(loaded.refresh())
        assert loaded.release.name == "2026-10-13-120000"

    def test_release_with_a_foreign_header_is_not_served(self, loaded, gcs):
        uri = publish(gcs, "2026-10-14-090000")
        rename_in_header(gcs.local(f"{uri}/summary.tsv.gz"), b"mean_log1p_cp10k", b"mean_log1p_tpm")
        with pytest.raises(ValueError, match="header"):
            asyncio.run(loaded.refresh())
        assert loaded.release.name == "2026-10-13-120000"

    def test_release_missing_its_side_tables_is_not_served(self, loaded, gcs):
        uri = publish(gcs, "2026-10-14-090000")
        Path(gcs.local(f"{uri}/measured_genes.tsv.gz")).unlink()
        with pytest.raises(FileNotFoundError):
            asyncio.run(loaded.refresh())
        assert loaded.release.name == "2026-10-13-120000"

    def test_a_failed_loop_iteration_is_survived_and_the_next_one_switches(self, access, gcs):
        publish(gcs, "2026-10-13-120000")
        asyncio.run(access.refresh())
        access.refresh_seconds = 0.01

        async def scenario():
            gcs.pointer = RuntimeError("GCS unavailable")
            task = asyncio.create_task(access._refresh_loop())
            await asyncio.sleep(0.05)
            assert not task.done()
            assert access.release.name == "2026-10-13-120000"
            publish(gcs, "2026-10-14-090000")
            for _ in range(200):
                if access.release.name == "2026-10-14-090000":
                    break
                await asyncio.sleep(0.05)
            task.cancel()
            return access.release.name

        assert asyncio.run(scenario()) == "2026-10-14-090000"

    def test_initial_load_failure_is_swallowed_and_the_loop_still_starts(self, access, gcs):
        gcs.pointer = RuntimeError("GCS unavailable")

        async def scenario():
            await access.warm_all()
            started = access._task is not None
            await access.cleanup()
            return started

        assert asyncio.run(scenario()) is True

    def test_requests_in_flight_keep_the_release_they_started_on(self, loaded, gcs):
        old = loaded.release
        publish(gcs, "2026-10-14-090000")
        asyncio.run(loaded.refresh())
        stream = asyncio.run(loaded.stream_summary(old, [GENES["WASH7P"]]))

        async def drain():
            return b"".join([c async for c in stream])

        assert b"WASH7P" in asyncio.run(drain())
        assert loaded.release is not old


class TestMeasuredGenes:
    def test_bridge_from_version_id_to_dataset_id(self):
        datasets = b"dataset_key\tdataset_id\tdataset_version_id\n0\tD1\tV1\n1\tD2\tV2\n"
        measured = svc.gzip.compress(
            b"dataset_version_id\tgene_id\nV1\tENSG1.2\nV2\tENSG1.3\nV2\tENSG9.1\n"
        )
        masks, ids = svc.parse_measured(measured, datasets)
        assert ids == ("D1", "D2")
        assert masks == {"ENSG1": 0b11, "ENSG9": 0b10}

    def test_a_dataset_the_bridge_cannot_place_refuses_the_release(self):
        datasets = b"dataset_key\tdataset_id\tdataset_version_id\n0\tD1\tV1\n"
        measured = svc.gzip.compress(b"dataset_version_id\tgene_id\nV9\tENSG1.2\n")
        with pytest.raises(ValueError, match="absent from datasets.tsv"):
            svc.parse_measured(measured, datasets)


class TestStartupCheck:
    @pytest.fixture(autouse=True)
    def _local(self, monkeypatch, tmp_path):
        monkeypatch.setattr(gcloud_tabix_base, "ensure_gcs_token", lambda: None)
        monkeypatch.setattr(GCloudTabixBase, "TBI_CACHE_ROOT", str(tmp_path / "tbi"))
        self.tmp = tmp_path

    def local_release(self, monkeypatch, columns_override=None):
        """A pointer and release on local disk; the check's fsspec reads accept plain paths."""
        uri = build_release(self.tmp, "rel")
        local = f"{self.tmp}/rel"
        pointer = self.tmp / "CURRENT"
        pointer.write_bytes(pointer_bytes("rel", uri))
        monkeypatch.setattr(config, "release_pointer", str(pointer))
        real = svc.release_paths
        monkeypatch.setattr(svc, "release_paths", lambda p: {n: f"{local}/{n}" for n in real(p)})
        return local

    def test_good_release_passes(self, monkeypatch):
        self.local_release(monkeypatch)
        assert startup_checks._check_cell_type_expression_release(GCloudTabixBase()) == []

    def test_unconfigured_deployment_checks_nothing(self, monkeypatch):
        monkeypatch.setattr(config, "release_pointer", None)
        assert startup_checks._check_cell_type_expression_release(GCloudTabixBase()) == []

    def test_absent_pointer_is_a_warning_not_an_error(self, monkeypatch, caplog):
        monkeypatch.setattr(config, "release_pointer", str(self.tmp / "missing"))
        with caplog.at_level("WARNING", logger=startup_checks.logger.name):
            assert startup_checks._check_cell_type_expression_release(GCloudTabixBase()) == []
        assert "release pointer" in caplog.text and "503" in caplog.text

    def test_malformed_pointer_is_a_warning_not_an_error(self, monkeypatch):
        pointer = self.tmp / "CURRENT"
        pointer.write_bytes(b"not json")
        monkeypatch.setattr(config, "release_pointer", str(pointer))
        assert startup_checks._check_cell_type_expression_release(GCloudTabixBase()) == []

    def test_startup_succeeds_when_current_is_absent(self, monkeypatch):
        monkeypatch.setattr(config, "release_pointer", str(self.tmp / "missing"))
        monkeypatch.setattr(startup_checks, "_collect_tabix_files", lambda: [])
        monkeypatch.setattr(startup_checks, "_collect_mapping_files", lambda: [])
        startup_checks.verify_all_data_files()

    def test_missing_side_table_and_foreign_header_are_reported(self, monkeypatch):
        local = self.local_release(monkeypatch)
        Path(f"{local}/measured_genes.tsv.gz").unlink()
        rename_in_header(f"{local}/summary.tsv.gz", b"tau", b"tau2")
        errors = startup_checks._check_cell_type_expression_release(GCloudTabixBase())
        assert any("summary.tsv.gz" in e and "mismatch" in e for e in errors)
        assert any("measured_genes.tsv.gz" in e and "does not exist" in e for e in errors)

    @pytest.mark.parametrize("name", [config.GROUPS_FILE, config.MANIFEST_FILE])
    def test_groups_and_manifest_are_required(self, monkeypatch, name):
        local = self.local_release(monkeypatch)
        Path(f"{local}/{name}").unlink()
        errors = startup_checks._check_cell_type_expression_release(GCloudTabixBase())
        assert any(name in e and "does not exist" in e for e in errors)
