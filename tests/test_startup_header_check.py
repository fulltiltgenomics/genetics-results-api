"""Startup refuses a credible-set file whose header is not the canonical one.

A cross-resource credible-set response is emitted positionally under the FIRST file's
header, so a file with a renamed, shifted or missing column would serve wrong values
under right names rather than failing. The check runs over the same `tabix -H` read the
reachability check already does; here it is driven against local bgzip+tabix fixtures,
so nothing touches GCS.
"""

import subprocess

import pytest

from app.config.credible_sets import cs_file_header, cs_qtl_file_header
from app.services import gcloud_tabix_base, startup_checks
from app.services.gcloud_tabix_base import GCloudTabixBase

# the header lines as the combined files on the bucket carry them, spelled out rather
# than built from the schema so the test can say the derived constant matches the data
CS_HEADER_LINE = (
    "#dataset\tdata_type\ttrait\ttrait_original\tcell_type\tchr\tpos\tref\talt\tmlog10p"
    "\tbeta\tse\tpip\tcs_id\tcs_size\tcs_min_r2\taaf\tmost_severe\tgene_most_severe"
)
QTL_HEADER_LINE = CS_HEADER_LINE + "\ttrait_chr\ttrait_start\ttrait_end"


def _write_tabix(path, header_line: str) -> str:
    columns = header_line.split("\t")
    chr_col, pos_col = columns.index("chr") + 1, columns.index("pos") + 1
    row = ["NA"] * len(columns)
    row[chr_col - 1], row[pos_col - 1] = "1", "1000"
    path.write_text(header_line + "\n" + "\t".join(row) + "\n")
    subprocess.run(["bgzip", str(path)], check=True)
    subprocess.run(
        ["tabix", f"-s{chr_col}", f"-b{pos_col}", f"-e{pos_col}", f"{path}.gz"],
        check=True,
    )
    return f"{path}.gz"


@pytest.fixture
def tabix(monkeypatch, tmp_path):
    monkeypatch.setattr(gcloud_tabix_base, "ensure_gcs_token", lambda: None)
    monkeypatch.setattr(GCloudTabixBase, "TBI_CACHE_ROOT", str(tmp_path / "tbi"))
    return GCloudTabixBase()


def test_derived_headers_match_the_lines_the_files_carry(tabix, tmp_path):
    cs = _write_tabix(tmp_path / "cs.tsv", CS_HEADER_LINE)
    qtl = _write_tabix(tmp_path / "qtl.tsv", QTL_HEADER_LINE)
    assert startup_checks._check_tabix_header(tabix, "cs:x", cs, cs_file_header) is None
    assert (
        startup_checks._check_tabix_header(tabix, "cs_qtl:x", qtl, cs_qtl_file_header)
        is None
    )
    # the qtl file is not an acceptable cs file or vice versa: the extra columns matter
    assert startup_checks._check_tabix_header(tabix, "cs:x", qtl, cs_file_header)
    assert startup_checks._check_tabix_header(tabix, "cs_qtl:x", cs, cs_qtl_file_header)


def test_renamed_column_names_file_and_column(tabix, tmp_path):
    path = _write_tabix(
        tmp_path / "renamed.tsv", CS_HEADER_LINE.replace("\tpip\t", "\tprob\t")
    )
    err = startup_checks._check_tabix_header(tabix, "cs:renamed", path, cs_file_header)
    assert err is not None
    assert "cs:renamed" in err and path in err
    assert f"column {cs_file_header.index(b'pip') + 1} is 'prob', expected 'pip'" in err


def test_missing_and_extra_trailing_columns_are_named(tabix, tmp_path):
    short = _write_tabix(tmp_path / "short.tsv", CS_HEADER_LINE.rsplit("\t", 1)[0])
    err = startup_checks._check_tabix_header(tabix, "cs:short", short, cs_file_header)
    assert err and "missing trailing column(s) ['gene_most_severe']" in err

    long = _write_tabix(tmp_path / "long.tsv", CS_HEADER_LINE + "\textra")
    err = startup_checks._check_tabix_header(tabix, "cs:long", long, cs_file_header)
    assert err and "unexpected trailing column(s) ['extra']" in err


def test_shifted_column_is_reported_at_its_first_departure(tabix, tmp_path):
    columns = CS_HEADER_LINE.split("\t")
    del columns[columns.index("cell_type")]
    path = _write_tabix(tmp_path / "shifted.tsv", "\t".join(columns))
    err = startup_checks._check_tabix_header(tabix, "cs:shifted", path, cs_file_header)
    assert err and "column 5 is 'chr', expected 'cell_type'" in err


def test_verify_all_data_files_fails_startup_on_a_mismatch(monkeypatch, tabix, tmp_path):
    good = _write_tabix(tmp_path / "good.tsv", CS_HEADER_LINE)
    bad = _write_tabix(tmp_path / "bad.tsv", CS_HEADER_LINE.replace("\taaf\t", "\taf\t"))
    checks = [("cs:good", good, cs_file_header), ("cs:bad", bad, cs_file_header)]
    monkeypatch.setattr(startup_checks, "_collect_tabix_files", lambda: checks)
    monkeypatch.setattr(startup_checks, "_collect_mapping_files", lambda: [])

    with pytest.raises(RuntimeError) as exc:
        startup_checks.verify_all_data_files()
    message = str(exc.value)
    assert "cs:bad" in message and bad in message
    assert "expected 'aaf'" in message
    assert "cs:good" not in message

    monkeypatch.setattr(startup_checks, "_collect_tabix_files", lambda: checks[:1])
    startup_checks.verify_all_data_files()


def test_unreadable_file_is_still_reported(monkeypatch, tabix, tmp_path):
    missing = str(tmp_path / "missing.tsv.gz")
    monkeypatch.setattr(
        startup_checks,
        "_collect_tabix_files",
        lambda: [("cs:missing", missing, cs_file_header)],
    )
    monkeypatch.setattr(startup_checks, "_collect_mapping_files", lambda: [])
    monkeypatch.setattr(gcloud_tabix_base, "_TABIX_RETRY_BASE_DELAY", 0)
    with pytest.raises(RuntimeError, match="cs:missing: tabix header failed"):
        startup_checks.verify_all_data_files()


def test_every_configured_credible_set_file_gets_its_family_header():
    """The wiring over the real profile: cs and cs_qtl entries carry the expected header,
    every other family is reachability-only."""
    checks = startup_checks._collect_tabix_files()
    by_family = {}
    for label, _, expected in checks:
        by_family.setdefault(label.split(":")[0], set()).add(
            tuple(expected) if expected else None
        )
    assert by_family["cs"] == {tuple(cs_file_header)}
    assert by_family["cs_qtl"] == {tuple(cs_qtl_file_header)}
    others = {k: v for k, v in by_family.items() if k not in ("cs", "cs_qtl")}
    assert others and all(v == {None} for v in others.values())
