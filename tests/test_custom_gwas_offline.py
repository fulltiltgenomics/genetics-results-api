"""Offline tests of the custom GWAS catalog, its SuSiE translation and HLA widening.

The catalog is built from a bucket listing, so these drive the parsers with listings
shaped like the three releases: R14 (one phenotype per folder), R13 (a run-title folder
holding several phenotypes' files), and the objects that must be ignored.
"""

import asyncio

import pytest

from app.services.custom_gwas_catalog import metadata_row, parse_listing
from app.services.custom_gwas_finemap_data_access import SERVED_HEADER, translate_susie
from app.services.custom_gwas_hla import hla_gene_of, widen_hla_alleles
from app.services.metadata_harmonizer import MetadataHarmonizer

BASE = "gs://bucket/finngen_R14/sandbox_custom_gwas/"

R14_LISTING = [
    "placeholder",
    "AIH/AIH.gz",
    "AIH/AIH.gz.tbi",
    "AIH/metadata.json",
    "AIH/hla/AIH.gz",
    "AIH/finemap/had_results",
    "AIH/finemap/finemap/AIH.FINEMAP.snp.bgz",
    "AIH/finemap/susie/AIH.SUSIE.snp.bgz",
    "AIH/finemap/susie/AIH.SUSIE.snp.filter.tsv",
    "AIH/finemap/susie/AIH.SUSIE.cred.summary.tsv",
    "AIH/finemap/susie/AIH.SUSIE_99.snp.filter.tsv",
    "AIH/autoreporting/AIH.top.out",
    "ABSDLT_2/ABSDLT_2.gz",
    "ABSDLT_2/ABSDLT_2.gz.tbi",
    "ABSDLT_2/metadata.json",
    "ABSDLT_2/coding_variants.txt.gz",
    "PARTIAL/PARTIAL.gz",
]

R13_LISTING = [
    "ABSOLUTEINT/ABSOLUTEINT.gz",
    "ABSOLUTEINT/ABSOLUTEINT.gz.tbi",
    "ABSOLUTEINT/ABSOLUTEINT_cases_controls.json",
    "ABSOLUTEINT/RESPONSEINT.gz",
    "ABSOLUTEINT/RESPONSEINT.gz.tbi",
    "ABSOLUTEINT/RESPONSEINT_cases_controls.json",
    "ABSOLUTEINT/metadata.json",
    "ABSOLUTEINT/finemap/susie/RESPONSEINT.SUSIE.snp.filter.tsv",
    "ABSOLUTEINT/finemap/susie/RESPONSEINT.SUSIE.cred.summary.tsv",
]


class TestParseListing:
    def test_r14_folder_is_the_phenotype(self):
        phenos = parse_listing(BASE, R14_LISTING)
        assert set(phenos) == {"AIH", "ABSDLT_2"}
        aih = phenos["AIH"]
        assert aih.sumstats == f"{BASE}AIH/AIH.gz"
        assert aih.hla == f"{BASE}AIH/hla/AIH.gz"
        assert aih.susie_members == f"{BASE}AIH/finemap/susie/AIH.SUSIE.snp.filter.tsv"
        assert aih.susie_summary == f"{BASE}AIH/finemap/susie/AIH.SUSIE.cred.summary.tsv"
        assert aih.metadata_path == f"{BASE}AIH/metadata.json"

    def test_a_run_without_finemapping_has_no_susie_paths(self):
        phenos = parse_listing(BASE, R14_LISTING)
        assert phenos["ABSDLT_2"].susie_members is None
        assert phenos["ABSDLT_2"].hla is None

    def test_gz_without_tbi_and_coding_variants_are_not_phenotypes(self):
        phenos = parse_listing(BASE, R14_LISTING)
        assert "PARTIAL" not in phenos
        assert "coding_variants.txt" not in phenos

    def test_r13_run_title_folder_holds_several_phenotypes(self):
        base = "gs://bucket/finngen_R13/sandbox_custom_gwas/"
        phenos = parse_listing(base, R13_LISTING)
        assert set(phenos) == {"ABSOLUTEINT", "RESPONSEINT"}
        resp = phenos["RESPONSEINT"]
        assert resp.folder == "ABSOLUTEINT"
        assert resp.sumstats == f"{base}ABSOLUTEINT/RESPONSEINT.gz"
        assert resp.susie_members is not None
        assert resp.cases_controls_path == f"{base}ABSOLUTEINT/RESPONSEINT_cases_controls.json"
        # the folder's metadata.json is attached to both; metadata_row keeps it by name
        assert resp.metadata_path == phenos["ABSOLUTEINT"].metadata_path

    def test_duplicate_name_prefers_the_run_titled_after_it(self):
        listing = [
            ("A/X.gz", "2026-03-01T00:00:00Z"), ("A/X.gz.tbi", ""),
            ("X/X.gz", "2025-01-01T00:00:00Z"), ("X/X.gz.tbi", ""),
            ("Z/X.gz", "2026-06-01T00:00:00Z"), ("Z/X.gz.tbi", ""),
        ]
        assert parse_listing(BASE, listing)["X"].folder == "X"

    def test_duplicate_name_else_prefers_the_newest_run(self):
        listing = [
            ("A/X.gz", "2026-03-01T00:00:00Z"), ("A/X.gz.tbi", ""),
            ("Z/X.gz", "2026-06-01T00:00:00Z"), ("Z/X.gz.tbi", ""),
            ("M/X.gz", "2025-06-01T00:00:00Z"), ("M/X.gz.tbi", ""),
        ]
        assert parse_listing(BASE, listing)["X"].folder == "Z"

    def test_written_is_the_served_runs_sumstats_timestamp(self):
        listing = [
            ("A/X.gz", "2026-03-01T00:00:00Z"), ("A/X.gz.tbi", ""),
            ("Z/X.gz", "2026-06-01T12:30:00.123Z"), ("Z/X.gz.tbi", ""),
        ]
        assert parse_listing(BASE, listing)["X"].written == "2026-06-01T12:30:00.123Z"
        assert parse_listing(BASE, R14_LISTING)["AIH"].written == ""
        # without timestamps the listing order (lexical folder) decides
        assert parse_listing(BASE, [n for n, _ in listing])["X"].folder == "A"


class TestMetadataRow:
    def test_folder_metadata_applies_to_its_named_phenotype_only(self):
        base = "gs://bucket/finngen_R13/sandbox_custom_gwas/"
        phenos = parse_listing(base, R13_LISTING)
        meta = {"name": "ABSOLUTEINT", "description": "absolute change", "num_cases": 11166,
                "num_controls": 0, "pheno_coding": "continuous"}
        row = metadata_row(phenos["ABSOLUTEINT"], meta, {"cases": 0, "controls": 1})
        assert row["phenostring"] == "absolute change"
        assert row["num_cases"] == 11166 and row["pheno_coding"] == "continuous"
        other = metadata_row(phenos["RESPONSEINT"], meta, {"cases": 0, "controls": 1})
        assert other["phenostring"] == "RESPONSEINT"
        # a continuous trait's sidecar says 0 cases / 1 control, which is not a sample size
        assert other["num_cases"] is None and other["num_controls"] is None
        assert other["has_credible_sets"] is True

    def test_cases_controls_sidecar_gives_sizes_for_a_binary_trait(self):
        phenos = parse_listing(BASE, R13_LISTING)
        row = metadata_row(phenos["RESPONSEINT"], None, {"cases": 120, "controls": 3400})
        assert (row["num_cases"], row["num_controls"]) == (120, 3400)

    def test_date_is_the_day_the_run_was_written_and_nothing_names_the_submitter(self):
        listing = [("X/X.gz", "2026-06-01T12:30:00Z"), ("X/X.gz.tbi", ""), ("X/metadata.json", "")]
        meta = {"name": "X", "description": "x", "num_cases": 1, "num_controls": 2,
                "pheno_coding": "binary", "submitter": ["someone@example.org"],
                "submitter_email": ["someone@example.org"], "admin_email": ["a@example.org"]}
        row = metadata_row(parse_listing(BASE, listing)["X"], meta, None)
        assert row["date"] == "2026-06-01"
        assert "someone" not in repr(row) and "example.org" not in repr(row)
        assert metadata_row(parse_listing(BASE, R14_LISTING)["AIH"], None, None)["date"] is None


class TestHarmonizer:
    def _harmonize(self, rows):
        config = {"metadata": {"type": "custom_gwas", "author": "FinnGen sandbox users",
                               "publication_date": "NA", "version_label": "R14",
                               "resource": "finngen_custom_r14"}}
        return MetadataHarmonizer().harmonize_metadata("finngen_custom_r14", rows, config)

    def test_binary_and_continuous(self):
        out = self._harmonize([
            {"phenocode": "AIH", "phenostring": "AIH", "num_cases": 277, "num_controls": 1620,
             "pheno_coding": "binary"},
            {"phenocode": "ABI", "phenostring": "Average birth interval", "num_cases": 137176,
             "num_controls": 0, "pheno_coding": "continuous"},
            {"phenocode": "X", "phenostring": "X", "num_cases": None, "num_controls": None,
             "pheno_coding": None},
        ])
        aih, abi, x = out
        assert (aih.trait_type, aih.n_samples, aih.n_cases, aih.n_controls) == ("binary", 1897, 277, 1620)
        assert (abi.trait_type, abi.n_samples, abi.n_cases, abi.n_controls) == ("quantitative", 137176, 137176, 0)
        assert (x.n_samples, x.n_cases, x.n_controls) == ("NA", "NA", "NA")
        assert abi.resource == "finngen_custom_r14" and abi.version == "R14"

    def test_date_is_the_runs_own_and_falls_back_to_the_config(self):
        dated, undated = self._harmonize([
            {"phenocode": "A", "phenostring": "A", "pheno_coding": "binary", "date": "2026-06-01"},
            {"phenocode": "B", "phenostring": "B", "pheno_coding": "binary", "date": None},
        ])
        assert dated.date == "2026-06-01"
        assert undated.date == "NA"


MEMBERS = "\n".join([
    "trait\tregion\tv\tcs\tcs_specific_prob\tchromosome\tposition\tallele1\tallele2\tmaf\tbeta\tp\tse\tmost_severe\tgene_most_severe",
    "AIH\tchr9:96296811-99296811\t9:97796811:C:T\t1\t0.059963604424928\tchr9\t97796811\tC\tT\t0.65\t0.998369\t7.41481e-21\t0.106576\tintron_variant\tPTCSC2",
    "AIH\tchr9:96296811-99296811\t9:97772921:C:G\t1\t0.0277332004727801\tchr9\t97772921\tC\tG\t0.34734\t0.991017\t1.68927e-20\t0.106787\tintron_variant\tPTCSC2",
    "AIH\tchrX:1000000-4000000\tX:2000000:A:G\t1\t0.5\tchrX\t2000000\tA\tG\t0.1\t0.5\t0\t0.1\tmissense_variant\tGENE",
])
SUMMARY = "\n".join([
    "trait\tregion\tcs\tcs_log10bf\tcs_avg_r2\tcs_min_r2\tlow_purity\tcs_size\tgood_cs\tcs_id\tv\trsid\tp\tbeta\tsd\tprob\tcs_specific_prob\tmost_severe\tgene_most_severe",
    "AIH\tchr9:96296811-99296811\t1\t14.95\t0.997\t0.992141499969\tFalse\t29\tTrue\tchr9:96296811-99296811_1\t9:97796811:C:T\tchr9_97796811_C_T\t7.41481e-21\t0.998369\t0.2357\t0.0599\t0.0599\tintron_variant\tPTCSC2",
    "AIH\tchrX:1000000-4000000\t1\t3.0\t0.9\t0.8\tFalse\t2\tTrue\tchrX:1000000-4000000_1\tX:2000000:A:G\trs1\t0\t0.5\t0.1\t0.5\t0.5\tmissense_variant\tGENE",
])


class TestTranslateSusie:
    def test_rows_take_the_served_shape(self):
        rows = translate_susie(MEMBERS, SUMMARY, "FinnGen_R14_custom", "AIH")
        assert len(rows) == 3
        by_pos = {row[SERVED_HEADER.index("pos")]: dict(zip(SERVED_HEADER, row)) for row in rows}
        lead = by_pos["97796811"]
        assert lead["dataset"] == "FinnGen_R14_custom" and lead["data_type"] == "GWAS"
        assert (lead["trait"], lead["trait_original"], lead["cell_type"]) == ("AIH", "AIH", "NA")
        assert (lead["chr"], lead["ref"], lead["alt"]) == ("9", "C", "T")
        assert lead["mlog10p"] == "20.1299"
        assert lead["pip"] == "0.0600"
        assert lead["cs_id"] == "chr9:96296811-99296811_1"
        assert (lead["cs_size"], lead["cs_min_r2"], lead["aaf"]) == ("29", "0.9921", "NA")
        assert (lead["most_severe"], lead["gene_most_severe"]) == ("intron_variant", "PTCSC2")

    def test_sorted_by_position_and_x_is_23_and_p_zero_is_inf(self):
        rows = translate_susie(MEMBERS, SUMMARY, "D", "AIH")
        positions = [(int(r[5]), int(r[6])) for r in rows]
        assert positions == sorted(positions)
        x_row = dict(zip(SERVED_HEADER, rows[-1]))
        assert x_row["chr"] == "23" and x_row["mlog10p"] == "inf"

    def test_cs_id_sits_at_the_index_the_by_id_filter_defaults_to(self):
        assert SERVED_HEADER.index("cs_id") == 13

    def test_leads_reduce_to_one_row_per_set(self):
        from app.config.credible_sets import cs_header_schema
        from app.core.streams import accumulate_cs_leads

        async def lines():
            yield SERVED_HEADER
            for row in translate_susie(MEMBERS, SUMMARY, "D", "AIH"):
                yield row

        header, leads = asyncio.run(accumulate_cs_leads(lines(), cs_header_schema))
        assert header == SERVED_HEADER
        assert {lead["cs_id"]: lead["pos"] for lead in leads} == {
            "chr9:96296811-99296811_1": 97796811,
            "chrX:1000000-4000000_1": 2000000,
        }


class TestHlaWidening:
    def test_gene_from_allele(self):
        assert hla_gene_of(b"A*01:01") == b"HLA-A"
        assert hla_gene_of(b"DRB3*01:01") == b"HLA-DRB3"

    def test_ref_alt_become_gene_allele(self):
        header = [b"chrom", b"pos", b"ref", b"alt", b"pval", b"info"]
        rows = [[b"6", b"29941260", b"<absent>", b"A*01:01", b"0.7", b"0.9"]]
        new_header, new_rows = widen_hla_alleles(header, rows)
        assert new_header == [b"chrom", b"pos", b"gene", b"allele", b"pval", b"info"]
        assert new_rows == [[b"6", b"29941260", b"HLA-A", b"A*01:01", b"0.7", b"0.9"]]

    def test_not_an_hla_file(self):
        with pytest.raises(ValueError):
            widen_hla_alleles([b"chrom", b"pos"], [])


class TestOnRequestSearchGating:
    """on_request resources are indexed apart and searched only when named."""

    @staticmethod
    def _index(monkeypatch):
        from app.services import search_service as ss

        monkeypatch.setattr(ss, "get_resources_with_metadata", lambda: ["finngen", "finngen_custom_r14"])
        monkeypatch.setattr(ss, "get_datasets", lambda: {})
        monkeypatch.setattr(ss, "on_request_resources", lambda: {"finngen_custom_r14"})
        rows = {
            "finngen": [{"phenotype_code": "AIH", "phenotype_string": "Autoimmune hepatitis",
                         "n_samples": 300000, "n_cases": 1000, "n_controls": 299000, "data_type": "gwas"}],
            "finngen_custom_r14": [{"phenotype_code": "AIH", "phenotype_string": "AIH",
                                    "n_samples": 1897, "n_cases": 277, "n_controls": 1620, "data_type": "gwas"},
                                   {"phenotype_code": "AIH_prev", "phenotype_string": "AIH prevalent",
                                    "n_samples": 1500, "n_cases": 200, "n_controls": 1300, "data_type": "gwas"}],
        }

        class FakeDataAccess:
            def get_harmonized_metadata(self, resource, include_data_type=True):
                return rows[resource]

        idx = ss.SearchIndex.__new__(ss.SearchIndex)
        idx.phenotypes, idx.genes, idx.search_items, idx.on_request_items = [], [], [], []
        idx.genes_by_hgnc_id, idx._symbol_index = {}, None
        idx._data_access, idx._gene_name_mapping = FakeDataAccess(), None
        idx._load_phenotypes()
        return idx

    def test_default_search_sees_only_the_release_phenotype(self, monkeypatch):
        idx = self._index(monkeypatch)
        hits = idx.search("AIH", limit=10, types=["phenotypes"])
        assert [(h["resource"], h["code"]) for h in hits] == [("finngen", "AIH")]

    def test_naming_the_resource_searches_the_runs(self, monkeypatch):
        idx = self._index(monkeypatch)
        hits = idx.search("AIH", limit=10, types=["phenotypes"], resources=["finngen_custom_r14"])
        assert {(h["resource"], h["code"]) for h in hits} == {
            ("finngen_custom_r14", "AIH"), ("finngen_custom_r14", "AIH_prev")}

    def test_reload_keeps_the_split(self, monkeypatch):
        idx = self._index(monkeypatch)
        idx.reload_phenotypes()
        assert len(idx.on_request_items) == 3 and len(idx.search_items) == 2
