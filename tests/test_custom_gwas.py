"""Custom GWAS resources, served straight from the userresults bucket.

Runs against whichever release the profile configures with a convention fallback (the
current one), on the example phenotype its credible-set entry names.
"""

import re

import pytest
import requests
from helpers.validators import validate_json_response, validate_tsv_response


@pytest.fixture(scope="session")
def custom_release():
    from app.config.custom_gwas import releases

    current = [r for r in releases if r.get("convention_fallback")]
    if not current:
        pytest.skip("No custom GWAS release configured in this profile")
    return current[0]


@pytest.fixture(scope="session")
def custom_resource(custom_release):
    return custom_release["resource"]


@pytest.fixture(scope="session")
def custom_phenotype(custom_release):
    """The phenotype the credible-set config names as its example; it has fine-mapping."""
    from app.config.credible_sets import data_file_by_id

    return data_file_by_id[custom_release["gwas_dataset_id"]]["example_pheno_or_study"]


@pytest.fixture(scope="session")
def custom_lead(server_url, custom_resource, custom_phenotype):
    """The strongest credible-set lead of the example phenotype, as a variant string."""
    r = requests.get(
        f"{server_url}/api/v1/credible_sets_by_phenotype_leads/{custom_resource}/{custom_phenotype}",
        params={"format": "json"},
    )
    assert r.status_code == 200, r.text
    leads = r.json()
    assert leads, "example phenotype has no credible sets"
    lead = max(leads, key=lambda row: row["mlog10p"] or 0)
    return lead, f"{lead['chr']}-{lead['pos']}-{lead['ref']}-{lead['alt']}"


class TestCatalogue:
    def test_default_catalogue_hides_the_release(self, server_url, custom_release):
        """on_request: the general listing never shows a user's run next to release data."""
        r = requests.get(f"{server_url}/api/v1/datasets")
        assert r.status_code == 200
        assert custom_release["gwas_dataset_id"] not in {d["dataset_id"] for d in r.json()}
        r = requests.get(f"{server_url}/api/v1/datasets", params={"data_type": "gwas"})
        assert custom_release["gwas_dataset_id"] not in {d["dataset_id"] for d in r.json()}

    def test_datasets_list_the_release_with_stats_when_asked_for(self, server_url, custom_release):
        r = requests.get(f"{server_url}/api/v1/datasets", params={"resource": custom_release["resource"]})
        assert r.status_code == 200
        by_id = {d["dataset_id"]: d for d in r.json()}
        gwas = by_id[custom_release["gwas_dataset_id"]]
        assert gwas["products"] == {"credible_sets": True, "summary_stats": True}
        assert gwas["stats"]["n_phenotypes"] > 100
        assert gwas["metadata_endpoint"].endswith(custom_release["resource"])
        if custom_release.get("hla_dataset_id"):
            assert by_id[custom_release["hla_dataset_id"]]["products"] == {"summary_stats": True}

    def test_on_request_listing_is_every_hidden_dataset_and_nothing_else(self, server_url, custom_release):
        """The probe a chat backend uses to learn which on-request resources this
        deployment serves; a resource filter still applies to it."""
        from app.services import config_util

        r = requests.get(f"{server_url}/api/v1/datasets/on_request")
        assert r.status_code == 200
        listed = {d["dataset_id"]: d for d in r.json()}
        assert set(listed) == {d for d in config_util.get_datasets() if config_util.is_on_request(d)}
        assert listed[custom_release["gwas_dataset_id"]]["resource"] == custom_release["resource"]
        assert "stats" not in listed[custom_release["gwas_dataset_id"]]
        r = requests.get(f"{server_url}/api/v1/datasets/on_request", params={"resource": "finngen"})
        assert r.status_code == 200 and r.json() == []

    def test_resource_metadata_carries_the_run_descriptions(self, server_url, custom_resource, custom_phenotype):
        r = requests.get(f"{server_url}/api/v1/resource_metadata/{custom_resource}", params={"format": "json"})
        assert r.status_code == 200
        rows = {row["phenotype_code"]: row for row in r.json()}
        assert custom_phenotype in rows
        row = rows[custom_phenotype]
        assert row["trait_type"] in ("binary", "quantitative")
        assert row["version"] and row["resource"] == custom_resource
        assert any(v["n_cases"] not in ("NA", 0) for v in rows.values())
        # the day the pipeline wrote the run is served; who ran it is not
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", row["date"]), row["date"]
        assert row["author"] == "FinnGen sandbox users"

    def test_one_run_is_reachable_by_name(self, server_url, custom_resource, custom_phenotype):
        """A release holds hundreds of runs; the chat reads one by `phenotypes` rather than
        paging the whole table, where a row cap would cut an alphabetically late run."""
        r = requests.get(
            f"{server_url}/api/v1/resource_metadata/{custom_resource}",
            params={"format": "json", "phenotypes": custom_phenotype},
        )
        assert r.status_code == 200
        (row,) = r.json()
        assert row["phenotype_code"] == custom_phenotype and row["date"]

    def test_default_search_never_returns_a_custom_run(self, server_url, custom_resource, custom_phenotype):
        r = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": custom_phenotype, "types": "phenotypes", "limit": 100, "format": "json"},
        )
        assert r.status_code == 200, r.text
        assert not [h for h in r.json() if h.get("resource") == custom_resource]

    def test_search_finds_a_custom_run_when_its_resource_is_named(self, server_url, custom_resource, custom_phenotype):
        r = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": custom_phenotype, "types": "phenotypes", "limit": 50, "format": "json",
                    "resources": custom_resource},
        )
        assert r.status_code == 200, r.text
        hits = r.json()
        assert hits and {h["resource"] for h in hits} == {custom_resource}
        hit = next(h for h in hits if h["code"] == custom_phenotype)
        assert hit["has_summary_stats"] is True and hit["has_credible_sets"] is True

    def test_resources_filter_also_narrows_ordinary_resources(self, server_url):
        r = requests.get(
            f"{server_url}/api/v1/search",
            params={"q": "asthma", "types": "phenotypes", "limit": 50, "format": "json",
                    "resources": "finngen"},
        )
        assert r.status_code == 200, r.text
        assert r.json() and {h["resource"] for h in r.json()} == {"finngen"}


class TestCredibleSets:
    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_by_phenotype(self, server_url, custom_resource, custom_phenotype, format):
        r = requests.get(
            f"{server_url}/api/v1/credible_sets_by_phenotype/{custom_resource}/{custom_phenotype}",
            params={"format": format},
        )
        assert r.status_code == 200, r.text
        if format == "tsv":
            parsed = validate_tsv_response(r.text, min_data_lines=1)
            assert parsed["header"][:5] == ["dataset", "data_type", "trait", "trait_original", "cell_type"]
            assert "cs_id" in parsed["header"] and "pip" in parsed["header"]
        else:
            rows = r.json()
            assert validate_json_response(rows, min_items=1)["valid"]
            assert rows[0]["data_type"] == "GWAS" and rows[0]["trait_original"] == custom_phenotype
            assert 0 <= rows[0]["pip"] <= 1

    def test_by_id_returns_only_that_set(self, server_url, custom_resource, custom_phenotype, custom_lead):
        lead, _ = custom_lead
        r = requests.get(
            f"{server_url}/api/v1/credible_sets_by_id/{custom_resource}/{custom_phenotype}/{lead['cs_id']}",
            params={"format": "json"},
        )
        assert r.status_code == 200, r.text
        rows = r.json()
        assert rows and {row["cs_id"] for row in rows} == {lead["cs_id"]}
        assert len(rows) == lead["cs_size"]

    def test_unknown_run_is_404(self, server_url, custom_resource):
        r = requests.get(f"{server_url}/api/v1/credible_sets_by_phenotype/{custom_resource}/NO_SUCH_RUN_XYZ")
        assert r.status_code == 404

    def test_run_without_finemapping_is_404_not_500(self, server_url, custom_resource, custom_release):
        from app.core.service_container import container

        catalog = container.get("custom_gwas_catalogs").get(custom_release["id"])
        without = next(
            (n for n in catalog.phenotype_names() if catalog.susie_paths(n) is None and catalog.sumstats_path(n)),
            None,
        )
        if without is None:
            pytest.skip("every run in the catalog has fine-mapping")
        r = requests.get(f"{server_url}/api/v1/credible_sets_by_phenotype/{custom_resource}/{without}")
        assert r.status_code == 404


class TestSummaryStats:
    @pytest.mark.parametrize("format", ["tsv", "json"])
    def test_lead_variant_has_a_row(self, server_url, custom_resource, custom_phenotype, custom_lead, format):
        lead, variant = custom_lead
        r = requests.get(
            f"{server_url}/api/v1/summary_stats/{custom_resource}/gwas",
            params={"variants": variant, "phenotypes": custom_phenotype, "format": format},
        )
        assert r.status_code == 200, r.text
        if format == "json":
            rows = r.json()
            assert len(rows) == 1
            row = rows[0]
            assert (row["resource"], row["phenotype"]) == (custom_resource, custom_phenotype)
            assert row["pos"] == lead["pos"] and row["mlog10p"] > 5
            assert isinstance(row["info"], float)
        else:
            parsed = validate_tsv_response(r.text, min_data_lines=1)
            assert parsed["header"][:3] == ["resource", "version", "phenotype"]

    def test_region_query(self, server_url, custom_resource, custom_phenotype, custom_lead):
        lead, _ = custom_lead
        region = f"{lead['chr']}:{lead['pos'] - 5000}-{lead['pos'] + 5000}"
        r = requests.get(
            f"{server_url}/api/v1/summary_stats_by_region/{custom_resource}/gwas/{region}",
            params={"phenotypes": custom_phenotype, "format": "json"},
        )
        assert r.status_code == 200, r.text
        rows = r.json()
        assert any(row["pos"] == lead["pos"] for row in rows)
        assert all(lead["pos"] - 5000 <= row["pos"] <= lead["pos"] + 5000 for row in rows)

    def test_unknown_run_is_404(self, server_url, custom_resource, custom_lead):
        _, variant = custom_lead
        r = requests.get(
            f"{server_url}/api/v1/summary_stats/{custom_resource}/gwas",
            params={"variants": variant, "phenotypes": "NO_SUCH_RUN_XYZ"},
        )
        assert r.status_code == 404


class TestHla:
    @pytest.fixture(scope="session")
    def hla_run(self, custom_release):
        if not custom_release.get("hla_dataset_id"):
            pytest.skip("release has no HLA product")
        from app.core.service_container import container

        catalog = container.get("custom_gwas_catalogs").get(custom_release["id"])
        run = next((n for n in catalog.phenotype_names() if catalog.phenotype(n).hla), None)
        if run is None:
            pytest.skip("no run with an HLA file")
        return run

    def test_whole_profile(self, server_url, custom_resource, hla_run):
        r = requests.get(f"{server_url}/api/v1/hla/{custom_resource}", params={"phenotypes": hla_run, "format": "json"})
        assert r.status_code == 200, r.text
        rows = r.json()
        assert len(rows) > 50
        genes = {row["gene"] for row in rows}
        assert {"HLA-A", "HLA-B", "HLA-DQB1"} <= genes
        row = next(row for row in rows if row["gene"] == "HLA-A")
        assert row["allele"].startswith("A*") and isinstance(row["info"], float)
        assert row["phenotype"] == hla_run and row["resource"] == custom_resource

    def test_gene_filter(self, server_url, custom_resource, hla_run):
        r = requests.get(
            f"{server_url}/api/v1/hla/{custom_resource}",
            params={"phenotypes": hla_run, "genes": "HLA-B", "format": "json"},
        )
        assert r.status_code == 200, r.text
        rows = r.json()
        assert rows and {row["gene"] for row in rows} == {"HLA-B"}
