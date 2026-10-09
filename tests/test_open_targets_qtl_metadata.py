"""The Open Targets QTL sub-study metadata: harmonized for the dataset's sample-size stats,
and kept out of the resource's phenotype metadata (search index, /trait_name_mapping)."""

from app.config.datasets import datasets as registry
from app.services import config_util, dataset_stats
from app.services.metadata_harmonizer import MetadataHarmonizer

RAW = [
    {"substudy": "GTEx_v10_lung_ge", "cell_type": "lung|naive", "data_type": "eQTL", "n_samples": "604"},
    {"substudy": "IBDverse_CD4+_CRM_ge", "cell_type": "CD4+_CRM|naive", "data_type": "eQTL", "n_samples": "339"},
    {"substudy": "GTEx_v10_x_ge", "cell_type": "x|naive", "data_type": "eQTL", "n_samples": "NA"},
]

CONFIG = {
    "metadata": {
        "type": "open_targets_qtl",
        "author": "Open Targets",
        "publication_date": "2026-09-24",
        "version_label": "26.09",
        "resource": "open_targets",
    }
}


def test_harmonizer_reads_one_row_per_substudy():
    rows = [h.to_dict() for h in MetadataHarmonizer().harmonize_metadata("open_targets", RAW, CONFIG)]
    assert [(r["phenotype_code"], r["phenotype_string"], r["n_samples"]) for r in rows] == [
        ("GTEx_v10_lung_ge", "lung|naive", 604),
        ("IBDverse_CD4+_CRM_ge", "CD4+_CRM|naive", 339),
        ("GTEx_v10_x_ge", "x|naive", "NA"),
    ]
    assert {r["trait_type"] for r in rows} == {"quantitative"}
    assert {r["resource"] for r in rows} == {"open_targets"}


def _dataset_with_resource_metadata():
    """A (resource, dataset_id) whose metadata feeds its resource, in whichever profile runs."""
    for resource in config_util.get_resources_with_metadata():
        ids = config_util.get_metadata_dataset_ids_for_resource(resource)
        if ids:
            return resource, ids[0]
    raise AssertionError("no dataset feeds any resource's metadata")


def test_substudy_metadata_stays_out_of_the_resource_metadata(monkeypatch):
    resource, dataset_id = _dataset_with_resource_metadata()
    monkeypatch.setitem(registry, dataset_id, {**registry[dataset_id], "substudy_metadata": True})

    assert dataset_id not in config_util.get_metadata_dataset_ids_for_resource(
        resource, include_coloc_partners=True
    )


def test_substudy_metadata_stats_count_substudies(monkeypatch):
    _, dataset_id = _dataset_with_resource_metadata()
    monkeypatch.setitem(registry, dataset_id, {**registry[dataset_id], "substudy_metadata": True})
    harmonized = [h.to_dict() for h in MetadataHarmonizer().harmonize_metadata("open_targets", RAW, CONFIG)]
    monkeypatch.setattr(dataset_stats, "_load_and_harmonize", lambda *_: harmonized)
    dataset_stats.clear_cache()
    try:
        stats = dataset_stats.get_dataset_stats(dataset_id, None)
    finally:
        dataset_stats.clear_cache()

    assert stats == {"n_subdatasets": 3, "n_samples_median": 471, "n_samples_range": [339, 604]}
