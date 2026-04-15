"""Daly profile: exome results data paths."""

exome_data_files = [
    {
        "id": "genebass",
        "dataset_id": "genebass_exome",
        "resource": "genebass",
        "data_source": "gcloud",
        "example_pheno_or_study": "categorical_41210_both_sexes_S068_",
        "gencode_version": 35,
        "metadata": {
            "metadata_file": "gs://daly-genetics-results/mapping_files/genebass_pheno_results.txt.bgz",
            "type": "genebass",
            "author": "GeneBass",
            "publication_date": "2022-01-01",
            "version_label": "500k",
        },
        "exome": {
            "version": "v1",
            "prefix": "gs://daly-genetics-results/exome_results/genebass/individual/",
            "suffix": ".mlog10p4.tsv",
            "all_exome_file": "gs://daly-genetics-results/exome_results/genebass/genebass_variant_results_mlog10p4.tsv.gz",
        },
    },
]
