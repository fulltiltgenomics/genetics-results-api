"""Daly profile: exome results data paths."""

exome_data_files = [
    {
        "id": "genebass",
        "dataset_id": "genebass_exome",
        "resource": "genebass",
        "data_source": "gcloud",
        "example_pheno_or_study": "categorical_41210_both_sexes_S068_",
        "gencode_version": 35,
        "exome": {
            "version": "v1",
            "prefix": "gs://daly-genetics-results/exome_results/genebass/individual/",
            "suffix": ".mlog10p4.tsv",
            "all_exome_file": "gs://daly-genetics-results/exome_results/genebass/genebass_variant_results_mlog10p4.tsv.gz",
        },
    },
]
