"""Daly profile: expression data paths."""

expression_data = [
    {
        "data_source": "gcloud",
        "resource": "gtex",
        "gencode_version": 39,
        "file": "gs://daly-genetics-results/expression/gtex_v10_median_tpm.long.tsv.gz",
    },
    {
        "data_source": "gcloud",
        "resource": "hpa",
        "gencode_version": 43,
        "file": "gs://daly-genetics-results/expression/hpa_normal_ihc_data_26.0.long.tsv.gz",
    },
]
