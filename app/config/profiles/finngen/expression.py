"""FinnGen profile: expression data paths."""

expression_data = [
    {
        "data_source": "gcloud",
        "resource": "gtex",
        "gencode_version": 39,
        "file": "gs://finngen-commons/results_api_data/expression/gtex_v10_median_tpm.long.tsv.gz",
    },
    {
        "data_source": "gcloud",
        "resource": "hpa",
        "gencode_version": 43,
        "file": "gs://finngen-commons/results_api_data/expression/hpa_normal_ihc_data_24.1.long.tsv.gz",
    },
]
