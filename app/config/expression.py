"""
Expression data configuration including schemas and data sources.

This module contains all configuration related to expression data:
- Header schema for expression data validation
- Expression data source configurations (GTEx, HPA, etc.)
"""

expression_header_schema = {
    "resource": str,
    "version": str,
    "dataset": str,
    "chrom": int,
    "gene_start": int,
    "gene_end": int,
    "gene_name": str,
    "gene_id": str,
    "tissue_cell": str,
    "level": str,  # can be numeric or string depending on data source
}

# column names for merging data across files
simple_columns = {
    "dataset": b"dataset",
}

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
