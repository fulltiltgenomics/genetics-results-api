"""Daly profile: gene-based results data paths."""

gene_based_data_files = [
    {
        "id": "genebass_gene_based",
        "dataset_id": "genebass_gene_based",
        "resource": "genebass",
        "data_source": "gcloud",
        "gencode_version": 35,
        "gene_based": {
            "file": "gs://daly-genetics-results/exome_results/genebass/gene_burden_results.tsv.gz",
        },
    },
    {
        "id": "schema_gene_based",
        "dataset_id": "schema_gene_based",
        "resource": "schema",
        "data_source": "gcloud",
        "gencode_version": 19,
        "gene_based": {
            "file": "gs://finngen-commons/results_api_data/exome_results/schema/SCHEMA_gene_results.munged.tsv.gz",
        },
    },
]
