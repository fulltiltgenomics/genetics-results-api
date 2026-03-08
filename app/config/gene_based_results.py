"""
Configuration settings for gene-based results.

This module contains settings for gene-based results, such as the file path
and other configuration parameters.
"""

gene_based_data_files = [
    {
        "id": "genebass_gene_based",
        "resource": "genebass",
        "data_source": "gcloud",
        "gencode_version": 35,
        "gene_based": {
            "file": "gs://finngen-commons/results_api_data/exome_results/genebass/gene_burden_results.tsv.gz",
        },
        "metadata": {
            "metadata_file": "gs://finngen-commons/results_api_data/mapping_files/genebass_pheno_results.txt.bgz",
            "type": "genebass",
            "author": "GeneBass",
            "publication_date": "2022-09-14",
            "version_label": "N_394841",
        },
    },
]

# build lookup dictionaries
gene_based_data_file_by_id = {df["id"]: df for df in gene_based_data_files}

# build resource to data file IDs mapping
resource_to_gene_based_data_file_ids = {}
for df in gene_based_data_files:
    resource = df["resource"]
    if resource not in resource_to_gene_based_data_file_ids:
        resource_to_gene_based_data_file_ids[resource] = []
    resource_to_gene_based_data_file_ids[resource].append(df["id"])
