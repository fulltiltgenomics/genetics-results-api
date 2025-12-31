"""
Configuration settings for gene-based results.

This module contains settings for gene-based results, such as the file path
and other configuration parameters.
"""

gene_based_data_files = [
    {
        "id": "genebass_gene_based",
        "resource": "genebass",
        "data_source": "local",
        "gencode_version": 35,
        "gene_based": {
            "file": "/mnt/disks/data/gene_burden_results.tsv.gz",
        },
        "metadata": {
            "metadata_file": "/mnt/disks/data/pheno_results.txt.bgz",
            "type": "genebass",
            "author": "GeneBass",
            "publication_date": "2022-01-01",
            "version_label": "500k",
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
