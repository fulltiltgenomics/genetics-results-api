"""
Configuration settings for gene-based results.

This module contains settings for gene-based results, such as the file path
and other configuration parameters.
"""

from app.config.profile import load_profile_module

_profile = load_profile_module("gene_based_results")
gene_based_data_files = _profile.gene_based_data_files

# build lookup dictionaries
gene_based_data_file_by_id = {df["id"]: df for df in gene_based_data_files}

# build resource to data file IDs mapping
resource_to_gene_based_data_file_ids = {}
for df in gene_based_data_files:
    resource = df["resource"]
    if resource not in resource_to_gene_based_data_file_ids:
        resource_to_gene_based_data_file_ids[resource] = []
    resource_to_gene_based_data_file_ids[resource].append(df["id"])
