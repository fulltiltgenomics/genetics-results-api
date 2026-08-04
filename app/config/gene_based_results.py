"""
Configuration settings for gene-based results.

This module contains settings for gene-based results, such as the file path
and other configuration parameters.
"""

from app.config.profile import load_profile_module

# header schema for gene burden results. `file` holds every trait of a dataset
# indexed on the gene locus and backs /gene_based/{gene}; `prefix`/`suffix` point
# at the unfiltered per-trait copies that /gene_based_results_by_phenotype streams.
gene_based_header_schema = {
    "dataset": str,
    "trait": str,
    "gene": str,
    "gene_id": str,
    "gene_chr": int,
    "gene_start_pos": int,
    "gene_end_pos": int,
    "annotation": str,
    "mlog10p_burden": float,
    "beta": float,
    "se": float,
    "total_variants": int,
    "total_variants_pheno": int,
    "n_cases": int,
    "n_controls": int,
    "trait_original": str,
    "flags": str,
}

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
