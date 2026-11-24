"""Configuration for exome results data."""

from app.config import credible_sets
from app.config.profile import load_profile_module

# header schema for exome results
exome_header_schema = {
    "resource": str,  # added by API
    "version": str,  # added by API
    "dataset": str,
    "chr": int,
    "pos": int,
    "ref": str,
    "alt": str,
    "gene": str,
    "annotation": str,
    "mlog10p": float,
    "beta": float,
    "se": float,
    "af_overall": float,
    "af_cases": float,
    "af_controls": float,
    "ac": int,
    "an": int,
    "n_cases": int,
    "n_controls": int,
    "trait": str,
    "trait_original": str,
}

# column names for merging data across files: exome files name them as credible-set files do
variant_columns = credible_sets.variant_columns

# exome data files loaded from profile
_profile = load_profile_module("exome_results")
exome_data_files = _profile.exome_data_files

# build lookup dictionaries
exome_data_file_by_id = {df["id"]: df for df in exome_data_files}

# build resource to data file IDs mapping
resource_to_exome_data_file_ids = {}
for df in exome_data_files:
    resource = df["resource"]
    if resource not in resource_to_exome_data_file_ids:
        resource_to_exome_data_file_ids[resource] = []
    resource_to_exome_data_file_ids[resource].append(df["id"])
