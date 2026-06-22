"""
Credible sets configuration including schemas and data files.
"""

from app.config.datasets import datasets as _datasets
from app.config.profile import load_profile_module

cs_header_schema = {
    "resource": str,  # this is added to the header by the api
    "version": str,  # this is added to the header by the api
    "dataset": str,
    "data_type": str,
    "trait": str,
    "trait_original": str,
    "cell_type": str,
    "chr": int,
    "pos": int,
    "ref": str,
    "alt": str,
    "mlog10p": float,
    "beta": float,
    "se": float,
    "pip": float,
    "cs_id": str,
    "cs_size": int,
    "cs_min_r2": float,
    "aaf": float,
    "most_severe": str,
    "gene_most_severe": str,
}

cs_qtl_header_schema = {
    "resource": str,  # this is added to the header by the api
    "version": str,  # this is added to the header by the api
    "dataset": str,
    "data_type": str,
    "trait": str,
    "trait_original": str,
    "cell_type": str,
    "chr": int,
    "pos": int,
    "ref": str,
    "alt": str,
    "mlog10p": float,
    "beta": float,
    "se": float,
    "pip": float,
    "cs_id": str,
    "cs_size": int,
    "cs_min_r2": float,
    "aaf": float,
    "most_severe": str,
    "gene_most_severe": str,
    "trait_chr": int,
    "trait_start": int,
    "trait_end": int,
}

# column names for merging data across files
variant_columns = {
    "chr": b"chr",
    "pos": b"pos",
    "ref": b"ref",
    "alt": b"alt",
    "dataset": b"dataset",
}

qtl_columns = {
    "trait_start": b"trait_start",
    "trait_end": b"trait_end",
    "dataset": b"dataset",
}

# data files loaded from the active profile
_profile = load_profile_module("credible_sets")
data_files = _profile.data_files


def _build_data_file_by_id():
    """Build lookup dict from data file ID to data file config."""
    return {df["id"]: df for df in data_files}


def _build_resource_to_data_file_ids():
    """Build mapping from resource name to list of data file IDs."""
    mapping = {}
    for df in data_files:
        resource = df.get("resource", df["id"])
        if resource not in mapping:
            mapping[resource] = []
        mapping[resource].append(df["id"])
    return mapping


data_file_by_id = _build_data_file_by_id()
resource_to_data_file_ids = _build_resource_to_data_file_ids()


def _data_type_of(df: dict) -> str:
    """Resolve the data_type for a credible_sets entry via the dataset registry."""
    return _datasets[df["dataset_id"]]["data_type"]


def get_credible_set_resources_and_types() -> list[tuple[str, str]]:
    """All (resource, data_type) pairs that have credible sets available.

    Mirrors summary_stats.get_available_resources_and_types so the search index can flag
    has_credible_sets the same way it flags has_summary_stats (per (resource, data_type)).
    """
    return sorted({(df["resource"], _data_type_of(df)) for df in data_files})
