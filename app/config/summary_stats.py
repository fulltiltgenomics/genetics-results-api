"""
Summary statistics configuration including column mappings per resource/data type.

Each data file entry defines:
- resource/version/data_type: identifies which resource and data type this file belongs to
- prefix/suffix: GCS path pattern where per-phenotype files live ({prefix}{phenotype}{suffix})
- column_mapping: maps file column names to unified output column names.
  Only columns present in the mapping are included in output.
"""

from app.config.profile import load_profile_module

_profile = load_profile_module("summary_stats")
data_files = _profile.data_files

data_file_by_id = {df["id"]: df for df in data_files}


def get_data_files_by_resource_and_type(resource: str, data_type: str) -> list[dict]:
    """Get all data file configs matching a resource and data_type."""
    return [
        df
        for df in data_files
        if df["resource"] == resource and df["data_type"] == data_type
    ]


def get_available_resources_and_types() -> list[tuple[str, str]]:
    """Get all configured (resource, data_type) pairs."""
    return sorted(set((df["resource"], df["data_type"]) for df in data_files))
