"""
Summary statistics configuration including column mappings per resource/data type.

Each data file entry defines:
- dataset_id: FK into app/config/datasets.py — the registry is the single source
  of truth for `data_type`, `version`, and other dataset-level provenance.
- resource: resource routing key (also present on the dataset entry — kept here
  because profile configs may colocate sumstats for a resource differently)
- prefix/suffix: path pattern where per-phenotype files live ({prefix}{phenotype}{suffix})
- OR file/phenotype: single file path + phenotype code for external sumstats
- column_mapping: maps file column names to unified output column names.
  Only columns present in the mapping are included in output.

Fields derived from the dataset registry (populated on module load):
- version: copied from datasets[dataset_id]["version"]
"""

from app.config.datasets import datasets as _datasets
from app.config.profile import load_profile_module

_profile = load_profile_module("summary_stats")
data_files = _profile.data_files

# fields copied from the dataset registry onto each data_files entry at load
# time so downstream code has a flat view without the profile config restating them
_REGISTRY_DERIVED_FIELDS = ("version",)

# validate dataset_id FK at load time so profile misconfig fails fast, and
# enrich each entry with registry-derived fields
for _df in data_files:
    _dsid = _df.get("dataset_id")
    if _dsid is None:
        raise KeyError(
            f"summary_stats entry {_df.get('id')!r} is missing required 'dataset_id' field"
        )
    if _dsid not in _datasets:
        raise KeyError(
            f"summary_stats entry {_df['id']!r} references unknown dataset_id {_dsid!r}"
        )
    for _field in _REGISTRY_DERIVED_FIELDS:
        if _field in _df:
            raise KeyError(
                f"summary_stats entry {_df['id']!r} sets {_field!r} directly; "
                f"this field is derived from the dataset registry and must not be duplicated"
            )
        _df[_field] = _datasets[_dsid][_field]

data_file_by_id = {df["id"]: df for df in data_files}


def _data_type_of(df: dict) -> str:
    """Resolve the data_type for a summary_stats entry via the dataset registry."""
    return _datasets[df["dataset_id"]]["data_type"]


def get_data_files_by_resource_and_type(resource: str, data_type: str) -> list[dict]:
    """Get all data file configs matching a resource and data_type."""
    return [
        df
        for df in data_files
        if df["resource"] == resource and _data_type_of(df) == data_type
    ]


def get_available_resources_and_types() -> list[tuple[str, str]]:
    """Get all configured (resource, data_type) pairs."""
    return sorted(set((df["resource"], _data_type_of(df)) for df in data_files))
