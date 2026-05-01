"""
Central dataset registry loaded from the active profile.

Each dataset_id maps to a dict with resource, version, description, author,
publication_date, trait_type, data_type, metadata_file, metadata_harmonizer,
and optionally collection + subdataset_id_field.
See app/config/profiles/daly/datasets.py for full field documentation.
"""

from app.config.profile import load_profile_module

_profile = load_profile_module("datasets")
datasets = _profile.datasets


def get_dataset(dataset_id: str) -> dict | None:
    """Return registry entry for a dataset_id, or None if not found."""
    return datasets.get(dataset_id)


def get_datasets_by_resource(resource: str) -> dict[str, dict]:
    """Return all dataset registry entries matching a resource name."""
    return {k: v for k, v in datasets.items() if v.get("resource") == resource}


def build_harmonizer_config(dataset_id: str) -> dict | None:
    """Build the legacy `config` dict (nested under 'metadata') that
    MetadataHarmonizer expects, from a registry entry. Returns None if the
    dataset has no metadata_file.
    """
    entry = datasets.get(dataset_id)
    if not entry or not entry.get("metadata_file"):
        return None
    return {
        "metadata": {
            "type": entry.get("metadata_harmonizer"),
            "author": entry.get("author"),
            "publication_date": entry.get("publication_date"),
            "version_label": entry.get("version"),
            "metadata_file": entry.get("metadata_file"),
        }
    }
