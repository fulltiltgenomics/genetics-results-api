"""
Central dataset registry loaded from datasets.yaml via the YAML loader.

Each dataset_id maps to a dict with resource, version, description, author,
publication_date, trait_type, data_type, metadata_file, metadata_harmonizer,
and optionally collection + subdataset_id_field.
See docs/datasets-yaml-schema.md for full field documentation.
"""

from app.config.yaml_loader import datasets


def get_dataset(dataset_id: str) -> dict | None:
    """Return registry entry for a dataset_id, or None if not found."""
    return datasets.get(dataset_id)



def build_harmonizer_config(dataset_id: str) -> dict | None:
    """Build the legacy `config` dict (nested under 'metadata') that
    MetadataHarmonizer expects, from a registry entry. Returns None if the
    dataset has no metadata_file.
    """
    entry = datasets.get(dataset_id)
    if not entry:
        return None
    if not entry.get("metadata_file"):
        # a sandbox custom GWAS dataset has no metadata file: its rows come from the
        # release's bucket catalog, addressed by the `catalog://` scheme that
        # data_access._read_metadata_file resolves
        from app.config.custom_gwas import release_for_dataset

        release = release_for_dataset(dataset_id)
        if release is None:
            return None
        return {
            "metadata": {
                "type": "custom_gwas",
                "author": entry.get("author"),
                "publication_date": entry.get("publication_date"),
                "version_label": entry.get("version"),
                "metadata_file": f"catalog://{release['id']}",
                "resource": entry.get("resource"),
            }
        }
    return {
        "metadata": {
            "type": entry.get("metadata_harmonizer"),
            "author": entry.get("author"),
            "publication_date": entry.get("publication_date"),
            "version_label": entry.get("version"),
            "metadata_file": entry.get("metadata_file"),
            # only the harmonizers that are not FinnGen-specific read this; the older ones
            # hardcode "finngen" and are only ever pointed at FinnGen metadata files
            "resource": entry.get("resource"),
        }
    }
