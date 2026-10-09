"""
Central dataset registry loaded from datasets.yaml via the YAML loader.

Each dataset_id maps to a dict with resource, version, description, author,
publication_date, trait_type, data_type, metadata_file, metadata_harmonizer,
and optionally collection + subdataset_id_field, or substudy_metadata (a metadata_file
listing the dataset's sub-studies rather than phenotypes; it feeds only the dataset's stats).
See docs/datasets-yaml-schema.md for full field documentation.
"""

from app.config.yaml_loader import datasets


def _dataset_to_resource(registry: dict[str, dict]) -> dict[str, tuple[str, str]]:
    """`dataset` column value -> (resource, version), from each entry's `dataset` field.

    The label is what the munged files carry in their `dataset` column, so this is how a row
    is attributed to a resource; rows whose label has no entry resolve to "unknown" and are
    dropped from shared-file range queries. Where several entries share a label (pgc_scz +
    pgc_bip are both "PGC"; finngen_kanta and finngen_kanta_r12 are both "FinnGen_kanta") the
    first entry in registry order wins, so the version is the current release's and the
    resource - the part that matters for filtering - is the same either way.
    """
    mapping: dict[str, tuple[str, str]] = {}
    for entry in registry.values():
        labels = entry.get("dataset")
        if labels is None:
            continue
        for label in labels if isinstance(labels, list) else [labels]:
            mapping.setdefault(label, (entry["resource"], str(entry.get("version"))))
    return mapping


dataset_to_resource = _dataset_to_resource(datasets)


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
