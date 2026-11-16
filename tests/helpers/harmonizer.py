"""MetadataHarmonizer run on an inline fixture, with the registry entry written by hand."""

from app.services.metadata_harmonizer import MetadataHarmonizer


def metadata_config(
    type: str, resource: str, author: str, publication_date: str, version_label: str
) -> dict:
    """The `metadata` block of a registry entry, which also selects the harmonizer."""
    return {
        "type": type,
        "author": author,
        "publication_date": publication_date,
        "version_label": version_label,
        "resource": resource,
    }


def harmonize_by_code(resource: str, items: list[dict], config: dict) -> dict:
    """Harmonized rows keyed by phenotype code."""
    rows = MetadataHarmonizer().harmonize_metadata(resource, items, {"metadata": config})
    return {row.phenotype_code: row for row in rows}
