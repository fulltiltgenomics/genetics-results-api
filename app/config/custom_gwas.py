"""Sandbox custom GWAS releases: the profile's list, validated against the dataset registry.

A release entry names the bucket prefix the userresults pipeline writes to and the registry
datasets its products are served under. The product configs (summary_stats, credible_sets)
reference a release by `catalog: <id>`; the catalog service in
app/services/custom_gwas_catalog.py resolves phenotype names to paths under that prefix.
"""

from app.config.datasets import datasets as _datasets
from app.config.profile import load_profile_module

_REQUIRED = ("id", "resource", "bucket", "prefix", "dataset_label", "gwas_dataset_id")

releases: list[dict] = load_profile_module("custom_gwas").releases

for _r in releases:
    for _field in _REQUIRED:
        if not _r.get(_field):
            raise KeyError(f"custom_gwas release {_r.get('id')!r} is missing {_field!r}")
    if not _r["prefix"].endswith("/"):
        raise ValueError(f"custom_gwas release {_r['id']!r}: prefix must end with '/'")
    for _key in ("gwas_dataset_id", "hla_dataset_id"):
        _dsid = _r.get(_key)
        if _dsid is not None and _dsid not in _datasets:
            raise KeyError(
                f"custom_gwas release {_r['id']!r} references unknown {_key} {_dsid!r}"
            )
    _r.setdefault("convention_fallback", False)
    _r.setdefault("refresh_seconds", 900)

release_by_id: dict[str, dict] = {r["id"]: r for r in releases}
if len(release_by_id) != len(releases):
    raise ValueError("custom_gwas release ids must be unique")


def release_for_dataset(dataset_id: str) -> dict | None:
    """The release whose GWAS or HLA product is registered under `dataset_id`, if any."""
    for r in releases:
        if dataset_id in (r["gwas_dataset_id"], r.get("hla_dataset_id")):
            return r
    return None


def base_path(release: dict) -> str:
    return f"gs://{release['bucket']}/{release['prefix']}"
