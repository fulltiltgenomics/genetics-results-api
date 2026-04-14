from app.config.credible_sets import (
    data_files as cs_data_files,
    data_file_by_id as cs_data_file_by_id,
    resource_to_data_file_ids as cs_resource_to_data_file_ids,
)
from app.config.exome_results import (
    exome_data_files,
    exome_data_file_by_id,
    resource_to_exome_data_file_ids,
)
from app.config.gene_based_results import (
    gene_based_data_files,
    gene_based_data_file_by_id,
    resource_to_gene_based_data_file_ids,
)
from app.config.datasets import datasets as _dataset_registry
from app.config.coloc import coloc as _coloc_configs
from app.config.summary_stats import data_files as _sumstats_data_files

# build coloc partner index from explicit pairs
_coloc_partners: dict[str, set[str]] = {}
for _c in _coloc_configs:
    for _ds_a, _ds_b in _c.get("pairs", []):
        if _ds_a not in _dataset_registry:
            raise ValueError(f"coloc pair references unknown dataset_id {_ds_a!r} in {_c['name']!r}")
        if _ds_b not in _dataset_registry:
            raise ValueError(f"coloc pair references unknown dataset_id {_ds_b!r} in {_c['name']!r}")
        _coloc_partners.setdefault(_ds_a, set()).add(_ds_b)
        _coloc_partners.setdefault(_ds_b, set()).add(_ds_a)

# merge configurations
data_files = cs_data_files + exome_data_files + gene_based_data_files
data_file_by_id = {**cs_data_file_by_id, **exome_data_file_by_id, **gene_based_data_file_by_id}

# merge resource mappings
resource_to_data_file_ids = {}
for resource, ids in cs_resource_to_data_file_ids.items():
    resource_to_data_file_ids[resource] = ids
for resource, ids in resource_to_exome_data_file_ids.items():
    if resource in resource_to_data_file_ids:
        resource_to_data_file_ids[resource].extend(ids)
    else:
        resource_to_data_file_ids[resource] = ids
for resource, ids in resource_to_gene_based_data_file_ids.items():
    if resource in resource_to_data_file_ids:
        resource_to_data_file_ids[resource].extend(ids)
    else:
        resource_to_data_file_ids[resource] = ids


def get_data_file_ids_for_resource(resource: str) -> list[str]:
    """Get all data file IDs for a given resource name."""
    return resource_to_data_file_ids.get(resource, [])


def get_resources(data_type: str | None = None) -> list[str]:
    """Get a list of unique resources from the config, optionally filtered by data type."""
    if data_type is None:
        return sorted(list(resource_to_data_file_ids.keys()))

    # filter resources that have at least one data file with the requested data type
    filtered_resources = []
    for resource in resource_to_data_file_ids.keys():
        data_file_ids = get_data_file_ids_for_resource(resource)
        for data_file_id in data_file_ids:
            df = data_file_by_id.get(data_file_id)
            if df and data_type in df:
                filtered_resources.append(resource)
                break
    return sorted(filtered_resources)


def get_resources_with_metadata() -> list[str]:
    """Get the resources with metadata from the config."""
    resources_with_meta = set()
    for df in data_files:
        # check if metadata section exists and has metadata_file
        metadata_config = df.get("metadata", {})
        if metadata_config.get("metadata_file"):
            resource = df.get("resource", df["id"])
            resources_with_meta.add(resource)
    return sorted(list(resources_with_meta))


def get_datasets() -> dict[str, dict]:
    """Return the central dataset registry."""
    return _dataset_registry


def dataset_products(dataset_id: str) -> dict:
    """Determine which products a dataset supports (credible sets, summary stats, colocalization)."""
    products: dict = {}

    if dataset_id in cs_data_file_by_id:
        products["credible_sets"] = True

    # summary stats: check if any sumstats entry references this dataset
    for df in _sumstats_data_files:
        if df.get("dataset_id") == dataset_id:
            products["summary_stats"] = True
            break

    partners = _coloc_partners.get(dataset_id)
    if partners:
        products["colocalization"] = {"partners": sorted(partners)}

    return products
