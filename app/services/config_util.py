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
from app.config.datasets import datasets as _dataset_registry, build_harmonizer_config
from app.config.coloc import coloc as _coloc_configs
from app.config.summary_stats import data_files as _sumstats_data_files
from app.config.expression import expression_data as _expression_data
from app.config.chromatin_peaks import chromatin_peaks_data as _chromatin_peaks_data
from app.config.gene_disease import gene_disease as _gene_disease_config

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

# build dataset_id lookups for exome / gene_based / expression / chromatin_peaks / gene_disease
_exome_dataset_ids = {df["dataset_id"] for df in exome_data_files if "dataset_id" in df}
_gene_based_dataset_ids = {df["dataset_id"] for df in gene_based_data_files if "dataset_id" in df}
_expression_dataset_ids = {f"{d['resource']}_expression" for d in _expression_data}
_chromatin_peaks_dataset_ids = {f"{d['resource']}_chromatin_peaks" for d in _chromatin_peaks_data}
_gene_disease_dataset_ids = {k for k in _gene_disease_config if k != "output_columns"}

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


def get_metadata_dataset_ids_for_resource(
    resource: str, include_coloc_partners: bool = False
) -> list[str]:
    """Dataset IDs whose metadata should populate a resource's harmonized metadata.

    Always includes the datasets backing the resource's products (credible sets,
    exome, gene-based). When ``include_coloc_partners`` is set, also includes
    coloc-partner datasets that belong to this resource but ship no product of
    their own (e.g. ``finngen_kanta_r12``, retained only as the eQTL/caQTL coloc
    partner). Those partners are referenced in coloc results by their own
    phenocodes (bare OMOP ids for the R12 Kanta labs), so their names must be
    resolvable via /resource_metadata and /trait_name_mapping even though the
    search index (which lists independently queryable phenotypes) leaves them out.
    """
    dataset_ids: list[str] = []
    seen: set[str] = set()

    def _add(ds_id: str | None) -> None:
        if ds_id and ds_id not in seen:
            seen.add(ds_id)
            dataset_ids.append(ds_id)

    for data_file_id in get_data_file_ids_for_resource(resource):
        df = data_file_by_id.get(data_file_id)
        if df:
            _add(df.get("dataset_id"))

    if include_coloc_partners:
        for ds_id in _coloc_partners:
            entry = _dataset_registry.get(ds_id)
            if entry and entry.get("resource") == resource:
                _add(ds_id)

    return dataset_ids


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
        dataset_id = df["dataset_id"]
        if build_harmonizer_config(dataset_id):
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

    if dataset_id in _exome_dataset_ids:
        products["exome_results"] = True

    if dataset_id in _gene_based_dataset_ids:
        products["gene_based_results"] = True

    if dataset_id in _expression_dataset_ids:
        products["expression"] = True

    if dataset_id in _chromatin_peaks_dataset_ids:
        products["chromatin_peaks"] = True

    if dataset_id in _gene_disease_dataset_ids:
        products["gene_disease"] = True

    return products
