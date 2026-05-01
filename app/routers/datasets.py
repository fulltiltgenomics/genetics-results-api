"""
Router for listing datasets with descriptions, provenance, available products,
and aggregate sample-size statistics.
"""

import logging

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse

from app.dependencies import get_data_access
from app.services.data_access import DataAccess
from app.services import config_util, dataset_stats

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/datasets",
    summary="List all datasets with descriptions, products, and sample sizes",
    responses={
        200: {
            "description": "List of datasets",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "dataset_id": "finngen_gwas",
                            "resource": "finngen",
                            "version": "R13",
                            "description": "FinnGen R13 core GWAS ...",
                            "author": "FinnGen Consortium",
                            "publication_date": "2025-09-01",
                            "trait_type": "binary",
                            "products": {
                                "credible_sets": True,
                                "summary_stats": True,
                                "colocalization": {
                                    "partners": ["finngen_drugs", "finngen_gwas", "finngen_kanta"],
                                },
                            },
                            "stats": {
                                "n_phenotypes": 2408,
                                "n_samples_median": 456912,
                                "n_cases_range": [10, 82341],
                            },
                            "metadata_endpoint": "/api/v1/resource_metadata/finngen",
                        }
                    ]
                }
            },
        },
        401: {"description": "Not authenticated"},
    },
)
async def list_datasets(
    resource: str | None = Query(
        default=None,
        description="Optional: filter to datasets for a specific resource",
        example="finngen",
    ),
    data_type: str | None = Query(
        default=None,
        description="Optional: filter to datasets of a specific data type (e.g. gwas, expression, gene_disease)",
        example="gwas",
    ),
    include_stats: bool = Query(
        default=True,
        description="Whether to include aggregate sample-size stats (may trigger metadata file loads)",
    ),
    data_access: DataAccess = Depends(get_data_access),
):
    """Return the dataset catalog from the registry, enriched with the set of
    products each dataset supports (credible sets, summary stats, coloc
    comparisons) and aggregate sample-size statistics."""
    registry = config_util.get_datasets()

    results = []
    for dataset_id, entry in registry.items():
        if resource and entry.get("resource") != resource:
            continue
        if data_type and entry.get("data_type") != data_type:
            continue

        products = config_util.dataset_products(dataset_id)

        item = {
            "dataset_id": dataset_id,
            "resource": entry.get("resource"),
            "version": entry.get("version"),
            "description": entry.get("description"),
            "author": entry.get("author"),
            "publication_date": entry.get("publication_date"),
            "trait_type": entry.get("trait_type"),
            "data_type": entry.get("data_type"),
            "products": products,
        }
        # derive qtl_types from data_type when not explicitly set
        qtl_type_map = {
            "eqtl": ["eQTL"],
            "pqtl": ["pQTL"],
            "caqtl": ["caQTL"],
            "sqtl": ["sQTL"],
            "metaboqtl": ["metaboQTL"],
        }
        qtl_types = entry.get("qtl_types") or qtl_type_map.get(entry.get("data_type", ""))
        if qtl_types:
            item["qtl_types"] = qtl_types
        if entry.get("n_samples") is not None:
            item["n_samples"] = entry["n_samples"]
        if entry.get("n_phenotypes") is not None:
            item["n_phenotypes"] = entry["n_phenotypes"]
        if entry.get("pseudo_credible_sets"):
            item["pseudo_credible_sets"] = True
        if entry.get("collection"):
            item["collection"] = True
            item["subdataset_id_field"] = entry.get("subdataset_id_field")

        if include_stats and entry.get("metadata_file"):
            stats = dataset_stats.get_dataset_stats(dataset_id, data_access)
            if stats:
                item["stats"] = stats
            item["metadata_endpoint"] = f"/api/v1/resource_metadata/{entry.get('resource')}"

        results.append(item)

    return JSONResponse(content=results)
