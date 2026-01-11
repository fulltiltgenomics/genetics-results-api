"""
Router for listing available data resources.
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import app.config.credible_sets as credible_sets_config
import app.config.coloc as coloc_config
import app.config.expression as expression_config
import app.config.chromatin_peaks as chromatin_peaks_config
import app.config.exome_results as exome_results_config
import app.config.gene_based_results as gene_based_config
import app.config.gene_disease as gene_disease_config

router = APIRouter()


def _extract_credible_set_info(data_file: dict) -> dict:
    """Extract relevant info from a credible set config entry."""
    info = {
        "id": data_file["id"],
        "resource": data_file.get("resource", data_file["id"]),
    }
    if "gencode_version" in data_file:
        info["gencode_version"] = data_file["gencode_version"]
    if "metadata" in data_file:
        meta = data_file["metadata"]
        info["metadata"] = {
            k: v for k, v in meta.items() if k != "metadata_file" and k != "type"
        }
    return info


def _extract_exome_info(data_file: dict) -> dict:
    """Extract relevant info from an exome results config entry."""
    info = {
        "id": data_file["id"],
        "resource": data_file.get("resource", data_file["id"]),
    }
    if "gencode_version" in data_file:
        info["gencode_version"] = data_file["gencode_version"]
    if "exome" in data_file and "version" in data_file["exome"]:
        info["version"] = data_file["exome"]["version"]
    if "metadata" in data_file:
        meta = data_file["metadata"]
        info["metadata"] = {
            k: v for k, v in meta.items() if k != "metadata_file" and k != "type"
        }
    return info


def _extract_gene_based_info(data_file: dict) -> dict:
    """Extract relevant info from a gene-based config entry."""
    info = {
        "id": data_file["id"],
        "resource": data_file.get("resource", data_file["id"]),
    }
    if "gencode_version" in data_file:
        info["gencode_version"] = data_file["gencode_version"]
    if "metadata" in data_file:
        meta = data_file["metadata"]
        info["metadata"] = {
            k: v for k, v in meta.items() if k != "metadata_file" and k != "type"
        }
    return info


@router.get(
    "/resources",
    summary="List all available data resources",
    responses={
        200: {
            "description": "Available data resources",
            "content": {
                "application/json": {
                    "example": {
                        "credible_sets": [
                            {
                                "id": "finngen_gwas",
                                "resource": "finngen",
                                "gencode_version": 49,
                                "metadata": {
                                    "author": "FinnGen Consortium",
                                    "publication_date": "2025-09-01",
                                    "version_label": "R13",
                                },
                            }
                        ],
                        "colocalization": [{"name": "FinnGen_R13_vs_many"}],
                        "expression": [{"resource": "gtex", "gencode_version": 39}],
                        "chromatin_peaks": [{"resource": "finngen", "version": "R12"}],
                        "exome_results": [{"id": "genebass", "resource": "genebass"}],
                        "gene_based": [
                            {"id": "genebass_gene_based", "resource": "genebass"}
                        ],
                        "gene_disease": ["gencc", "monarch"],
                    }
                }
            },
        },
        401: {"description": "Not authenticated"},
    },
)
async def list_resources():
    """
    Return information about all available data resources.
    """
    resources = {
        "credible_sets": [
            _extract_credible_set_info(df) for df in credible_sets_config.data_files
        ],
        "colocalization": [{"name": c["name"]} for c in coloc_config.coloc],
        "expression": [
            {
                "resource": e["resource"],
                "gencode_version": e.get("gencode_version"),
            }
            for e in expression_config.expression_data
        ],
        "chromatin_peaks": [
            {
                "resource": c["resource"],
                "version": c.get("version"),
            }
            for c in chromatin_peaks_config.chromatin_peaks_data
        ],
        "exome_results": [
            _extract_exome_info(df) for df in exome_results_config.exome_data_files
        ],
        "gene_based": [
            _extract_gene_based_info(df)
            for df in gene_based_config.gene_based_data_files
        ],
        "gene_disease": [
            k for k in gene_disease_config.gene_disease.keys() if k != "output_columns"
        ],
    }

    return JSONResponse(content=resources)
