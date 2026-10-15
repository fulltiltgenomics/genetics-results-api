import logging
import time
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response

import app.config.cell_type_expression as config
from app.core.exceptions import GeneNotFoundException
from app.core.responses import DATASET_VERSION_HEADER, range_response
from app.dependencies import get_data_access_cell_type_expression, get_gene_name_mapping
from app.services.data_access_cell_type_expression import (
    CellTypeExpressionAccess,
    ReleaseUnavailable,
)
from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping

logger = logging.getLogger(__name__)

router = APIRouter()

_SUMMARY_EXAMPLE = (
    "gene_id\tchrom\tgene_start\tgene_end\tgene_name\tsource_gene_ids\tcell_type_rollup_ontology_term_id"
    "\tcell_type_rollup\ttissue_general_ontology_term_id\ttissue_general\tn_cells\tn_donors\tn_datasets"
    "\tmean_log1p_cp10k\tfrac_expressing\tdonor_pseudobulk_mean_cp10k\tdonor_pseudobulk_n_donors\ttau\n"
    "ENSG00000227232.6\t1\t14696\t29570\tWASH7P\tENSG00000227232\tCL:0000057\tfibroblast"
    "\tUBERON:0002048\tlung\t2234\t3\t1\t0.0074\t0.0113\t0.0\t3\tNA\n..."
)
_DATASET_EXAMPLE = (
    "gene_id\tchrom\tgene_start\tgene_end\tgene_name\tsource_gene_id\tdataset_id\t...\tn_cells\tn_donors"
    "\tmean_log1p_cp10k\tfrac_expressing\tdonor_pseudobulk_mean_cp10k\tstatus\n"
    "ENSG00000227232.6\t1\t14696\t29570\tWASH7P\tENSG00000227232\t37a17b78-...\t...\t27258\t58"
    "\t0.0074\t0.0113\t0.0\tpresent\n"
    "ENSG00000227232.6\t1\t14696\t29570\tWASH7P\tNA\t31f657dc-...\tNA\t...\tNA\tNA"
    "\t0\t0\tNA\tmeasured_no_row\n..."
)


def _resolve_genes(gene: str, mapping: GeneNameAndPositionMapping) -> list[dict]:
    """The requested genes as GENCODE v49 records; the files are keyed on those coordinates."""
    found: dict[str, dict] = {}
    for name in (g.strip() for g in gene.split(",") if g.strip()):
        try:
            ensgs = mapping.get_ensg_ids_by_gene_name(name)
        except GeneNotFoundException as e:
            raise HTTPException(status_code=404, detail=str(e))
        positions = mapping.gene_positions[config.GENCODE_VERSION]
        rows = positions.filter(positions["ensg"].is_in(sorted(ensgs))).to_dicts()
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"Gene {name} is not in GENCODE v{config.GENCODE_VERSION}",
            )
        for r in rows:
            found[r["ensg"]] = r
    if not found:
        raise HTTPException(status_code=422, detail="No valid gene names provided")
    return list(found.values())


@router.get(
    "/cell_type_expression/{gene}",
    summary="Get single-cell expression by cell type for a gene",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": _SUMMARY_EXAMPLE,
                },
            },
        },
        404: {"description": "Gene not found, or no cell-type expression data in this deployment"},
        422: {"description": "Invalid level or format parameter"},
        503: {"description": "No release is loaded yet"},
        500: {"description": "Internal server error"},
    },
)
async def cell_type_expression(
    request: Request,
    gene: str = Path(
        ..., description="Gene name, ENSG ID, or comma-separated list of them", examples=["PCSK9"]
    ),
    level: Literal["summary", "dataset"] = Query(
        default="summary",
        description=(
            "summary: normal tissue, pooled across datasets per (cell type, tissue), leaving out "
            "broad lineage labels such as 'leukocyte' or 'epithelial cell'. "
            "dataset: every group of every dataset that passes the suppression thresholds. "
            "A dataset that has at least one such group and measured the gene but has no row "
            "for it gets one placeholder row (status=measured_no_row): the gene has no "
            "expressing cell in any of the dataset's groups, so mean_log1p_cp10k and "
            "frac_expressing are 0 and donor_pseudobulk_mean_cp10k and the group columns are "
            "NA. Datasets that did not measure the gene, or whose every group is "
            "suppressed, are absent."
        ),
    ),
    format: Literal["tsv", "json"] = Query(default="tsv", description="Response format"),
    data_access: CellTypeExpressionAccess = Depends(get_data_access_cell_type_expression),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
) -> Response:
    """Mean log1p CP10K and the fraction of expressing cells per cell type, from CELLxGENE."""
    start_time = time.time()
    if not data_access.configured:
        raise HTTPException(
            status_code=404, detail="Cell-type expression data is not configured for this deployment"
        )
    genes = _resolve_genes(gene, gene_name_and_position_mapping)
    try:
        release = data_access.release
    except ReleaseUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e))

    try:
        if level == "summary":
            stream = await data_access.stream_summary(release, genes)
            schema = config.summary_header_schema
        else:
            stream = await data_access.stream_per_dataset(release, genes)
            schema = config.per_dataset_header_schema
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal server error")

    response = await range_response(str(request.url), stream, schema, format, start_time)
    response.headers[DATASET_VERSION_HEADER] = release.name
    return response
