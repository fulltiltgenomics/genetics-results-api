import time
import logging
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from app.dependencies import get_data_access_chromatin_peaks, get_gene_name_mapping
from app.core.responses import range_response
from app.core.exceptions import GeneNotFoundException, NotFoundException
from app.services.data_access_chromatin_peaks import DataAccessChromatinPeaks
from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping
import app.config.chromatin_peaks as config
import app.config.common as config_common

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/peak_to_genes/{peak_id}",
    summary="Get genes associated with a chromatin peak",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "resource\tversion\tchrom\tstart\tend\tpeak_id\tgene_id\tsymbol\tcell_type\ttotal_cell_num\texpr_cell_num\topen_cell_num\thurdle_zero_beta\thurdle_zero_se\thurdle_zero_z\thurdle_zero_nlog10p\thurdle_count_beta\thurdle_count_se\thurdle_count_z\thurdle_count_nlog10p\thurdle_aic\thurdle_bic\tgene_chrom\tgene_start\tgene_end\nfinngen_atacseq\t\tchr1\t817095\t817594\tchr1-817095-817594\tENSG00000228794\tLINC01128\tpredicted.celltype.l1.PBMC\t7088529\t187191\t118742\t0.122891591499534\t0.0139613929348723\t8.80224430848729\t17.8725490512647\t0.287993450475682\t0.0381107939160484\t7.55674235257555\t13.3837456571306\t1645822.3526141\t1645973.86648652\tchr1\t826206\t827522\n...",
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "resource": {"type": "string"},
                                "version": {"type": "string"},
                                "chrom": {"type": "string"},
                                "start": {"type": "integer"},
                                "end": {"type": "integer"},
                                "peak_id": {"type": "string"},
                                "gene_id": {"type": "string"},
                                "symbol": {"type": "string"},
                                "cell_type": {"type": "string"},
                                "total_cell_num": {"type": "integer"},
                                "expr_cell_num": {"type": "integer"},
                                "open_cell_num": {"type": "integer"},
                                "hurdle_zero_beta": {"type": "number"},
                                "hurdle_zero_se": {"type": "number"},
                                "hurdle_zero_z": {"type": "number"},
                                "hurdle_zero_nlog10p": {"type": "number"},
                                "hurdle_count_beta": {"type": "number"},
                                "hurdle_count_se": {"type": "number"},
                                "hurdle_count_z": {"type": "number"},
                                "hurdle_count_nlog10p": {"type": "number"},
                                "hurdle_aic": {"type": "number"},
                                "hurdle_bic": {"type": "number"},
                                "gene_chrom": {"type": "string"},
                                "gene_start": {"type": "integer"},
                                "gene_end": {"type": "integer"},
                            },
                        },
                    },
                    "example": [
                        {
                            "resource": "finngen_atacseq",
                            "version": "",
                            "chrom": "chr1",
                            "start": 817095,
                            "end": 817594,
                            "peak_id": "chr1-817095-817594",
                            "gene_id": "ENSG00000228794",
                            "symbol": "LINC01128",
                            "cell_type": "predicted.celltype.l1.PBMC",
                            "total_cell_num": 7088529,
                            "expr_cell_num": 187191,
                            "open_cell_num": 118742,
                            "hurdle_zero_beta": 0.122891591499534,
                            "hurdle_zero_se": 0.0139613929348723,
                            "hurdle_zero_z": 8.80224430848729,
                            "hurdle_zero_nlog10p": 17.8725490512647,
                            "hurdle_count_beta": 0.287993450475682,
                            "hurdle_count_se": 0.0381107939160484,
                            "hurdle_count_z": 7.55674235257555,
                            "hurdle_count_nlog10p": 13.3837456571306,
                            "hurdle_aic": 1645822.3526141,
                            "hurdle_bic": 1645973.86648652,
                            "gene_chrom": "chr1",
                            "gene_start": 826206,
                            "gene_end": 827522,
                        },
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Resources not found"},
        422: {"description": "Invalid peak_id or format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def peak_to_genes(
    request: Request,
    peak_id: str = Path(
        ...,
        description="Peak ID in format chr-start-end",
        examples=["chr1-817095-817594"],
    ),
    resources: list[str] | None = Query(
        default=None,
        description="Comma-separated list of resources to get data from (if not given, all available resources are used)",
    ),
    gencode_version: int | None = Query(
        default=None,
        description="GENCODE version to use for gene coordinates (default: latest available)",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    data_access_chromatin_peaks: DataAccessChromatinPeaks = Depends(get_data_access_chromatin_peaks),
    gene_name_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
) -> Response:
    """
    Get genes associated with a chromatin peak.

    The peak_id should be in format: chr1-817095-817594
    """
    start_time = time.time()

    try:
        from app.services.data_access_chromatin_peaks import (
            DataAccessObjectChromatinPeaks,
        )

        DataAccessObjectChromatinPeaks.parse_peak_id(peak_id)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    if resources is None:
        resources = [c["resource"] for c in config.chromatin_peaks_data]

    available_resources = [c["resource"] for c in config.chromatin_peaks_data]
    invalid_resources = [r for r in resources if r not in available_resources]
    if invalid_resources:
        raise HTTPException(
            status_code=404,
            detail=f"Unrecognized resource(s): {', '.join(invalid_resources)}. Available resources: "
            + ", ".join(available_resources),
        )

    coordinates_lookup = None
    try:
        coordinates_lookup = gene_name_mapping.get_coordinates_lookup(gencode_version)
    except Exception as e:
        logger.warning(f"Could not load gene coordinates: {e}")

    try:
        stream = await data_access_chromatin_peaks.stream_by_peak_id(
            peak_id,
            resources,
            config_common.read_chunk_size,
            config_common.response_chunk_size,
            coordinates_lookup=coordinates_lookup,
        )
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal server error")

    return await range_response(
        str(request.url),
        stream,
        config.chromatin_peaks_header_schema,
        format,
        start_time,
    )


@router.get(
    "/gene_to_peaks/{gene}",
    summary="Get chromatin peaks linked to a gene",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "resource\tversion\tchrom\tstart\tend\tpeak_id\tgene_id\tsymbol\tcell_type\ttotal_cell_num\texpr_cell_num\topen_cell_num\thurdle_zero_beta\thurdle_zero_se\thurdle_zero_z\thurdle_zero_nlog10p\thurdle_count_beta\thurdle_count_se\thurdle_count_z\thurdle_count_nlog10p\thurdle_aic\thurdle_bic\tgene_chrom\tgene_start\tgene_end\nfinngen\tR12\t5\t35853087\t35853586\tchr5-35853087-35853586\tENSG00000168685\tIL7R\tl1.CD4_T\t7088529\t187191\t118742\t0.122891591499534\t0.0139613929348723\t8.80224430848729\t17.8725490512647\t0.287993450475682\t0.0381107939160484\t7.55674235257555\t13.3837456571306\t1645822.3526141\t1645973.86648652\tchr5\t35852695\t35879603\n...",
                },
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {"type": "object"},
                    },
                    "example": [
                        {
                            "resource": "finngen",
                            "version": "R12",
                            "chrom": "5",
                            "start": 35853087,
                            "end": 35853586,
                            "peak_id": "chr5-35853087-35853586",
                            "gene_id": "ENSG00000168685",
                            "symbol": "IL7R",
                            "cell_type": "l1.CD4_T",
                            "gene_chrom": "chr5",
                            "gene_start": 35852695,
                            "gene_end": 35879603,
                        },
                    ],
                },
            },
        },
        401: {"description": "Not authenticated"},
        404: {"description": "Gene or resources not found"},
        422: {"description": "Invalid format parameter"},
        500: {"description": "Internal server error"},
    },
)
async def gene_to_peaks(
    request: Request,
    gene: str = Path(
        ...,
        description="Gene name or ENSG ID",
        examples=["IL7R"],
    ),
    resources: list[str] | None = Query(
        default=None,
        description="Comma-separated list of resources to get data from (if not given, all available resources are used)",
    ),
    gencode_version: int | None = Query(
        default=None,
        description="GENCODE version to use for gene coordinates (default: latest available)",
    ),
    format: Literal["tsv", "json"] = Query(
        default="tsv", description="Response format"
    ),
    data_access_chromatin_peaks: DataAccessChromatinPeaks = Depends(get_data_access_chromatin_peaks),
    gene_name_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
) -> Response:
    """
    Get chromatin peaks linked to a gene, i.e. the inverse of peak_to_genes.

    Returns the same columns as peak_to_genes: one row per peak, linked gene and cell type.
    """
    start_time = time.time()

    if resources is None:
        resources = [c["resource"] for c in config.chromatin_peaks_data]

    available_resources = [c["resource"] for c in config.chromatin_peaks_data]
    invalid_resources = [r for r in resources if r not in available_resources]
    if invalid_resources:
        raise HTTPException(
            status_code=404,
            detail=f"Unrecognized resource(s): {', '.join(invalid_resources)}. Available resources: "
            + ", ".join(available_resources),
        )

    try:
        coords_by_version = gene_name_mapping.get_coordinates_by_gene_name(gene)
        gene_ids = gene_name_mapping.get_ensg_ids_by_gene_name(gene)
    except GeneNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))

    # the file is indexed on the gene locus of ONE GENCODE version, so query every version
    # the gene is known in and let the ENSG filter drop the rows of neighbouring genes
    loci = sorted(
        {
            (int(c["chrom"]), int(c["gene_start"]), int(c["gene_end"]))
            for coords in (coords_by_version or {}).values()
            for c in coords
        }
    )
    if not loci:
        raise HTTPException(
            status_code=404, detail=f"No coordinates found for gene {gene}"
        )

    coordinates_lookup = None
    try:
        coordinates_lookup = gene_name_mapping.get_coordinates_lookup(gencode_version)
    except Exception as e:
        logger.warning(f"Could not load gene coordinates: {e}")

    try:
        stream = await data_access_chromatin_peaks.stream_by_gene(
            gene_ids,
            loci,
            resources,
            config_common.read_chunk_size,
            config_common.response_chunk_size,
            coordinates_lookup=coordinates_lookup,
        )
    except NotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal server error")

    return await range_response(
        str(request.url),
        stream,
        config.chromatin_peaks_header_schema,
        format,
        start_time,
    )
