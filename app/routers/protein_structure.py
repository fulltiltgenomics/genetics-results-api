import asyncio
import logging
import time
from typing import AsyncGenerator, AsyncIterator, Literal

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

import app.config.protein_structure_contract as contract
from app.core.exceptions import GeneNotFoundException, ParseException
from app.core.responses import (
    DATASET_VERSION_HEADER,
    columns_header,
    range_response,
    verified_columns_header,
)
from app.dependencies import get_gene_name_mapping, get_protein_structure
from app.services.data_access_protein_structure import (
    FEATURES,
    MAX_STRUCTURE_TEXT_BYTES,
    RESIDUES,
    SCORES,
    ObjectMismatch,
    ProteinStructureData,
    ResponseTooLarge,
    Table,
    inflate_structure,
    json_body,
    json_build_slot,
    parse_positions,
    tsv_chunks,
)
from app.services.gene_name_and_position_mapping import GeneNameAndPositionMapping
from app.services.protein_structure import Model, Predictor, Release, ReleaseUnavailable

logger = logging.getLogger(__name__)

router = APIRouter()

# the bytes under a model file id change only with the release, and the ETag names the release.
# no-cache: a stored copy is revalidated on every use, which costs a 304 and no object read, so
# none outlives a release change. private: every route here is behind authentication
_OBJECT_CACHE_CONTROL = "private, no-cache"
_PDB_MEDIA_TYPE = "chemical/x-pdb"
PAE_SIZE_HEADER = "X-PAE-Size"
PAE_ROWS_HEADER = "X-PAE-Rows"
# what a score row's values mean, per predictor the response may hold: `percentile` ranks the
# raw score ascending, so without the direction a caller cannot tell which end is damaging
SCORE_PREDICTORS_HEADER = "X-Score-Predictors"
# the app's gzip middleware passes a response that states a content coding through untouched.
# A PAE body is megabytes of bytes it would otherwise deflate on the event loop in one call,
# and an identity model file must not be gzipped for a client that refused gzip
_UNCOMPRESSED = {"Content-Encoding": "identity"}

_PROTEIN = Path(
    ...,
    description="UniProt accession, gene name or ENSG ID",
    examples=["SETD1A"],
)
_MODEL_FILE_ID = Path(
    ...,
    description="Model file id, as listed by /protein_structure/models/{protein}",
    examples=["AF-O15047-F1-model_v6"],
)
_UNAVAILABLE = {503: {"description": "No release is loaded yet"}}


def _release(data_access: ProteinStructureData) -> Release:
    try:
        return data_access.release
    except ReleaseUnavailable as e:
        raise HTTPException(status_code=503, detail=str(e))


def _accepts_gzip(header: str) -> bool:
    """Whether an Accept-Encoding value takes gzip: named with a non-zero q, or covered by `*`."""
    weights: dict[str, float] = {}
    for item in header.split(","):
        coding, _, params = item.partition(";")
        q = 1.0
        for param in params.split(";"):
            name, _, value = param.partition("=")
            if name.strip().lower() == "q":
                try:
                    q = float(value)
                except ValueError:
                    q = 0.0
        weights[coding.strip().lower()] = q
    return weights.get("gzip", weights.get("*", 0.0)) > 0


def _genes(ensgs: set[str], mapping: GeneNameAndPositionMapping) -> list[dict]:
    """GENCODE records of the given genes; the tabix files are keyed on those coordinates."""
    # gene_positions is a defaultdict: a version that was never loaded yields an empty frame
    # with no `ensg` column, and the lookup below raises rather than finding nothing
    positions = mapping.gene_positions[contract.GENCODE_VERSION]
    return positions.filter(positions["ensg"].is_in(sorted(ensgs))).to_dicts()


def _resolve(
    protein: str, release: Release, mapping: GeneNameAndPositionMapping
) -> tuple[str | None, set[str]]:
    """(accession, its gene ids) for an accession in the release, else (None, the gene's ENSG ids)."""
    name = protein.strip()
    accession = name.upper()
    if accession in release.by_accession:
        return accession, {g for m in release.models_of(accession) for g in m.gene_ids}
    try:
        return None, mapping.get_ensg_ids_by_gene_name(name)
    except GeneNotFoundException:
        raise HTTPException(
            status_code=404,
            detail=f"{name} is neither a UniProt accession in release {release.name} nor a known gene",
        )


def _model(model_file_id: str, release: Release) -> Model:
    model = release.models.get(model_file_id)
    if model is None:
        raise HTTPException(
            status_code=404, detail=f"No model file {model_file_id!r} in release {release.name}"
        )
    return model


def _model_with_pae(model_file_id: str, release: Release) -> Model:
    model = _model(model_file_id, release)
    if not model.has_pae:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Model file {model.model_file_id} has no PAE: release {release.name} holds no "
                f"predicted aligned error for any model file of {model.accession}"
            ),
        )
    return model


def _object_mismatch(e: ObjectMismatch) -> HTTPException:
    logger.error(f"protein structure: {e}")
    return HTTPException(status_code=500, detail="The stored object does not match the release index")


def _etag(release: Release, model: Model, kind: str) -> str:
    return f'"{release.name}/{kind}/{model.model_file_id}"'


def _not_modified(request: Request, etag: str) -> bool:
    return etag in (t.strip() for t in request.headers.get("if-none-match", "").split(","))


async def _prepend(first: list[bytes], rest: AsyncIterator[list[bytes]]) -> AsyncGenerator[list[bytes], None]:
    yield first
    async for item in rest:
        yield item


def score_predictors_header(predictors: list[Predictor]) -> dict[str, str]:
    """`name;direction=...;licence=...` per predictor, comma-separated; the loader keeps separators out of each part."""
    return {
        SCORE_PREDICTORS_HEADER: ",".join(
            f"{p.name};direction={p.direction};licence={p.licence}" for p in predictors
        )
    }


def _predictors(release: Release, asked: list[str] | None) -> list[Predictor]:
    """The served predictors a scores request is about: all of them, or those it names."""
    if release.predictors is None:
        raise HTTPException(status_code=404, detail=f"Release {release.name} holds no predictor scores")
    if not release.predictors:
        raise HTTPException(
            status_code=404, detail=f"This deployment serves no predictor scores of release {release.name}"
        )
    if asked is None:
        return list(release.predictors.values())
    names = {n.strip() for item in asked for n in item.split(",")}
    unknown = sorted(n for n in names if n not in release.predictors)
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=(
                f"No predictor {', '.join(repr(n[:40]) for n in unknown[:5])} in release {release.name}; "
                f"it has {', '.join(release.predictors)}"
            ),
        )
    return [p for name, p in release.predictors.items() if name in names]


async def _rows_response(
    request: Request,
    protein: str,
    table: Table,
    kind: str,
    format: str,
    data_access: ProteinStructureData,
    mapping: GeneNameAndPositionMapping,
    release: Release | None = None,
    predictors: list[Predictor] | None = None,
) -> Response:
    start_time = time.time()
    if release is None:
        release = _release(data_access)
    narrowed = frozenset(p.name for p in predictors) if predictors is not None else None
    extra_headers = score_predictors_header(predictors) if predictors is not None else {}
    accession, ensgs = _resolve(protein, release, mapping)
    if accession is not None and not ensgs:
        raise HTTPException(
            status_code=404,
            detail=(
                f"{accession} is listed under no gene in release {release.name}, so its {kind} "
                "rows, if it has any, are reachable by gene only"
            ),
        )
    genes = _genes(ensgs, mapping)
    if not genes:
        raise HTTPException(
            status_code=404,
            detail=f"{protein.strip()} is not in GENCODE v{contract.GENCODE_VERSION}",
        )
    async def rows() -> AsyncIterator[list[bytes]]:
        try:
            found = data_access.stream_rows(release, table, genes, accession, narrowed)
            # read ahead by one row: a protein without rows is a 404, which a started stream cannot say
            first = await anext(found, None)
        except Exception as e:
            logger.exception(e)
            raise HTTPException(status_code=500, detail="Internal server error")
        if first is None:
            raise HTTPException(
                status_code=404, detail=f"No {kind} rows for {protein.strip()} in release {release.name}"
            )
        return _prepend(first, found)

    if format == "tsv":
        response = await range_response(
            str(request.url), tsv_chunks(table, await rows()), table.header_schema, "tsv", start_time
        )
    else:
        # the slot is taken before the first read, so a waiting request holds no rows
        async with json_build_slot():
            found = await rows()
            try:
                body = await json_body(table, found, contract.MAX_RESPONSE_BYTES)
            except ResponseTooLarge:
                raise HTTPException(
                    status_code=413,
                    detail=(
                        f"The {kind} rows of {protein.strip()} are over {contract.MAX_RESPONSE_BYTES} "
                        "bytes as JSON; request format=tsv"
                        + (", or one predictor at a time with predictor=" if len(predictors or ()) > 1 else "")
                    ),
                )
            except Exception as e:
                logger.exception(e)
                raise HTTPException(status_code=500, detail="Error streaming or parsing data")
        response = Response(body, media_type="application/json", headers=columns_header(table.names))
    response.headers[DATASET_VERSION_HEADER] = release.name
    response.headers.update(extra_headers)
    return response


@router.get(
    "/protein_structure/models/{protein}",
    summary="List the model files of a protein",
    responses={
        200: {"description": "One object per model file, in window order within each accession"},
        404: {"description": "Not an accession or gene with a model in the release"},
        **_UNAVAILABLE,
    },
)
async def models(
    protein: str = _PROTEIN,
    data_access: ProteinStructureData = Depends(get_protein_structure),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
) -> Response:
    """The model files a structure is addressed by: a protein longer than one model is cut into overlapping windows."""
    release = _release(data_access)
    accession, ensgs = _resolve(protein, release, gene_name_and_position_mapping)
    accessions = (
        [accession]
        if accession is not None
        else sorted({a for g in ensgs for a in release.by_gene.get(g, ())})
    )
    if not accessions:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No model file is listed under gene {protein.strip()} in release {release.name}; "
                "a protein the release lists under no gene is reachable by its UniProt accession"
            ),
        )
    rows = [m._asdict() for a in accessions for m in release.models_of(a)]
    return JSONResponse(
        rows,
        headers={**verified_columns_header(Model._fields, rows), DATASET_VERSION_HEADER: release.name},
    )


_ROWS_RESPONSES = {
    404: {"description": "Not an accession or gene of the release, or it has no such rows"},
    413: {"description": "format=json and the body would pass the per-response byte limit; request format=tsv"},
    **_UNAVAILABLE,
    500: {"description": "Internal server error"},
}


@router.get(
    "/protein_structure/residues/{protein}",
    summary="Get per-residue rows of a protein's model files",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "\t".join(RESIDUES.names) + "\n...",
                }
            },
        },
        **_ROWS_RESPONSES,
    },
)
async def residues(
    request: Request,
    protein: str = _PROTEIN,
    format: Literal["tsv", "json"] = Query(default="tsv", description="Response format"),
    data_access: ProteinStructureData = Depends(get_protein_structure),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
) -> Response:
    """Every residue of every model file: CA coordinates, pLDDT and the per-residue mutation rate.

    A residue is addressed by (model_file_id, file_position). A protein position covered by
    several windows has one row per window; `is_owner` marks the one a per-residue consumer uses.
    """
    return await _rows_response(
        request, protein, RESIDUES, "residue", format, data_access, gene_name_and_position_mapping
    )


@router.get(
    "/protein_structure/features/{protein}",
    summary="Get sequence features of a protein",
    responses={
        200: {
            "description": "Successful response",
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "\t".join(FEATURES.names) + "\n...",
                }
            },
        },
        **_ROWS_RESPONSES,
    },
)
async def features(
    request: Request,
    protein: str = _PROTEIN,
    format: Literal["tsv", "json"] = Query(default="tsv", description="Response format"),
    data_access: ProteinStructureData = Depends(get_protein_structure),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
) -> Response:
    """UniProt features and TED domains; `start` and `end` are protein positions, 1-based inclusive."""
    return await _rows_response(
        request, protein, FEATURES, "feature", format, data_access, gene_name_and_position_mapping
    )


@router.get(
    "/protein_structure/scores/{protein}",
    summary="Get per-residue missense predictor scores of a protein",
    responses={
        200: {
            "description": (
                f"Successful response; {SCORE_PREDICTORS_HEADER} gives each predictor's direction and licence"
            ),
            "content": {
                "text/tab-separated-values": {
                    "schema": {"type": "string"},
                    "example": "\t".join(SCORES.names) + "\n...",
                }
            },
        },
        **_ROWS_RESPONSES,
        404: {
            "description": (
                "Not an accession or gene of the release, it has no such rows, or the release "
                "holds no predictor scores"
            )
        },
        422: {"description": "A predictor the release does not hold or this deployment does not serve"},
    },
)
async def scores(
    request: Request,
    protein: str = _PROTEIN,
    format: Literal["tsv", "json"] = Query(default="tsv", description="Response format"),
    predictor: list[str] | None = Query(
        default=None,
        description=(
            "Only these predictors: one name, several comma-separated, or the parameter repeated. "
            "Default: every predictor served."
        ),
        examples=["alphamissense"],
    ),
    data_access: ProteinStructureData = Depends(get_protein_structure),
    gene_name_and_position_mapping: GeneNameAndPositionMapping = Depends(get_gene_name_mapping),
) -> Response:
    """One row per (accession, predictor, protein position): the mean raw score and its percentile.

    `percentile` ranks the predictor's raw `mean_snv` ascending; it is not turned so that high
    means damaging. Which end is the damaging one differs by predictor and is stated, with the
    predictor's licence, in the X-Score-Predictors response header. A residue without a row
    has no score from that predictor, which says nothing about its tolerance. Rows are per
    protein position, not per model window: join to the residue rows with `is_owner`.
    """
    release = _release(data_access)
    return await _rows_response(
        request, protein, SCORES, "score", format, data_access, gene_name_and_position_mapping,
        release, _predictors(release, predictor),
    )


@router.get(
    "/protein_structure/structure/{model_file_id}",
    summary="Get a model file",
    responses={
        200: {"description": "The model in PDB format", "content": {_PDB_MEDIA_TYPE: {"schema": {"type": "string"}}}},
        304: {"description": "The copy named by If-None-Match is current"},
        404: {"description": "No such model file in the release"},
        **_UNAVAILABLE,
        500: {"description": "The stored object is missing or not the size the release gives it"},
    },
)
async def structure(
    request: Request,
    model_file_id: str = _MODEL_FILE_ID,
    data_access: ProteinStructureData = Depends(get_protein_structure),
) -> Response:
    """The model file as the source published it; its residue numbers are the file positions."""
    release = _release(data_access)
    model = _model(model_file_id, release)
    gzipped = _accepts_gzip(request.headers.get("accept-encoding", ""))
    etag = _etag(release, model, "structure" if gzipped else "structure-identity")
    headers = {
        "ETag": etag,
        "Cache-Control": _OBJECT_CACHE_CONTROL,
        "Vary": "Accept-Encoding",
        DATASET_VERSION_HEADER: release.name,
    }
    if _not_modified(request, etag):
        return Response(status_code=304, headers=headers)
    try:
        body = await data_access.structure(release, model)
        if gzipped:
            # the stored bytes are the gzip stream: sent as they are, the client inflates them
            headers["Content-Encoding"] = "gzip"
        else:
            body = await asyncio.to_thread(inflate_structure, body, MAX_STRUCTURE_TEXT_BYTES)
            headers.update(_UNCOMPRESSED)
    except ObjectMismatch as e:
        raise _object_mismatch(e)
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal server error")
    return Response(body, media_type=_PDB_MEDIA_TYPE, headers=headers)


_PAE_RESPONSES = {
    404: {"description": "No such model file in the release, or the model has no PAE"},
    **_UNAVAILABLE,
    500: {"description": "The stored object is missing or not the size the release gives it"},
}


@router.get(
    "/protein_structure/pae/{model_file_id}",
    summary="Get the predicted aligned error matrix of a model file",
    responses={
        200: {
            "description": "n x n unsigned bytes, row-major, n in X-PAE-Size",
            "content": {"application/octet-stream": {"schema": {"type": "string", "format": "binary"}}},
        },
        304: {"description": "The copy named by If-None-Match is current"},
        **_PAE_RESPONSES,
    },
)
async def pae(
    request: Request,
    model_file_id: str = _MODEL_FILE_ID,
    data_access: ProteinStructureData = Depends(get_protein_structure),
) -> Response:
    """The whole matrix: entry (r, c) is the byte at r*n + c for file positions r+1, c+1; PAE = byte / 8.

    The largest models' matrices are over the SDK's per-response limit; a caller bound by it
    asks for /slice instead.
    """
    release = _release(data_access)
    model = _model_with_pae(model_file_id, release)
    etag = _etag(release, model, "pae")
    headers = {
        "ETag": etag,
        "Cache-Control": _OBJECT_CACHE_CONTROL,
        PAE_SIZE_HEADER: str(model.n_residues),
        DATASET_VERSION_HEADER: release.name,
        **_UNCOMPRESSED,
    }
    if _not_modified(request, etag):
        return Response(status_code=304, headers=headers)
    try:
        body = await data_access.open_pae(release, model)
    except ObjectMismatch as e:
        raise _object_mismatch(e)
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal server error")
    headers["Content-Length"] = str(model.pae_bytes)
    return StreamingResponse(body, media_type="application/octet-stream", headers=headers)


@router.get(
    "/protein_structure/pae/{model_file_id}/slice",
    summary="Get the predicted aligned error between chosen residues of a model file",
    responses={
        200: {
            "description": "k x k unsigned bytes, row-major, k in X-PAE-Size",
            "content": {"application/octet-stream": {"schema": {"type": "string", "format": "binary"}}},
        },
        422: {"description": "Invalid positions, or more of them than one request may ask for"},
        **_PAE_RESPONSES,
    },
)
async def pae_slice(
    model_file_id: str = _MODEL_FILE_ID,
    positions: str = Query(
        ...,
        description=(
            "File positions, comma-separated, each N or an inclusive range N-M. The response's "
            "rows and columns are the distinct positions in ascending order, whatever order "
            f"they are given in. At most floor({contract.MAX_RESPONSE_BYTES} / n) positions for a "
            "model of n residues."
        ),
        examples=["100-140,812,815"],
    ),
    data_access: ProteinStructureData = Depends(get_protein_structure),
) -> Response:
    """The matrix restricted to the given file positions, on both axes; PAE = byte / 8."""
    release = _release(data_access)
    model = _model_with_pae(model_file_id, release)
    try:
        wanted = parse_positions(positions, model.n_residues)
    except ParseException as e:
        raise HTTPException(status_code=422, detail=str(e))
    try:
        body = await data_access.pae_slice(release, model, wanted)
    except ObjectMismatch as e:
        raise _object_mismatch(e)
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal server error")
    return Response(
        body,
        media_type="application/octet-stream",
        headers={PAE_SIZE_HEADER: str(len(wanted)), DATASET_VERSION_HEADER: release.name, **_UNCOMPRESSED},
    )


@router.get(
    "/protein_structure/pae/{model_file_id}/rows",
    summary="Get whole rows of the predicted aligned error matrix of a model file",
    responses={
        200: {
            "description": "k x n unsigned bytes, row-major: n in X-PAE-Size, k in X-PAE-Rows",
            "content": {"application/octet-stream": {"schema": {"type": "string", "format": "binary"}}},
        },
        422: {"description": "Invalid positions, or more of them than one request may ask for"},
        **_PAE_RESPONSES,
    },
)
async def pae_rows(
    model_file_id: str = _MODEL_FILE_ID,
    positions: str = Query(
        ...,
        description=(
            "File positions, comma-separated, each N or an inclusive range N-M. The response holds "
            "the complete row of each distinct position, in ascending order whatever order they "
            f"are given in. At most floor({contract.MAX_RESPONSE_BYTES} / n) positions for a "
            "model of n residues."
        ),
        examples=["100-140,812,815"],
    ),
    data_access: ProteinStructureData = Depends(get_protein_structure),
) -> Response:
    """Each given file position against every residue of the model; PAE = byte / 8."""
    release = _release(data_access)
    model = _model_with_pae(model_file_id, release)
    try:
        wanted = parse_positions(positions, model.n_residues)
    except ParseException as e:
        raise HTTPException(status_code=422, detail=str(e))
    try:
        body = await data_access.pae_rows(release, model, wanted)
    except ObjectMismatch as e:
        raise _object_mismatch(e)
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal server error")
    return Response(
        body,
        media_type="application/octet-stream",
        headers={
            PAE_SIZE_HEADER: str(model.n_residues),
            PAE_ROWS_HEADER: str(len(wanted)),
            DATASET_VERSION_HEADER: release.name,
            **_UNCOMPRESSED,
        },
    )
