from io import StringIO
import sqlite3
import timeit
from fastapi import Body, FastAPI, Request, Response, Depends, HTTPException
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, RedirectResponse
import subprocess
from collections import defaultdict as dd
import sys
from typing import Any
import importlib.util
import logging
from starlette.middleware.sessions import SessionMiddleware
import asyncio

from app.core.cache import create_cached_decorator
from app.core.exceptions import (
    DataException,
    GeneNotFoundException,
    ParseException,
    VariantNotFoundException,
)
from app.services.datafetch_ld import LDDatafetch
from app.services.request_util import RequestUtil
from app.core.variant import Variant
from app.services.rsid_db import RsidDB
from app.services.gnomad import GnomAD
from app.services.metadata import Metadata
from app.services.datafetch import Datafetch
from app.core.logging_config import setup_logging
from app.core.datatypes import ResponseTime
from app.core.auth import get_current_user, google_auth, verify_membership

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://anno.finngen.fi",
        "https://annopublic.finngen.fi",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["X-Login-URL", "Set-Cookie"],
)

app.add_middleware(
    SessionMiddleware,
    secret_key="secret-key",
    max_age=24 * 60 * 60 * 365,  # 1 year
    same_site="lax",
    https_only=True,
    session_cookie="session",
    path="/",
)

try:
    spec = importlib.util.spec_from_file_location("config", "app/config/config.py")
    _conf_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(_conf_module)
except Exception:
    logger.error("Could not load config from ./app/config/config.py")
    sys.exit(1)

config = {
    key: getattr(_conf_module, key)
    for key in dir(_conf_module)
    if not key.startswith("_")
}

request_util = RequestUtil(config)
data_fetch = Datafetch(config)
ld_fetch = LDDatafetch(config)
meta = Metadata(config)
gnomad_fetch = GnomAD(config)
rsid_db = RsidDB(config)

cached = create_cached_decorator(config)


@cached
async def fetch_gnomad(variants: list[Variant]) -> dict[str, Any]:
    return await gnomad_fetch.fetch_variants(variants, None)


@cached
async def fetch_assoc(variants: list[Variant]) -> dict[str, Any]:
    return await data_fetch.fetch_variants("assoc", variants)


@cached
async def fetch_ld_assoc(variants: list[Variant]) -> dict[str, Any]:
    return await ld_fetch.fetch_variants(variants)


@cached
async def fetch_finemapped(variants: list[Variant]) -> dict[str, Any]:
    return await data_fetch.fetch_variants("finemapped", variants)


def is_public_endpoint(request: Request) -> bool:
    route = request.scope.get("route")
    if route and getattr(route.endpoint, "is_public", False):
        return True
    return False


async def auth_required(
    request: Request, user: str | None = Depends(get_current_user)
) -> str:
    if not config["authentication"]:
        return None
    if is_public_endpoint(request):
        return None
    if user is None:
        # Use x-Forwarded-Host header which contains the original host requested by the client
        logger.debug(request.headers)
        host = request.headers.get("x-forwarded-host") or request.headers.get(
            "host", "localhost:4000"
        )
        scheme = request.headers.get("x-forwarded-proto", "http")
        base_url = f"{scheme}://{host}"

        raise HTTPException(
            status_code=401,
            detail="Not authenticated",
            headers={
                "X-Login-URL": f"{base_url}/api/v1/login",  # Frontend can use this to redirect
            },
        )
    return user


def is_public(func):
    setattr(func, "is_public", True)
    return func


@app.get("/api/v1")
@is_public
async def root():
    return JSONResponse(
        {
            "name": "Genetics Results API",
            "status": "ok",
            "endpoints": {
                "health": "/api/v1/healthz",
                "auth": {
                    "login": "/api/v1/login",
                    "logout": "/api/v1/logout",
                },
            },
        }
    )


@app.get("/api/v1/healthz")
@is_public
async def healthz(request: Request) -> Response:
    return JSONResponse(
        content={"status": "ok!"},
        status_code=200,
        headers={
            "Content-Type": "application/json",
            "Cache-Control": "no-cache, no-store, must-revalidate",
        },
    )


@app.get("/api/v1/config")
async def get_config(user: str = Depends(auth_required)):
    return JSONResponse(
        {
            "gnomad": config["gnomad"],
            "assoc": {"resources": config["assoc_files"]},
            "ld_assoc": config["ld_assoc"],
            "finemapped": {"resources": config["finemapped_files"]},
        }
    )


@app.get("/api/v1/gene_model/{chr}/{start}/{end}")
async def gene_model(
    chr: str, start: int, end: int, user: str = Depends(auth_required)
) -> Response:
    return request_util.stream_tabix_response(
        config["genes"]["model_file"], f"{chr}:{start}-{end}"
    )


@app.get("/api/v1/gene_model_by_gene/{gene}/{padding}")
async def gene_model_by_gene(
    gene: str, padding: int, user: str = Depends(auth_required)
) -> Response:
    if padding < 0:
        return JSONResponse(
            {"message": "padding must be non-negative"}, status_code=400
        )
    try:
        chr, start, end = request_util.get_gene_range(gene.upper())
    except GeneNotFoundException as e:
        return JSONResponse({"message": str(e)}, status_code=404)
    return request_util.stream_tabix_response(
        config["genes"]["model_file"], f"{chr}:{start-padding}-{end+padding}"
    )


@app.get("/api/v1/gene_cs/{gene}")
async def gene_cs(
    gene: str, padding: int = 0, user: str = Depends(auth_required)
) -> Response:
    try:
        chr, start, end = request_util.get_gene_range(gene.upper())
    except GeneNotFoundException as e:
        return JSONResponse({"message": str(e)}, status_code=404)

    return request_util.stream_tabix_response(
        config["finemapped"]["file"], f"{chr}:{start-padding}-{end+padding}"
    )


@app.get("/api/v1/gene_cs_trans/{gene}")
async def gene_cs_trans(gene: str, user: str = Depends(auth_required)) -> Response:
    try:
        chr, start, end = request_util.get_gene_range(gene.upper())
    except GeneNotFoundException as e:
        return JSONResponse({"message": str(e)}, status_code=404)

    return request_util.stream_tabix_response(
        config["finemapped"]["trans_file"], f"{chr}:{start}-{start}"
    )


@app.post("/api/v1/dataset_metadata")
async def dataset_metadata(
    request: Request, user: str = Depends(auth_required)
) -> StreamingResponse:
    datasets = await request.json()

    def generate_tsv():
        try:
            conn = sqlite3.connect(config["metadata_db"], check_same_thread=False)
            c: sqlite3.Cursor = conn.cursor()
            output = StringIO()
            header = [
                "resource",
                "data_type",
                "dataset_id",
                "study_id",
                "study_label",
                "sample_group",
                "tissue_id",
                "tissue_label",
                "condition_label",
                "sample_size",
                "quant_method",
            ]
            output.write("\t".join(header) + "\n")
            yield output.getvalue()
            output.truncate(0)
            output.seek(0)

            query = """
                SELECT resource, data_type, dataset_id, study_id, study_label,
                    sample_group, tissue_id, tissue_label, condition_label,
                    sample_size, quant_method
                FROM dataset
                WHERE dataset_id IN ({})
            """.format(
                ",".join("?" * len(datasets))
            )

            c.execute(query, datasets)

            while True:
                row = c.fetchone()
                if row is None:
                    break
                output.write("\t".join(map(str, row)) + "\n")
                yield output.getvalue()
                output.truncate(0)
                output.seek(0)

            c.close()
            conn.close()
        except Exception as e:
            logger.error(e)
            yield f"!error: failed to read"

    return StreamingResponse(generate_tsv(), media_type="text/tab-separated-values")


@app.post("/api/v1/trait_metadata")
async def trait_metadata(
    request: Request, user: str = Depends(auth_required)
) -> StreamingResponse:
    phenotypes = await request.json()

    def generate_tsv():
        # TODO sqlite3.ProgrammingError: SQLite objects created in a thread can only be used in that same thread.
        # happens when we don't set check_same_thread=False
        # this problem occurs when there are multiple concurrent requests to the server, even when the other requests are to different endpoints
        try:
            conn = sqlite3.connect(config["metadata_db"], check_same_thread=False)
            c: sqlite3.Cursor = conn.cursor()
            output = StringIO()
            header = [
                "resource",
                "data_type",
                "trait_type",
                "phenocode",
                "phenostring",
                "category",
                "num_samples",
                "num_cases",
                "num_controls",
                "pub_author",
                "pub_date",
            ]
            output.write("\t".join(header) + "\n")
            yield output.getvalue()
            output.truncate(0)
            output.seek(0)

            query = """
                SELECT resource, data_type, trait_type, phenocode, phenostring, category,
                    num_samples, num_cases, num_controls,
                    pub_author, pub_date
                FROM trait
                WHERE resource = ? AND phenocode = ?
            """

            for p in phenotypes:
                c.execute(query, [p["resource"], p["phenocode"]])
                row = c.fetchone()
                if row:
                    output.write("\t".join(map(str, row)) + "\n")
                    yield output.getvalue()
                    output.truncate(0)
                    output.seek(0)

            c.close()
            conn.close()
        except Exception as e:
            logger.error(e)
            yield f"!error: failed to read"

    return StreamingResponse(generate_tsv(), media_type="text/tab-separated-values")


@app.post("/api/v1/variant_annotation_range/{chr}/{start}/{end}")
async def variant_annotation_range(
    chr: str,
    start: str,
    end: str,
    variants: list[str] = Body(...),
    user: str = Depends(auth_required),
) -> StreamingResponse:
    try:
        start = int(start)
        end = int(end)
    except ValueError as e:
        return JSONResponse({"message": "invalid start or end"}, status_code=400)
    variant_dict = {v.encode("utf-8"): True for v in variants}
    # # this would apply if we didn't search a range but specific variants
    # region_file_content = [
    #     f"{v.split(":")[0]}:{v.split(":")[1]}-{v.split(":")[1]}" for v in variants
    # ].join("\n")

    def iter_stdout():
        process = subprocess.Popen(
            [
                "tabix",
                "-h",
                # "-R",
                # "<(echo " + region_file_content + ")",
                config["gnomad"]["file"],
                f"{chr}:{start}-{end}",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            n = 0
            header = process.stdout.readline()
            yield header
            for line in process.stdout:
                n += 1
                fields = line.split(b"\t")
                # only return specified variants - we don't to return all variants in the region because there can be quite many of them
                if (
                    b":".join([fields[0], fields[1], fields[2], fields[3]])
                    in variant_dict
                ):
                    yield line
            process.stdout.close()
            process.wait()
            if process.returncode != 0:
                error_message = process.stderr.read().decode("utf-8")
                logger.error(error_message)
                yield f"!error: failed to read"
        except Exception as e:
            logger.error(e)
            yield f"!error: failed to read"

    return StreamingResponse(iter_stdout(), media_type="application/octet-stream")


@app.post("/api/v1/variant_annotation")
async def variant_annotation(
    variants: list[str] = Body(...), user: str = Depends(auth_required)
) -> StreamingResponse:
    variant_dict = {v.encode("utf-8"): True for v in variants}

    def iter_stdout():
        regions = "".join(
            f"{v.split(':')[0]}\t{v.split(':')[1]}\t{v.split(':')[1]}\n"
            for v in variants
        )
        process = subprocess.Popen(
            [
                "tabix",
                "-h",
                "-R",
                "/dev/stdin",
                config["gnomad"]["file"],
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # send regions to tabix's stdin
        process.stdin.write(regions.encode())
        process.stdin.close()
        try:
            n = 0
            header = process.stdout.readline()
            yield header
            for line in process.stdout:
                n += 1
                fields = line.split(b"\t")
                # only return specified variants because multiallelics
                if (
                    b":".join([fields[0], fields[1], fields[2], fields[3]])
                    in variant_dict
                ):
                    yield line
            process.stdout.close()
            process.wait()
            if process.returncode != 0:
                error_message = process.stderr.read().decode("utf-8")
                logger.error(error_message)
                yield f"!error: failed to read"
        except Exception as e:
            logger.error(e)
            yield f"!error: failed to read"

    return StreamingResponse(iter_stdout(), media_type="application/octet-stream")


@app.post("/api/v1/results")
async def results(request: Request, user: str = Depends(auth_required)) -> Response:
    start_time = timeit.default_timer()
    query = (await request.json())["query"].strip()
    if query in config["variant_set_files"]:
        query = open(config["variant_set_files"][query]["file"]).read()
    if request_util.looks_like_a_gene(query):
        return await gene_results(query)
    try:
        parsed = request_util.parse_query(query)
    except ParseException as e:
        return JSONResponse({"message": str(e)}, status_code=400)

    if len(parsed[1]) > config["max_query_variants"]:
        logger.warn(str(len(parsed[1])) + " variants given, too much")
        return JSONResponse(
            {
                "message": "a maximum of "
                + str(config["max_query_variants"])
                + " variants are accepted"
            },
            status_code=400,
        )

    data = []
    unparsed_variants = set()
    rsid_map = dd(list)
    input_variants = {}
    for tpl in parsed[1]:
        try:
            vars = [Variant(tpl[0])]
        except ParseException as e:
            try:
                vars = rsid_db.get_variants_by_rsid(tpl[0])
            except ParseException as e:
                unparsed_variants.add(tpl[0])
                continue
        input_variants.update({str(var): (var, tpl[1], tpl[2]) for var in vars})
        for var in vars:
            rsid_map[tpl[0]].append(str(var))

    input_variants_list = sorted([var[0] for var in input_variants.values()])
    gnomad_data, assoc_data, ld_assoc_data, finemapped_data = await asyncio.gather(
        fetch_gnomad(input_variants_list),
        fetch_assoc(input_variants_list),
        fetch_ld_assoc(input_variants_list),
        fetch_finemapped(input_variants_list),
    )

    end_time = timeit.default_timer()
    gnomad_time = gnomad_data["time"]
    assoc_time = assoc_data["time"]
    ld_assoc_time = ld_assoc_data["time"]
    finemapped_time = finemapped_data["time"]

    data = []
    uniq_phenos: set[tuple[str, str, str, str]] = set()
    uniq_datasets: set[str] = set()
    for variant_str in sorted(gnomad_data["data"]):
        variant_assoc = (
            assoc_data["data"][variant_str] if variant_str in assoc_data["data"] else []
        )
        variant_ld_assoc = (
            ld_assoc_data["data"][variant_str]
            if variant_str in ld_assoc_data["data"]
            else []
        )
        variant_finemapped = (
            finemapped_data["data"][variant_str]
            if variant_str in finemapped_data["data"]
            else []
        )
        data.append(
            {
                "variant": variant_str,
                "beta": input_variants[variant_str][1],
                "value": input_variants[variant_str][2],
                "gnomad": gnomad_data["data"][variant_str],
                "assoc": {
                    "data": sorted(
                        variant_assoc + variant_ld_assoc,
                        key=lambda x: -float(x["mlog10p"]),
                    ),
                    "resources": sorted(
                        list(set(assoc_data["resources"] + ld_assoc_data["resources"]))
                    ),
                    "time": assoc_time,
                },
                "finemapped": {
                    "data": variant_finemapped,
                    "resources": finemapped_data["resources"],
                    "time": finemapped_time,
                },
            }
        )
        uniq_phenos.update(
            [
                (a["data_type"], a["resource"], a["dataset"], a["phenocode"])
                for a in variant_assoc + variant_ld_assoc + variant_finemapped
            ]
        )
        uniq_datasets.update(
            a["dataset"] for a in variant_assoc + variant_ld_assoc + variant_finemapped
        )

    try:
        # use resource:phenocode as key
        # note that for eQTL Catalogue leafcutter, phenocode is dataset:phenocode
        phenos = {
            pheno[1]
            + ":"
            + pheno[3]: meta.get_phenotype(pheno[0], pheno[1], pheno[2], pheno[3])
            for pheno in uniq_phenos
        }
    except DataException as e:
        logger.error(e)
        return JSONResponse({"message": str(e)}, status_code=500)
    datasets = {
        ds["dataset_id"]: ds
        for ds in [meta.get_dataset(dataset) for dataset in uniq_datasets]
        if ds is not None
    }

    uniq_most_severe = set()
    for variant_str in gnomad_data["data"]:
        for type in ["exomes", "genomes"]:
            if (
                type in gnomad_data["data"][variant_str]
                and gnomad_data["data"][variant_str][type] is not None
            ):
                uniq_most_severe.add(
                    gnomad_data["data"][variant_str][type]["most_severe"]
                )

    logger.info(f"processing time (s): {end_time - start_time}")

    return JSONResponse(
        {
            "data": data,
            "most_severe": sorted(list(uniq_most_severe)),
            "phenos": phenos,
            "datasets": datasets,
            "freq_summary": gnomad_data["freq_summary"],
            "has_betas": parsed[0] == "group",
            "has_custom_values": parsed[1][0][2] is not None,
            "input_variants": {
                "found": gnomad_data["found_variants"],
                "not_found": list(
                    set(str(v) for v in input_variants)
                    - set(gnomad_data["found_variants"])
                ),
                "ac0": gnomad_data["ac0_variants"],
                "unparsed": sorted(list(unparsed_variants)),
                "rsid_map": rsid_map,
            },
            "meta": {
                "gnomad": config["gnomad"],
                "assoc": {"resources": config["assoc_files"]},
                "ld_assoc": {"resources": config["ld_assoc"]},
                "finemapped": {"resources": config["finemapped_files"]},
            },
            "query_type": "variant",
            "time": {
                "gnomad": gnomad_time,
                "finemapped": finemapped_time,
                "assoc": assoc_time,
                "ld_assoc": ld_assoc_time,
                "total": end_time - start_time,
            },
        },
        status_code=200,
    )


# TODO this is a copy of the variant results function, but it should be refactored to avoid code duplication
async def gene_results(gene: str) -> Any | tuple[Any, int]:
    start_time = timeit.default_timer()
    try:
        tabix_range = request_util.get_gene_range(gene)
    except GeneNotFoundException as e:
        return JSONResponse({"message": str(e)}, status_code=404)
    tabix_range_str = f"{tabix_range[0]}\t{tabix_range[1]}\t{tabix_range[2]}"
    time: ResponseTime = {"gnomad": 0, "finemapped": 0, "assoc": 0, "total": 0}
    uniq_most_severe: set[str] = set()
    uniq_phenos: set[tuple[str, str, str, str]] = set()
    uniq_datasets: set[str] = set()
    data = []
    try:
        gnomad_start = timeit.default_timer()
        gnomad = await gnomad_fetch.fetch_ranges(tabix_range_str, gene)
        gnomad_time = timeit.default_timer() - gnomad_start
    except VariantNotFoundException as e:
        return JSONResponse(
            {"message": f"No variants found for gene {gene}"}, status_code=404
        )
    try:  # TODO only parse variants that are in the gnomad result set for speedup
        finemapped_start = timeit.default_timer()
        finemapped = await data_fetch.fetch_ranges("finemapped", tabix_range_str)
        finemapped_time = timeit.default_timer() - finemapped_start
        assoc_start = timeit.default_timer()
        assoc = await data_fetch.fetch_ranges("assoc", tabix_range_str)
        assoc_time = timeit.default_timer() - assoc_start
    except DataException as e:
        return JSONResponse({"message": str(e)}, status_code=500)
    for variant in gnomad["data"]:
        if gnomad["data"][variant]["exomes"] is not None:
            csq = gnomad["data"][variant]["exomes"]["consequences"]
            for c in csq:
                if (
                    (
                        "gene_symbol" not in c
                        or c["gene_symbol"] is None
                        or c["gene_symbol"].upper() == gene.upper()
                    )
                    and c["consequence"] in config["coding_set"]
                    and (variant in finemapped["data"] or variant in assoc["data"])
                ):
                    data.append(
                        {
                            "variant": variant,
                            "gnomad": gnomad["data"][variant],
                            "finemapped": {
                                "data": finemapped["data"][variant],
                                "resources": finemapped["resources"],
                                "time": finemapped_time,
                            },
                            "assoc": {
                                "data": assoc["data"][variant],
                                "resources": assoc["resources"],
                                "time": assoc_time,
                            },
                        }
                    )
                    uniq_phenos.update(
                        [
                            (
                                a["data_type"],
                                a["resource"],
                                a["dataset"],
                                a["phenocode"],
                            )
                            for a in assoc["data"][variant]
                            + finemapped["data"][variant]
                        ]
                    )
                    uniq_datasets.update(
                        a["dataset"]
                        for a in assoc["data"][variant] + finemapped["data"][variant]
                    )
                    for type in ["exomes", "genomes"]:
                        if type in gnomad["data"] and gnomad["data"][type] is not None:
                            uniq_most_severe.add(gnomad["data"][type]["most_severe"])
                    break

    try:
        # use resource:phenocode as key
        # note that for eQTL Catalogue leafcutter, phenocode is dataset:phenocode
        phenos = {
            pheno[1]
            + ":"
            + pheno[3]: meta.get_phenotype(pheno[0], pheno[1], pheno[2], pheno[3])
            for pheno in uniq_phenos
        }
    except DataException as e:
        logger.error(e)
        return JSONResponse({"message": str(e)}, status_code=500)
    datasets = {
        ds["dataset_id"]: ds
        for ds in [meta.get_dataset(dataset) for dataset in uniq_datasets]
        if ds is not None
    }
    return JSONResponse(
        {
            "data": data,
            "most_severe": sorted(list(uniq_most_severe)),
            "phenos": phenos,
            "datasets": datasets,
            "freq_summary": gnomad["freq_summary"],
            "has_betas": False,
            "has_custom_values": False,
            "meta": {
                "gnomad": config["gnomad"],
                "assoc": {"resources": config["assoc_files"]},
                "ld_assoc": {"resources": config["ld_assoc"]},
                "finemapped": {"resources": config["finemapped_files"]},
            },
            "query_type": "gene",
            "time": {
                "gnomad": gnomad_time,
                "finemapped": finemapped_time,
                "assoc": assoc_time,
                "total": timeit.default_timer() - start_time,
            },
        },
        status_code=200,
    )


@app.get("/api/v1/login")
@is_public
async def login(request: Request, frontend_url: str | None = None):
    logger.debug(f"Login attempt - Frontend URL: {frontend_url}")
    logger.debug(f"Request headers: {dict(request.headers)}")

    next_url = frontend_url or "/"
    request.session["next"] = next_url
    request.session["frontend_url"] = frontend_url

    logger.debug(f"Session after setting next URL: {dict(request.session)}")

    # use X-Forwarded-Host header which contains the original host requested by the client
    host = request.headers.get("x-forwarded-host") or request.headers.get(
        "host", "localhost:4000"
    )
    scheme = request.headers.get("x-forwarded-proto", "http")
    base_url = f"{scheme}://{host}"
    redirect_uri = f"{base_url}/api/v1/callback/google"

    logger.debug(f"Redirect URI: {redirect_uri}")

    authorization_url = google_auth.get_authorization_url(redirect_uri)
    logger.debug(f"Google authorization URL: {authorization_url}")

    return RedirectResponse(authorization_url, status_code=303)


@app.get("/api/v1/callback/google")
@is_public
async def oauth_callback_google(request: Request, code: str):
    logger.debug(f"Google callback - Code received: {code[:10]}...")
    logger.debug(f"Request headers: {dict(request.headers)}")
    logger.debug(f"Session before processing: {dict(request.session)}")

    logger.debug(request.headers)
    host = request.headers.get("x-forwarded-host") or request.headers.get(
        "host", "localhost:4000"
    )
    scheme = request.headers.get("x-forwarded-proto", "http")
    base_url = f"{scheme}://{host}"
    redirect_uri = f"{base_url}/api/v1/callback/google"

    try:
        user_info = await google_auth.get_user_info(code, redirect_uri)
        email = user_info["email"]
        logger.debug(f"User info received: {email}")

        if not verify_membership(email):
            logger.error(f"Unauthorized email: {email}")
            raise HTTPException(status_code=403, detail="Unauthorized email address")

        request.session["user_email"] = email
        request.session["authenticated"] = True
        logger.debug(f"Session after setting email: {dict(request.session)}")

        frontend_url = request.session.get("frontend_url")
        logger.debug(f"Frontend URL from session: {frontend_url}")

        response = RedirectResponse(frontend_url, status_code=303)

        logger.debug(
            f"Response cookies: {response.headers.get('set-cookie', 'No cookies set!')}"
        )

        return response

    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Failed to authenticate: {str(e)}")


@app.get("/api/v1/logout")
@is_public
async def logout(request: Request):
    request.session.clear()
    response = JSONResponse({"status": "ok"})

    response.delete_cookie(
        "session", path="/", domain=None, secure=True, httponly=True, samesite="lax"
    )

    return response


@app.get("/api/v1/auth")
@is_public
async def debug_auth(request: Request):
    logger.debug(f"Debug Auth - Headers: {dict(request.headers)}")
    logger.debug(f"Debug Auth - Cookies: {request.cookies}")
    logger.debug(f"Debug Auth - Session: {dict(request.session)}")
    return JSONResponse(
        {
            "session": dict(request.session),
            "base_url": str(request.base_url),
            "origin": request.headers.get("origin"),
            "referer": request.headers.get("referer"),
        }
    )


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["Strict-Transport-Security"] = (
        "max-age=31536000; includeSubDomains"
    )
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    return response
