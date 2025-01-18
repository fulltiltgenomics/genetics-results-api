from io import StringIO
import sqlite3
from fastapi import Body, FastAPI, Request, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
import subprocess

import sys
from typing import Any, Callable
import importlib.util
import logging

from app.core.exceptions import (
    GeneNotFoundException,
)
from app.services.request_util import RequestUtil
from app.core.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI()

app.add_middleware(GZipMiddleware, minimum_size=1000)

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


def is_public(function: Callable[..., Any]) -> Callable[..., Any]:
    function.is_public = True  # type: ignore
    return function


@app.get("/api/v1/healthz")
@is_public
async def healthz() -> Any:
    return JSONResponse({"status": "ok!"})


@app.get("/auth")
@is_public
async def auth() -> str:
    return "auth.html"


@app.get("/api/v1/config")
async def get_config() -> Any:
    return JSONResponse(
        {
            "gnomad": config["gnomad"],
            "assoc": config["assoc"],
            "finemapped": config["finemapped"],
        }
    )


@app.get("/api/v1/gene_model/{chr}/{start}/{end}")
async def gene_model(chr: str, start: int, end: int) -> Response:
    return request_util.stream_tabix_response(
        config["genes"]["model_file"], f"{chr}:{start}-{end}"
    )


@app.get("/api/v1/gene_cs/{gene}")
async def gene_cs(gene: str, padding: int = 0) -> Response:
    try:
        chr, start, end = request_util.get_gene_range(gene.upper())
    except GeneNotFoundException as e:
        return JSONResponse({"message": str(e)}, status_code=404)

    return request_util.stream_tabix_response(
        config["finemapped"]["file"], f"{chr}:{start-padding}-{end+padding}"
    )


@app.post("/api/v1/trait_metadata")
async def trait_metadata(request: Request) -> StreamingResponse:
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

            chunk_size = 100
            for i in range(0, len(phenotypes), chunk_size):
                chunk = phenotypes[i : i + chunk_size]
                conditions = []
                params = []
                for phenotype in chunk:
                    conditions.append(" (resource = ? AND phenocode = ?)")
                    params.extend([phenotype["resource"], phenotype["phenocode"]])
                query = f"""
                    SELECT resource, data_type, trait_type, phenocode, phenostring, category,
                        num_samples, num_cases, num_controls,
                        pub_author, pub_date
                    FROM trait
                    WHERE {" OR ".join(conditions)}
                """
                c.execute(query, params)

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


@app.post("/api/v1/variant_annotation/{chr}/{start}/{end}")
async def variant_annotation(
    chr: str,
    start: str,
    end: str,
    variants: list[str] = Body(...),
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
