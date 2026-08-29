import logging
import time
from typing import AsyncIterator, Literal

from app.core.logging_config import setup_logging
from starlette.responses import Response
from app.core.streams import (
    tsv_line_iterator_str,
    tsv_stream_to_list_with_header,
    filter_stream_by_coding,
    filter_stream_by_column,
    filter_coding_rows,
    filter_rows_by_column,
)
import app.config.common as config_common
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi import HTTPException

setup_logging()
logger = logging.getLogger(__name__)


class TimedStreamingResponse(StreamingResponse):
    """
    Streaming response that logs the total time taken to stream the response.
    """

    def __init__(self, content, url, start_time, *args, **kwargs):
        super().__init__(content, *args, **kwargs)
        self.url = url
        self.start_time = start_time

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            total_time = time.time() - self.start_time
            logger.info(
                f"{self.url} total streaming time (including client transfer): {total_time:.3f}s"
            )


class TimedJSONResponse(JSONResponse):
    """
    JSON response that logs the total time taken to send the response.
    """

    def __init__(self, content, url, start_time, *args, **kwargs):
        super().__init__(content, *args, **kwargs)
        self.url = url
        self.start_time = start_time

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            total_time = time.time() - self.start_time
            logger.info(
                f"{self.url} total JSON response time (including client transfer): {total_time:.3f}s"
            )


COLUMNS_HEADER = "X-Columns"


def columns_header(columns: list[str]) -> dict[str, str]:
    """Advertise a JSON result's column names out of band, as a response header.

    A JSON range response is a bare array, so an empty result carries no schema at all and
    a client cannot tell "no rows" from "no such columns". Consumers that build a dataframe
    from it (the SDK behind the sandboxed code-execution agent) then raise ColumnNotFound
    on a perfectly ordinary no-hit query.

    This goes in a header rather than in the body ON PURPOSE: the browser and the MCP
    server both parse the body as an array, so wrapping it in an envelope would be a
    breaking change for them, while an added response header cannot be observed by a
    consumer that does not ask for it.

    JSON only. On the TSV path the header line is already the first line of the body, and
    genetics-results-suite-7yg established that path must not buffer the stream to inspect
    it.

    Fail-open by design: header values are latin-1 encoded by Starlette and the value is
    comma-delimited, so a name that cannot survive the round trip drops the header rather
    than turning a working request into a 500. Nothing today has such a name. That is not in
    tension with `verified_columns_header` below, which DOES raise: the two answer different
    questions. Here the columns are known to be right and only the transport is in doubt, so
    dropping the header loses nothing a client had before. There the columns themselves are
    in doubt, and serving a schema that contradicts the rows is the failure.

    This guard is defence in depth, not the primary control: every column here has already
    passed `header_schema` validation (app/core/streams.py), which only allows
    `[A-Za-z0-9_.-]+`. If that validation is ever loosened, the guard below is what stops a
    non-ASCII, comma-bearing, or control-character (e.g. CR/LF response-splitting) name from
    reaching the header.
    """
    if not columns or any(
        "," in c or not c.isascii() or not c.isprintable() for c in columns
    ):
        return {}
    return {COLUMNS_HEADER: ",".join(columns)}


class ColumnDeclarationError(RuntimeError):
    """A router's declared output columns disagree with the rows it is returning."""


def verified_columns_header(
    declared: list[str] | tuple[str, ...], rows: list[dict] | None
) -> dict[str, str]:
    """Advertise DECLARED columns, refusing when the rows on hand contradict them.

    The endpoints that stream a TSV read their column names off the file's own header line
    (``range_response`` above), so their advertisement cannot drift. The four SDK functions
    of genetics-results-suite-8a1 compute their JSON instead, and for them the declaration
    IS the ground truth — there is no file header to be a superset of.

    A hand-maintained list per router is exactly the thing that rots, so this refuses
    rather than degrading: every non-empty response cross-checks its own declaration
    against the rows it is about to serialize, and a disagreement raises. Order matters —
    the SDK builds a frame from this list, and a reordering is a silently wrong schema.

    The check is deliberately on the response path rather than only in a test: most of the
    declarations here are the same object the rows are built from (a ``.select()`` list, a
    profile's ``output_columns``), so they cannot drift at all, and the ones that CAN drift
    (search's result dicts) are assembled from data that only a running index has. A test
    fixture cannot prove anything about those; a real response can.
    """
    if rows:
        actual = list(rows[0])
        if actual != list(declared):
            raise ColumnDeclarationError(
                f"declared columns {list(declared)} do not match the returned row {actual}"
            )
    return columns_header(list(declared))


async def range_response(
    request_url: str,
    stream: AsyncIterator[bytes],
    header_schema: dict[str, type],
    format: Literal["tsv", "json"],
    start_time: float,
    coding_only: bool = False,
    data_types: set[str] | None = None,
) -> Response:
    """
    Helper function to create a TSV/JSON response from a stream, logging response times.

    When ``coding_only`` is True, rows are restricted to coding variants by their inline
    ``most_severe`` column (see config.common.coding_set); the default (False) is unchanged.
    When ``data_types`` is given, rows are restricted to those association types by their
    inline ``data_type`` column, matched case-insensitively.
    """
    if format == "tsv":
        other_time = time.time()
        logger.debug(
            f"{request_url} time to start streaming range: {other_time - start_time:.3f}s"
        )
        try:
            if data_types:
                stream = filter_stream_by_column(
                    stream, "data_type", data_types, case_insensitive=True
                )
            if coding_only:
                stream = filter_stream_by_coding(stream, config_common.coding_set)
            return TimedStreamingResponse(
                stream, request_url, start_time, media_type="text/tab-separated-values"
            )
        except Exception as e:
            logger.error(f"{request_url} error streaming range: {e}")
            raise HTTPException(status_code=500, detail="Error streaming range")
    elif format == "json":
        try:
            line_stream = tsv_line_iterator_str(stream)
            other_time = time.time()
            logger.debug(
                f"{request_url} time to start creating JSON response: {other_time - start_time:.3f}s"
            )
            header, rows = await tsv_stream_to_list_with_header(
                line_stream, header_schema
            )
            if data_types:
                rows = filter_rows_by_column(
                    rows, "data_type", data_types, case_insensitive=True
                )
            if coding_only:
                rows = filter_coding_rows(rows, config_common.coding_set)
            return TimedJSONResponse(
                rows,
                request_url,
                start_time,
                headers=columns_header(header),
            )
        except Exception as e:
            logger.error(f"{request_url} error streaming or parsing data: {e}")
            raise HTTPException(
                status_code=500, detail="Error streaming or parsing data"
            )
        finally:
            logger.debug(
                f"{request_url} time to create JSON response: {time.time() - other_time:.3f}s"
            )
