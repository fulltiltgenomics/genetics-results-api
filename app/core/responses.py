import logging
import time
from typing import AsyncIterator, Literal

from app.core.logging_config import setup_logging
from starlette.responses import Response
from app.core.streams import (
    tsv_line_iterator_str,
    tsv_stream_to_list,
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
            rows = await tsv_stream_to_list(line_stream, header_schema)
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
