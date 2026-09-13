import logging
from typing import Literal

from app.config.common import max_range_size_json, max_range_size_stream
from app.config.expression import expression_data

logger = logging.getLogger(__name__)


class RequestUtil:
    def __init__(self) -> None:
        pass

    def validate_range(
        self, chromosome_range: str, format: Literal["tsv", "json"]
    ) -> tuple[int, int, int]:
        """
        Validate a chromosome range. Raises ValueError if the range is invalid.
        """
        try:
            chr_str, start_end = chromosome_range.split(":")
            start_str, end_str = start_end.split("-")
            chr = int(chr_str.replace("chr", "").replace("X", "23"))
            start = int(start_str)
            end = int(end_str)
        except Exception:
            raise ValueError("Range should be in the format chr:start-end")
        if chr < 1 or chr > 23:
            raise ValueError(f"Invalid chromosome: {chr}")
        if start < 1 or end < 1:
            raise ValueError("Range start and end must be positive")
        if start > end:
            raise ValueError("Range start must be less than end")
        if format == "tsv" and end - start > max_range_size_stream:
            raise ValueError(
                f"Maximum range size for TSV is {int(max_range_size_stream)}"
            )
        if format == "json" and end - start > max_range_size_json:
            raise ValueError(
                f"Maximum range size for JSON is {int(max_range_size_json)}"
            )
        return (chr, start, end)

    def check_resources(
        self, resources_list: list[str], data_type: str | None = None
    ) -> bool:
        """
        Check if the resources are valid (included in config). When data_type is
        given, validate against resources offering that data type only (e.g. "cs"),
        so a valid-but-wrong-product resource is rejected up front instead of
        failing mid-stream.
        """
        from app.services.config_util import get_resources
        valid_resources = get_resources(data_type)
        for resource in resources_list:
            if resource not in valid_resources:
                return False
        return True

    def check_expression_resources(self, resources_list: list[str]) -> bool:
        """
        Check if the resources are valid (included in config).
        """
        for resource in resources_list:
            if resource not in [c["resource"] for c in expression_data]:
                return False
        return True
