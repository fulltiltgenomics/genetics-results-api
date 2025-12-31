import re
import logging
from typing import Literal
from app.config.expression import expression_data
from app.config.common import max_range_size_json, max_range_size_stream
from app.core.exceptions import (
    ParseException,
)
from app.core.variant import Variant

logger = logging.getLogger(__name__)

sep_re_line = re.compile(r"[\r\n|\n]+")
sep_re_delim = re.compile(r"[\s|\t|,]+")
sep_re_line_and_delim = re.compile(r"[\r\n|\n|\s|\t|,]+")


class RequestUtil:
    def __init__(self) -> None:
        pass

    def parse_query(
        self,
        query: str,
    ) -> tuple[Literal["single", "group"], list[tuple[str, float, str | None]]]:
        """
        Parse a query into a list of variants and groups.
        """
        items = sep_re_line.split(query)
        len_first = len(sep_re_delim.split(items[0]))
        if len_first > 1:  # input has betas and possibly groups attached
            input = []
            try:
                for line in items:
                    s = sep_re_delim.split(line)
                    if s[0] != "":
                        input.append(
                            (
                                s[0],
                                float(s[1]),
                                s[2] if len_first > 2 else None,
                            )
                        )
            except IndexError:
                raise ParseException(
                    "Oops, I cannot parse that. Try providing either one variant per line or all variants in one line separated by space or comma. Or variant, beta, and optionally any custom value separated by space or comma on each line."
                )
            except ValueError as e:
                raise ParseException(
                    "Oops, I cannot parse that. Looks like some beta value is not numeric in the input: "
                    + str(e)
                )
            return ("group", input)
        else:
            items = sep_re_line_and_delim.split(query)
            vars: list[str] = list(dict.fromkeys([item.strip() for item in items]))
            input = [
                (var, 0, None) for var in vars if var != ""
            ]  # would like to use None instead of 0 but mypy complains and I don't know how to fix it
            return ("single", input)

    def looks_like_a_gene(self, query: str) -> bool:
        """
        Check if a query looks like a gene.
        """
        items = [item for item in sep_re_line.split(query) if item.strip() != ""]
        if len(items) != 1:
            return False
        try:
            _ = Variant(items[0])
        except ParseException:
            if not re.match(
                r"^rs\d\d+", items[0]
            ):  # not an rsid - apparently RS1 is the only gene that starts with rs
                return True
        return False

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
            raise ValueError(f"Range should be in the format chr:start-end")
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

    def check_resources(self, resources_list: list[str]) -> bool:
        """
        Check if the resources are valid (included in config).
        """
        from app.services.config_util import get_resources
        valid_resources = get_resources()
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
