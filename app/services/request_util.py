import re
import subprocess
import time
import logging
from typing import Any, Literal

from fastapi.responses import StreamingResponse
from app.core.singleton import Singleton
from app.core.exceptions import (
    GeneNotFoundException,
    ParseException,
)
from app.models.domain.variant import Variant

logger = logging.getLogger(__name__)

sep_re_line = re.compile(r"[\r\n|\n]+")
sep_re_delim = re.compile(r"[\s|\t|,]+")
sep_re_line_and_delim = re.compile(r"[\r\n|\n|\s|\t|,]+")


class RequestUtil(object, metaclass=Singleton):
    def __init__(self, conf: dict[str, Any]) -> None:  # TODO type config
        self.conf = conf
        self.geneUpperToRange = {}
        with open(conf["genes"]["start_end_file"], "r") as f:
            header = {h: i for i, h in enumerate(f.readline().strip().split("\t"))}
            for line in f:
                s = line.strip().split("\t")
                self.geneUpperToRange[s[header["Gene name"]].upper()] = (
                    s[header["Chromosome/scaffold name"]],
                    int(s[header["Gene start (bp)"]]),
                    int(s[header["Gene end (bp)"]]),
                )

    def get_gene_range(self, gene: str) -> tuple[str, int, int]:
        """
        Get the chromosome, start, and end of a gene.
        """
        if gene.upper() not in self.geneUpperToRange:
            raise GeneNotFoundException(f"Gene {gene} not found")
        return self.geneUpperToRange[gene.upper()]

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

    def stream_tabix_response(self, file: str, region: str) -> StreamingResponse:
        """
        Stream a tabix response.
        """
        start_time = time.time()

        def iter_stdout():
            process = subprocess.Popen(
                [
                    "tabix",
                    "-h",
                    file,
                    region,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                buffer_size = 1024 * 8
                while True:
                    chunk = process.stdout.read(buffer_size)
                    if not chunk:
                        break
                    yield chunk
                process.stdout.close()
                process.wait()
                logger.info(
                    f"tabix {file} {region} took {time.time() - start_time} seconds"
                )
                if process.returncode != 0:
                    error_message = process.stderr.read().decode("utf-8")
                    logger.error(error_message)
                    yield f"!error: failed to read".encode("utf-8")
            except Exception as e:
                logger.error(e)
                yield f"!error: failed to read".encode("utf-8")

        return StreamingResponse(iter_stdout(), media_type="application/octet-stream")
