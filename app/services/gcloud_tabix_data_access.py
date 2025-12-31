import logging
from typing import Any, AsyncGenerator, Literal
from app.core.streams import tsv_line_iterator_str, tsv_stream_to_list
from app.services.data_access import DataAccessObject
from app.services.gcloud_tabix_base import GCloudTabixBase
from app.config.credible_sets import data_file_by_id as cs_data_file_by_id
from app.config.exome_results import exome_data_file_by_id
import aiohttp.client_exceptions
import time

# merge both config dicts
data_file_by_id = {**cs_data_file_by_id, **exome_data_file_by_id}

logger = logging.getLogger(__name__)


class GCloudTabixDataAccess(GCloudTabixBase, DataAccessObject):
    """GCloud Storage tabix / direct file access implementation of data access for a specific data file and data type (cs, assoc)."""

    def __init__(self, data_file_id: str, data_type: Literal["cs", "assoc"]):
        DataAccessObject.__init__(self, data_file_id, data_type)
        GCloudTabixBase.__init__(self)

        df = data_file_by_id[data_file_id]

        # check if this data file supports the requested data type
        valid_data_types = {"cs", "assoc", "exome"}
        if data_type not in df:
            available = [k for k in df.keys() if k in valid_data_types]
            raise ValueError(
                f"Data file '{data_file_id}' does not support '{data_type}'. "
                f"Supported: {available or 'none'}"
            )

        self.resource_config = df[data_type]
        self.gencode_version = df.get("gencode_version")
        self.header = None
        self.header = self.get_header()
        self.qtl_header = None
        if data_type == "cs" and "all_cs_qtl_file" in self.resource_config:
            self.qtl_header = self.get_header(True)
        self.gene_positions = None

    def _get_blob_path(
        self,
        phenotype: str,
        interval: Literal[95, 99] | None,
    ) -> str:
        """Get the blob path for the requested phenotype."""
        # if no interval specified, use suffix directly (for exome data)
        if interval is None:
            return f"{self.resource_config['prefix']}{phenotype}{self.resource_config['suffix']}"

        try:
            return f"{self.resource_config['prefix']}{phenotype}{self.resource_config[f'suffix_{interval}']}"
        except KeyError:
            raise ValueError(
                f"Interval {interval} not available for resource {self.resource}"
            )

    async def check_phenotype_exists(
        self, phenotype: str, interval: Literal[95, 99] | None = None
    ) -> bool:
        """Check if a phenotype's or study's data exists in the appropriate data source for the resource."""
        blob_path = self._get_blob_path(phenotype, interval)
        headers = await self.storage._headers()
        url = blob_path.replace("gs://", "https://storage.googleapis.com/")
        try:
            response = await self.session.get(url, headers=headers)
            response.raise_for_status()
        except aiohttp.client_exceptions.ClientResponseError as e:
            if e.status == 404:
                return False
            raise e
        return True

    def get_header(self, qtl: bool = False) -> list[bytes]:
        """Get the header for the all data file. Doing this with tabix to also be sure that the file is tabix-indexed and tabixing works."""
        if qtl:
            gs_path = self.resource_config["all_cs_qtl_file"]
            return self._cache_header("qtl_header", gs_path)
        else:
            # TODO configure this better
            gs_path = self.resource_config["all_cs_file"] if "all_cs_file" in self.resource_config else self.resource_config["all_exome_file"]
            return self._cache_header("header", gs_path)

    async def stream_range(
        self, chr: list[int], start: list[int], end: list[int], chunk_size: int
    ) -> AsyncGenerator[bytes, None]:
        """Stream data from a tabix-indexed file for a chromosome range."""
        blob_path = self.resource_config["all_cs_file"] if "all_cs_file" in self.resource_config else self.resource_config["all_exome_file"]
        logger.debug(f"Streaming range for {chr}, {start}, {end}")
        return await self._stream_range(blob_path, chr, start, end, chunk_size)

    async def stream_qtl_gene_range(
        self, chr: list[int], pos: list[int], chunk_size: int
    ) -> AsyncGenerator[bytes, None]:
        """Stream data from a tabix-indexed file for a QTL gene range."""
        blob_path = self.resource_config["all_cs_qtl_file"]
        return await self._stream_range(blob_path, chr, pos, pos, chunk_size)

    def has_qtl_gene_data(self) -> bool:
        """Check if the resource has QTL gene data."""
        return (
            self.data_type == "cs"
            and "all_cs_qtl_file" in self.resource_config
            and self.gencode_version is not None
        )

    async def stream_phenotype(
        self,
        phenotype: str,
        interval: Literal[95, 99] | None,
        chunk_size: int,
    ) -> AsyncGenerator[bytes, None]:
        """Stream phenotype data from GCloud Storage."""
        blob_path = self._get_blob_path(phenotype, interval)
        return self._stream_file(blob_path, chunk_size)

    async def json_phenotype(
        self,
        phenotype: str,
        interval: Literal[95, 99] | None,
        header_schema: dict[str, type],
        data_type: str,
        chunk_size: int,
    ) -> list[dict[str, Any]]:
        """Get data as JSON response from GCloud Storage."""
        start_time = time.time()

        blob_path = self._get_blob_path(phenotype, interval)
        stream = self._stream_file(blob_path, chunk_size)
        line_stream = tsv_line_iterator_str(stream)
        rows = await tsv_stream_to_list(line_stream, header_schema)

        logger.debug(
            f"read and parsed {len(rows)} rows from {phenotype} from {self.resource} in {time.time() - start_time} seconds"
        )
        return rows
