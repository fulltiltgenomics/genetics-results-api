import logging
from typing import AsyncGenerator

from app.config.expression import expression_data
from app.services.data_access_expression import DataAccessObjectExpression
from app.services.gcloud_tabix_base import GCloudTabixSingleFile

logger = logging.getLogger(__name__)


class GCloudTabixDataAccessExpression(GCloudTabixSingleFile, DataAccessObjectExpression):
    """GCloud Storage tabix / direct file access implementation of data access for expression data."""

    def __init__(self, resource: str):
        DataAccessObjectExpression.__init__(self, resource)
        self.resource_config = self._init_single_file(expression_data, "resource", resource)
        self.gencode_version = self.resource_config["gencode_version"]

    async def stream_range(
        self, chr: list[int], start: list[int], end: list[int], chunk_size: int
    ) -> AsyncGenerator[bytes, None]:
        """Stream data from a tabix-indexed file for a chromosome range."""
        return await self._stream_range(self.file, chr, start, end, chunk_size)
