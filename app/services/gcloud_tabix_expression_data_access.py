import logging
from typing import AsyncGenerator
from app.services.data_access_expression import DataAccessObjectExpression
from app.services.gcloud_tabix_base import GCloudTabixBase
from app.config.expression import expression_data

logger = logging.getLogger(__name__)


class GCloudTabixDataAccessExpression(GCloudTabixBase, DataAccessObjectExpression):
    """GCloud Storage tabix / direct file access implementation of data access for expression data."""

    def __init__(self, resource: str):
        DataAccessObjectExpression.__init__(self, resource)
        GCloudTabixBase.__init__(self)

        self.resource_config = [
            c for c in expression_data if c["resource"] == resource
        ][0]
        self.gencode_version = self.resource_config["gencode_version"]
        self.file = self.resource_config["file"]
        self.header = None
        self.header = self.get_header()

    def get_header(self) -> list[bytes]:
        """Get the header for the expression data file."""
        return self._cache_header('header', self.file)

    async def stream_range(
        self, chr: list[int], start: list[int], end: list[int], chunk_size: int
    ) -> AsyncGenerator[bytes, None]:
        """Stream data from a tabix-indexed file for a chromosome range."""
        return await self._stream_range(self.file, chr, start, end, chunk_size)
