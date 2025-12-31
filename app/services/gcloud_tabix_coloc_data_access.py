import logging
from typing import AsyncGenerator
from app.services.data_access_coloc import DataAccessObjectColoc
from app.services.gcloud_tabix_base import GCloudTabixBase
from app.config.coloc import coloc

logger = logging.getLogger(__name__)


class GCloudTabixDataAccessColoc(GCloudTabixBase, DataAccessObjectColoc):
    """GCloud Storage tabix / direct file access implementation of data access colocalization data."""

    def __init__(self, name: str):
        DataAccessObjectColoc.__init__(self, name)
        GCloudTabixBase.__init__(self)

        self.configuration = [c for c in coloc if c["name"] == name][0]
        self.credible_set_file = self.configuration["credset_file"]
        self.coloc_file = self.configuration["coloc_file"]
        self.credible_set_header = None
        self.credible_set_header = self.get_credible_set_header()
        self.coloc_header = None
        self.coloc_header = self.get_coloc_header()

    def get_credible_set_header(self) -> list[bytes]:
        """Get the header for the credible set file."""
        return self._cache_header('credible_set_header', self.credible_set_file)

    def get_coloc_header(self) -> list[bytes]:
        """Get the header for the coloc file."""
        return self._cache_header('coloc_header', self.coloc_file)

    async def stream_credible_set_range(
        self, chr: str, start: int, end: int, chunk_size: int
    ) -> AsyncGenerator[bytes, None]:
        """Stream data from a tabix-indexed file for a chromosome range."""
        return await self._stream_range(self.credible_set_file, [chr], [start], [end], chunk_size)

    async def stream_coloc_range(
        self, chr: str, start: int, end: int, chunk_size: int
    ) -> AsyncGenerator[bytes, None]:
        """Stream data from a tabix-indexed file for a chromosome range."""
        return await self._stream_range(self.coloc_file, [chr], [start], [end], chunk_size)
