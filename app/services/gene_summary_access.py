import json

import app.config.gene_summary as config
from app.core.exceptions import NotFoundException
from app.services.gcloud_tabix_base import GCloudTabixBase


class GeneSummaryAccess(GCloudTabixBase):
    """Reads a gene's summary object from the release folder, one small GET per request."""

    def __init__(self) -> None:
        super().__init__()
        self.prefix: str | None = config.prefix

    async def get(self, symbol: str) -> dict | None:
        """The summary of the gene with this approved symbol, or None when it has none."""
        try:
            return json.loads(await self._fetch_full(f"{self.prefix}{symbol}.json"))
        except NotFoundException:
            return None
