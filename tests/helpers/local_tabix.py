"""The real tabix range engine with its GCS reads taken from local disk."""

from app.services import gcloud_tabix_base
from app.services.tabix_query import parse_tabix_index


class LocalTabixReads:
    """Mixed in ahead of a GCloudTabixBase subclass: index and block reads from local files."""

    async def _get_index(self, file_path):
        with open(file_path + ".tbi", "rb") as fh:
            return parse_tabix_index(fh.read())

    async def _fetch_blocks(self, url, start, last_block_off):
        with open(url, "rb") as fh:
            fh.seek(start)
            return fh.read(last_block_off - start + gcloud_tabix_base._MAX_BGZF_BLOCK)
