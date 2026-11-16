from app.config.mpra import mpra_data
from app.services.data_access_mpra import DataAccessObjectMpra
from app.services.gcloud_tabix_base import GCloudTabixPerDataset


class GCloudTabixDataAccessMpra(GCloudTabixPerDataset, DataAccessObjectMpra):
    """GCloud Storage tabix / direct file access implementation for mpra data."""

    entries = mpra_data
    label = "mpra"
