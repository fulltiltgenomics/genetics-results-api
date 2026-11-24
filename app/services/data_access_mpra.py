import logging

from app.config.mpra import mpra_data
from app.config.sort_keys import SORT_CONFIG_MPRA
from app.services.base_data_access import (
    ConfigListFactory,
    DataAccessObjectPerDataset,
    PerDatasetDataAccess,
)

logger = logging.getLogger(__name__)


class DataAccessObjectMpra(DataAccessObjectPerDataset):
    """Abstract base class for data access operations for mpra data.

    The Siraj resource ships a single LONG tabix file (one row per variant x
    cell_line), so there is one per-dataset entry.
    """


class DataAccessFactoryMpra(ConfigListFactory):
    """Factory for creating per-dataset data access objects based on configuration."""

    entries = mpra_data
    key_field = "dataset_id"
    label = "MPRA"
    implementations = {"gcloud": "app.services.gcloud_tabix_mpra_data_access:GCloudTabixDataAccessMpra"}


class DataAccessMpra(PerDatasetDataAccess[DataAccessObjectMpra]):
    """Main data access class that manages per-dataset data access objects."""

    factory_class = DataAccessFactoryMpra
    # the LONG file row
    allele_columns = (3, 4)
    sort_config = SORT_CONFIG_MPRA
