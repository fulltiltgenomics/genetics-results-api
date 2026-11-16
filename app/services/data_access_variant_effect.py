import logging

from app.config.sort_keys import SORT_CONFIG_VARIANT_EFFECT
from app.config.variant_effect import variant_effect_data
from app.services.base_data_access import (
    ConfigListFactory,
    DataAccessObjectPerDataset,
    PerDatasetDataAccess,
)

logger = logging.getLogger(__name__)


class DataAccessObjectVariantEffect(DataAccessObjectPerDataset):
    """Abstract base class for data access operations for variant_effect data.

    The Marderstein resource ships two distinct predictor files (chrombpnet, flare),
    each a separate per-dataset entry.
    """


class DataAccessFactoryVariantEffect(ConfigListFactory):
    """Factory for creating per-dataset data access objects based on configuration."""

    entries = variant_effect_data
    key_field = "dataset_id"
    label = "variant effect"
    implementations = {"gcloud": "app.services.gcloud_tabix_variant_effect_data_access:GCloudTabixDataAccessVariantEffect"}


class DataAccessVariantEffect(PerDatasetDataAccess[DataAccessObjectVariantEffect]):
    """Main data access class that manages per-dataset data access objects."""

    factory_class = DataAccessFactoryVariantEffect
    allele_columns = (2, 3)
    sort_config = SORT_CONFIG_VARIANT_EFFECT
