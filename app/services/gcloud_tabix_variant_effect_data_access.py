from app.config.variant_effect import variant_effect_data
from app.services.data_access_variant_effect import DataAccessObjectVariantEffect
from app.services.gcloud_tabix_base import GCloudTabixPerDataset


class GCloudTabixDataAccessVariantEffect(
    GCloudTabixPerDataset, DataAccessObjectVariantEffect
):
    """GCloud Storage tabix / direct file access implementation for variant_effect data."""

    entries = variant_effect_data
    label = "variant effect"
