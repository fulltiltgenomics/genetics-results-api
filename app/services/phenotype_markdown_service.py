"""
Service for fetching phenotype markdown content from GCS.
"""

import logging

import fsspec

import app.config.common as config
from app.core.exceptions import NotFoundException

logger = logging.getLogger(__name__)


class PhenotypeMarkdownService:
    """Service for fetching phenotype markdown files from GCS."""

    def get_markdown(self, resource: str, phenocode: str) -> str:
        """
        Fetch markdown content for a phenotype.

        Args:
            resource: The resource name (e.g., "finngen")
            phenocode: The phenotype code (e.g., "T2D_WIDE")

        Returns:
            Markdown content as string

        Raises:
            NotFoundException: If file not found
        """
        gcs_path = config.phenotype_markdown_template.format(
            resource=resource, phenocode=phenocode
        )

        compression = "gzip" if gcs_path.endswith(".gz") else None

        try:
            with fsspec.open(gcs_path, "rt", compression=compression) as f:
                return f.read()
        except FileNotFoundError:
            raise NotFoundException(
                f"Markdown file not found for {resource}/{phenocode}"
            )
        except Exception as e:
            logger.error(f"Error fetching markdown from {gcs_path}: {e}")
            raise
