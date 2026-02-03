"""
Service for fetching phenotype markdown content from GCS.
"""

import logging

import app.config.common as config
from app.core.exceptions import NotFoundException
from app.core.file_utils import read_file

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
        path = config.phenotype_markdown_template.format(
            resource=resource, phenocode=phenocode
        )

        try:
            return read_file(path)
        except FileNotFoundError:
            raise NotFoundException(
                f"Markdown file not found for {resource}/{phenocode}"
            )
        except Exception as e:
            logger.error(f"Error fetching markdown from {path}: {e}")
            raise
