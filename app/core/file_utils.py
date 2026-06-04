"""
Shared file utilities for reading files via fsspec.
"""

import csv
import logging

import fsspec

from app.core.gcs_retry import with_gcs_retry

logger = logging.getLogger(__name__)


def read_file(path: str) -> str:
    """
    Read file content using fsspec, handling .gz/.bgz compression.

    Args:
        path: File path (local or GCS)

    Returns:
        File content as string

    Raises:
        FileNotFoundError: If file not found
    """
    compression = "gzip" if path.endswith((".gz", ".bgz")) else None

    def _read() -> str:
        with fsspec.open(path, "rt", compression=compression) as f:
            return f.read()

    return with_gcs_retry(_read)


def read_tsv_as_json(path: str) -> list[dict]:
    """
    Read TSV file and parse to list of dicts.

    Args:
        path: File path (local or GCS)

    Returns:
        List of dicts, one per row

    Raises:
        FileNotFoundError: If file not found
    """
    content = read_file(path)
    lines = content.strip().split("\n")
    if not lines:
        return []
    reader = csv.DictReader(lines, delimiter="\t")
    return list(reader)
