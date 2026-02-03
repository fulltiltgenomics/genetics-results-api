"""
Service for fetching credible set statistics files.
"""

import logging
from typing import Literal

from app.config.credible_sets import data_file_by_id, resource_to_data_file_ids
from app.core.exceptions import NotFoundException
from app.core.file_utils import read_file, read_tsv_as_json

logger = logging.getLogger(__name__)


class CredibleSetStatsService:
    """Service for fetching credible set stats files."""

    def _get_stats_paths(self, id_or_resource: str) -> list[tuple[str, str]]:
        """
        Get stats file paths for a data file ID or resource.

        Returns list of (data_file_id, stats_path) tuples.
        """
        if id_or_resource in data_file_by_id:
            data_file = data_file_by_id[id_or_resource]
            stats_path = data_file.get("cs", {}).get("stats_file")
            if stats_path:
                return [(id_or_resource, stats_path)]
            return []

        if id_or_resource in resource_to_data_file_ids:
            paths = []
            for df_id in resource_to_data_file_ids[id_or_resource]:
                data_file = data_file_by_id[df_id]
                stats_path = data_file.get("cs", {}).get("stats_file")
                if stats_path:
                    paths.append((df_id, stats_path))
            return paths

        raise NotFoundException(
            f"'{id_or_resource}' is not a valid data file ID or resource name"
        )

    def get_stats(
        self, id_or_resource: str, format: Literal["tsv", "json"] = "json"
    ) -> str | list[dict]:
        """
        Fetch stats for a data file ID or resource.

        Args:
            id_or_resource: Data file ID (e.g., "finngen_gwas") or resource (e.g., "finngen")
            format: Output format - "tsv" returns raw content, "json" returns parsed list

        Returns:
            TSV string or list of dicts depending on format

        Raises:
            NotFoundException: If not found or no stats configured
        """
        paths = self._get_stats_paths(id_or_resource)

        if not paths:
            raise NotFoundException(
                f"No stats file configured for '{id_or_resource}'"
            )

        if format == "json":
            return self._get_stats_json(paths)
        else:
            return self._get_stats_tsv(paths)

    def _convert_row_types(self, row: dict) -> dict:
        """Convert n_* columns to integers."""
        result = {}
        for key, value in row.items():
            if key.startswith("n_") and value:
                try:
                    result[key] = int(value)
                except ValueError:
                    result[key] = value
            else:
                result[key] = value
        return result

    def _get_stats_json(self, paths: list[tuple[str, str]]) -> list[dict]:
        """Get stats as combined JSON."""
        all_rows = []
        for df_id, path in paths:
            try:
                rows = read_tsv_as_json(path)
                all_rows.extend(self._convert_row_types(row) for row in rows)
            except FileNotFoundError:
                logger.warning(f"Stats file not found for {df_id}: {path}")
            except Exception as e:
                logger.error(f"Error reading stats from {path}: {e}")
                raise
        return all_rows

    def _get_stats_tsv(self, paths: list[tuple[str, str]]) -> str:
        """Get stats as combined TSV (header included once)."""
        header = None
        all_lines = []

        for df_id, path in paths:
            try:
                content = read_file(path)
                lines = content.strip().split("\n")
                if not lines:
                    continue

                if header is None:
                    header = lines[0]
                    all_lines.append(header)

                all_lines.extend(lines[1:])
            except FileNotFoundError:
                logger.warning(f"Stats file not found for {df_id}: {path}")
            except Exception as e:
                logger.error(f"Error reading stats from {path}: {e}")
                raise

        if not all_lines:
            raise NotFoundException("No stats data available")

        return "\n".join(all_lines) + "\n"
