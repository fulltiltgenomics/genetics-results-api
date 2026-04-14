"""
Aggregate sample-size statistics for datasets in the registry.

For each dataset with a metadata_file, we load the harmonized metadata once
and compute summary stats (phenotype count, sample-size median, case/control
ranges). Results are cached in-memory for the lifetime of the process.
"""

import logging
import statistics
from typing import Any

from app.config.datasets import datasets as _registry
from app.services.data_access import DataAccess

logger = logging.getLogger(__name__)

_stats_cache: dict[str, dict[str, Any]] = {}
_misses: set[str] = set()


def _as_int_list(values: list) -> list[int]:
    """Filter to integer values only (ignoring 'NA' / None / strings)."""
    out = []
    for v in values:
        if isinstance(v, int):
            out.append(v)
        elif isinstance(v, str):
            try:
                out.append(int(v))
            except (ValueError, TypeError):
                continue
    return out


def _compute_for_rows(rows: list[dict], collection: bool) -> dict[str, Any]:
    """Compute aggregate stats from harmonized metadata rows."""
    n_samples = _as_int_list([r.get("n_samples") for r in rows])
    # exclude zeros: 0 cases/controls means quantitative trait, not a real count
    n_cases = [v for v in _as_int_list([r.get("n_cases") for r in rows]) if v > 0]
    n_controls = [v for v in _as_int_list([r.get("n_controls") for r in rows]) if v > 0]

    stats: dict[str, Any] = {}
    count_key = "n_subdatasets" if collection else "n_phenotypes"
    stats[count_key] = len(rows)

    if n_samples:
        stats["n_samples_median"] = int(statistics.median(n_samples))
        stats["n_samples_range"] = [min(n_samples), max(n_samples)]
    if n_cases:
        stats["n_cases_range"] = [min(n_cases), max(n_cases)]
    if n_controls:
        stats["n_controls_range"] = [min(n_controls), max(n_controls)]

    return stats


def get_dataset_stats(
    dataset_id: str, data_access: DataAccess
) -> dict[str, Any] | None:
    """Get aggregate stats for a dataset_id, or None if the dataset has no
    metadata file or loading failed."""
    if dataset_id in _stats_cache:
        return _stats_cache[dataset_id]
    if dataset_id in _misses:
        return None

    entry = _registry.get(dataset_id)
    if not entry or not entry.get("metadata_file"):
        _misses.add(dataset_id)
        return None

    resource = entry.get("resource", dataset_id)
    try:
        rows = data_access.get_harmonized_metadata(resource)
    except Exception as e:
        logger.warning(f"Could not load metadata for {dataset_id}: {e}")
        _misses.add(dataset_id)
        return None

    # for a resource with multiple datasets sharing the same metadata file
    # (e.g. finngen + kanta + drugs), get_harmonized_metadata already merges
    # them. That's fine for our aggregate — the rows reflect the combined
    # set of phenotypes exposed under this resource.
    if not rows:
        _misses.add(dataset_id)
        return None

    collection = bool(entry.get("collection"))
    stats = _compute_for_rows(rows, collection)
    _stats_cache[dataset_id] = stats
    return stats


def clear_cache() -> None:
    """Clear the stats cache (useful for tests)."""
    _stats_cache.clear()
    _misses.clear()
