"""
Load dataset and resource definitions from the shared datasets.yaml file.

Reads the YAML config path from DATASETS_CONFIG_PATH env var (default:
./configs/datasets.yaml), selects the active profile from CONFIG_PROFILE,
and exposes the datasets dict in the same structure as the legacy
profiles/*/datasets.py modules.
"""

import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from app.config.profile import CONFIG_PROFILE

logger = logging.getLogger(__name__)

_DATASETS_CONFIG_PATH = os.environ.get(
    "DATASETS_CONFIG_PATH", "./configs/datasets.yaml"
)


@lru_cache(maxsize=1)
def _load_yaml() -> dict[str, Any]:
    path = Path(_DATASETS_CONFIG_PATH)
    if not path.exists():
        raise FileNotFoundError(
            f"datasets.yaml not found at {path.resolve()}. "
            f"Set DATASETS_CONFIG_PATH or run scripts/sync-datasets.sh"
        )
    with open(path) as f:
        data = yaml.safe_load(f)
    logger.info("Loaded datasets config from %s (profile=%s)", path.resolve(), CONFIG_PROFILE)
    return data


def _convert_none_strings(obj: Any) -> Any:
    """YAML loads null as None, but strings like 'null' stay as strings.
    The existing Python code uses Python None. This handles edge cases."""
    if isinstance(obj, dict):
        return {k: _convert_none_strings(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_none_strings(item) for item in obj]
    return obj


def _build_datasets_dict() -> dict[str, dict[str, Any]]:
    """Build the datasets dict from YAML, matching the structure produced by
    the legacy profiles/*/datasets.py modules."""
    raw = _load_yaml()
    profiles = raw.get("profiles", {})
    if CONFIG_PROFILE not in profiles:
        available = ", ".join(sorted(profiles.keys())) or "(none)"
        raise ValueError(
            f"Profile {CONFIG_PROFILE!r} not found in datasets.yaml. "
            f"Available profiles: {available}"
        )

    yaml_datasets = profiles[CONFIG_PROFILE].get("datasets", {})
    result: dict[str, dict[str, Any]] = {}

    for dataset_id, entry in yaml_datasets.items():
        converted = _convert_none_strings(entry)
        # YAML uses None for null values, which matches Python None
        # booleans in YAML (true/false) map correctly to Python True/False
        result[dataset_id] = converted

    return result


# module-level singletons, loaded once on first import
datasets: dict[str, dict[str, Any]] = _build_datasets_dict()
