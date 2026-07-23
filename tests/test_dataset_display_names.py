"""
Unit tests for the dataset display-name override config and endpoint.

Self-contained: exercises the config map and the router coroutine directly,
so they need neither a live server nor GCS access.
"""

import asyncio
import json

from app.config import common as common_config
from app.routers.datasets import get_dataset_display_names


def test_config_exposes_display_name_overrides():
    overrides = common_config.dataset_display_names
    assert isinstance(overrides, dict)
    # the motivating override: UKB_PPP humanizes to "UKB PPP", hiding that it is
    # only the Olink 3K panel (see genetics-results-api-4zg)
    assert overrides.get("UKB_PPP") == "UKBB PPP (Olink 3K)"


def test_override_keys_are_raw_dataset_column_values():
    # overrides must be keyed by the same raw `dataset` column value space as
    # dataset_to_resource so a frontend can join on the value it receives in rows
    for key in common_config.dataset_display_names:
        assert key in common_config.dataset_to_resource


def test_endpoint_returns_override_map():
    response = asyncio.run(get_dataset_display_names())
    payload = json.loads(response.body)
    assert payload == common_config.dataset_display_names
    assert payload["UKB_PPP"] == "UKBB PPP (Olink 3K)"
