"""Offline tests of GET /api/v1/gene_summary/{gene}: the handler is invoked directly and the
GCS read is replaced by a dict."""

import asyncio
import json
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException

import app.config.gene_summary as config
from app import server
from app.core.exceptions import GeneNotFoundException, NotFoundException
from app.routers.gene_summary import gene_summary
from app.services import startup_checks
from app.services.gene_summary_access import GeneSummaryAccess

PREFIX = "gs://test-bucket/gene_summaries/r1/"
SUMMARY = {"gene": "NOD2", "headline": "a headline", "findings": [], "gaps": []}
REQUEST = SimpleNamespace(url="http://test/api/v1/gene_summary/x")


class Mapping:
    def known_gene_name(self, name: str) -> str:
        known = {"nod2": "NOD2", "pcsk9": "PCSK9"}
        if name.lower() not in known:
            raise GeneNotFoundException(f"Gene {name} not found")
        return known[name.lower()]


@pytest.fixture
def access(monkeypatch):
    objects = {PREFIX + "NOD2.json": json.dumps(SUMMARY).encode()}
    asked: list[str] = []

    async def fetch(self, path: str) -> bytes:
        asked.append(path)
        if path not in objects:
            raise NotFoundException(f"File not found: {path}")
        return objects[path]

    monkeypatch.setattr(config, "prefix", PREFIX)
    monkeypatch.setattr(GeneSummaryAccess, "__init__", lambda self: setattr(self, "prefix", PREFIX))
    monkeypatch.setattr(GeneSummaryAccess, "_fetch_full", fetch)
    a = GeneSummaryAccess()
    a.asked = asked
    return a


def call(gene: str, access: GeneSummaryAccess):
    return asyncio.run(gene_summary(REQUEST, gene, access, Mapping()))


def test_serves_the_gene_s_object_under_the_mapping_s_spelling(access):
    response = call("nod2", access)
    assert json.loads(response.body) == SUMMARY
    assert access.asked == [PREFIX + "NOD2.json"]


def test_a_gene_without_a_summary_is_a_404(access):
    with pytest.raises(HTTPException) as e:
        call("PCSK9", access)
    assert e.value.status_code == 404


def test_an_unknown_name_is_a_404_and_reads_nothing(access):
    with pytest.raises(HTTPException) as e:
        call("../../other-folder/x", access)
    assert e.value.status_code == 404
    assert access.asked == []


def test_a_failed_read_is_a_500_not_a_missing_summary(access, monkeypatch):
    async def fail(self, path: str) -> bytes:
        raise RuntimeError("Failed to fetch")

    monkeypatch.setattr(GeneSummaryAccess, "_fetch_full", fail)
    with pytest.raises(HTTPException) as e:
        call("NOD2", access)
    assert e.value.status_code == 500


def test_the_route_is_registered_only_where_a_release_is_named(monkeypatch):
    def paths(prefix):
        monkeypatch.setattr(config, "prefix", prefix)
        app = FastAPI()
        assert server.include_gene_summary(app) is (prefix is not None)
        return [r.path for r in app.routes]

    assert "/api/v1/gene_summary/{gene}" in paths(PREFIX)
    assert "/api/v1/gene_summary/{gene}" not in paths(None)


def test_startup_checks_the_manifest_of_a_named_release(monkeypatch):
    def checked(prefix):
        monkeypatch.setattr(config, "prefix", prefix)
        return dict(startup_checks._collect_mapping_files()).get("gene_summary:manifest")

    assert checked(PREFIX) == PREFIX + "manifest.json"
    assert checked(None) is None
