"""The /datasets catalogue emits `cohorts` only for registry entries that carry it."""

from app.routers import datasets as datasets_router


def _catalogue(monkeypatch, registry):
    monkeypatch.setattr(datasets_router.config_util, "get_datasets", lambda: registry)
    monkeypatch.setattr(datasets_router.config_util, "dataset_products", lambda _id: {})
    monkeypatch.setattr(datasets_router.config_util, "is_on_request", lambda _id: False)
    items = datasets_router._catalogue(None, None, False, False, None)
    return {i["dataset_id"]: i for i in items}


def test_cohorts_emitted_when_present_and_absent_otherwise(monkeypatch):
    items = _catalogue(
        monkeypatch,
        {
            "with_cohorts": {"resource": "r", "data_type": "gwas", "cohorts": ["finngen", "ukb"]},
            "without": {"resource": "r", "data_type": "gwas"},
            "empty": {"resource": "r", "data_type": "gwas", "cohorts": []},
        },
    )
    assert items["with_cohorts"]["cohorts"] == ["finngen", "ukb"]
    assert "cohorts" not in items["without"]
    assert "cohorts" not in items["empty"]
