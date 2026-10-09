"""LD from the suite's own matrices, against a local bgzip+tabix fixture.

The files are long-format pair lists indexed on the first variant's position, both
orderings present (app/services/ld_panel_service.py). What is driven here is the whole
read path — the real range engine over the real tabix layout, with the GCS reads replaced by
local file reads — and the router on top of it: the window and threshold filters, the id
spellings in and out, the 404 that names the orientation the panel has, the pair query's
three answers (found, below the floor, not in the panel), the `fields` selection, and the
dispatch between a local panel and the upstream proxy. Offline: nothing touches the network.
"""

import asyncio
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi import FastAPI

from app.config import ld as ld_config
from app.dependencies import get_ld_panel_service, get_ld_service
from app.routers import ld as ld_router
from app.services import gcloud_tabix_base, ld_panel_service, startup_checks
from app.services.gcloud_tabix_base import GCloudTabixBase
from app.services.ld_panel_service import LDPanelService
from app.services.ld_service import LDPairTooFar, LDService, LDVariantNotInPanel
from app.services.tabix_query import parse_tabix_index

PANEL = "test_panel"

# (variant1, variant2, r, r2) for one ordering; the fixture writes both. 22:1000 carries two
# alleles (A>G and A>T) so a point read has a same-position neighbour to leave out, 22:900000
# is far enough from 22:1000 to fall outside a 100 kb window, and X has one pair.
PAIRS = [
    ("chr22_1000_A_G", "chr22_1500_C_T", 0.9, 0.81),
    ("chr22_1000_A_G", "chr22_3000_G_A", -0.5, 0.25),
    ("chr22_1000_A_G", "chr22_900000_T_C", 0.2, 0.04),
    ("chr22_1000_A_T", "chr22_1500_C_T", 0.1, 0.01),
    ("chr22_1500_C_T", "chr22_3000_G_A", 0.3, 0.09),
    ("chrX_5000_A_C", "chrX_6000_G_T", -0.7, 0.49),
]
HEADER = "#chr\tpos\tvariant1\tvariant2\tr\tr2"


def _rows() -> list[tuple[str, int, str, str, float, float]]:
    out = []
    for v1, v2, r, r2 in PAIRS:
        for a, b in ((v1, v2), (v2, v1)):
            chrom, pos = a.split("_")[0].removeprefix("chr"), int(a.split("_")[1])
            out.append((chrom, pos, a, b, r, r2))
    return sorted(out, key=lambda t: (t[0], t[1], t[2], int(t[3].split("_")[1])))


def _write_panel(tmp_path) -> str:
    """One bgzip+tabix file per chromosome, as the bucket holds them."""
    for chrom in ("22", "X"):
        path = tmp_path / f"ld_chr{chrom}.tsv"
        lines = [HEADER] + [
            f"{c}\t{p}\t{a}\t{b}\t{r:.4f}\t{r2:.4f}" for c, p, a, b, r, r2 in _rows() if c == chrom
        ]
        path.write_text("\n".join(lines) + "\n")
        subprocess.run(["bgzip", str(path)], check=True)
        subprocess.run(["tabix", "-s1", "-b2", "-e2", f"{path}.gz"], check=True)
    return str(tmp_path / "ld_chr{chrom}.tsv.gz")


class _LocalPanel(ld_panel_service._LDPanel):
    """The real read path with the index and block reads taken from local disk."""

    async def _get_index(self, file_path):
        with open(file_path + ".tbi", "rb") as fh:
            return parse_tabix_index(fh.read())

    async def _fetch_blocks(self, url, start, last_block_off):
        with open(url, "rb") as fh:
            fh.seek(start)
            return fh.read(last_block_off - start + gcloud_tabix_base._MAX_BGZF_BLOCK)


@pytest.fixture
def panels(tmp_path, monkeypatch):
    pool = ThreadPoolExecutor(1)
    monkeypatch.setattr(gcloud_tabix_base, "_get_filter_pool", lambda: pool)
    monkeypatch.setattr(gcloud_tabix_base, "_files_semaphore", None)
    monkeypatch.setattr(gcloud_tabix_base, "ensure_gcs_token", lambda: None)
    monkeypatch.setattr(GCloudTabixBase, "TBI_CACHE_ROOT", str(tmp_path / "tbi"))
    monkeypatch.setattr(ld_panel_service, "_LDPanel", _LocalPanel)
    monkeypatch.setattr(
        ld_config,
        "ld_panels",
        [
            {
                "panel": PANEL,
                "data_source": "gcloud",
                "file_template": _write_panel(tmp_path),
                "chromosomes": ["22", "X"],
                "fields": ("r", "r2"),
                "r2_floor": 0.01,
                "max_distance": 10_000,
                "n_samples": 7,
                "description": "fixture",
            }
        ],
    )
    service = LDPanelService()
    asyncio.run(service.warm_all())
    try:
        yield service
    finally:
        pool.shutdown()


def scan(service, variant, window=2_000_000, r2_threshold=0.0):
    return asyncio.run(
        service.variants_in_ld(variant, window=window, r2_threshold=r2_threshold, panel=PANEL)
    )


def pair(service, variant, other, r2_threshold=0.0):
    return asyncio.run(service.pair(variant, other, r2_threshold=r2_threshold, panel=PANEL))


# ------------------------------------------------------------------ the service: one variant


def test_every_partner_comes_back_in_position_order_with_the_panels_statistics(panels):
    entries = scan(panels, "22:1000:A:G")
    assert [e["variation2"] for e in entries] == ["22:1500:C:T", "22:3000:G:A", "22:900000:T:C"]
    assert entries[0] == {"variation1": "22:1000:A:G", "variation2": "22:1500:C:T", "r": 0.9, "r2": 0.81}
    assert entries[1]["r"] == -0.5, "the sign survives the round trip"


def test_the_query_is_echoed_as_the_caller_spelled_it_and_partners_as_the_upstream_spells_them(panels):
    entries = scan(panels, "chr22:1000:a:g")
    assert {e["variation1"] for e in entries} == {"chr22:1000:a:g"}
    assert entries[0]["variation2"] == "22:1500:C:T"


def test_the_window_is_a_total_span_centred_on_the_query(panels):
    # 22:900000 is 899 kb away: inside a 2 Mb window (1 Mb each side), outside a 100 kb one
    assert len(scan(panels, "22:1000:A:G", window=2_000_000)) == 3
    assert len(scan(panels, "22:1000:A:G", window=100_000)) == 2


def test_the_threshold_filters_on_r2(panels):
    entries = scan(panels, "22:1000:A:G", r2_threshold=0.5)
    assert [e["variation2"] for e in entries] == ["22:1500:C:T"]
    assert scan(panels, "22:1000:A:G", r2_threshold=0.9) == []


def test_a_same_position_allele_gets_only_its_own_rows(panels):
    entries = scan(panels, "22:1000:A:T")
    assert [(e["variation2"], e["r2"]) for e in entries] == [("22:1500:C:T", 0.01)]


def test_both_orderings_are_served_so_a_partner_is_itself_a_query(panels):
    entries = scan(panels, "22:1500:C:T")
    assert [(e["variation2"], e["r"]) for e in entries] == [
        ("22:1000:A:G", 0.9),
        ("22:1000:A:T", 0.1),
        ("22:3000:G:A", 0.3),
    ]


def test_a_swapped_orientation_is_404_naming_the_ids_the_panel_has_at_that_position(panels):
    """Matched as the caller oriented it, as the upstream does: the sign of r depends on
    it, so the panel's spelling is named rather than a negated r returned."""
    with pytest.raises(LDVariantNotInPanel) as caught:
        scan(panels, "22:1000:G:A")
    assert str(caught.value) == (
        f"22:1000:G:A is not in LD panel {PANEL}; at that position the panel has "
        "22:1000:A:G, 22:1000:A:T"
    )


def test_a_variant_with_no_rows_is_404_and_says_what_absence_can_mean(panels):
    with pytest.raises(LDVariantNotInPanel) as caught:
        scan(panels, "22:7000:G:C")
    assert "22:7000:G:C is not in LD panel" in str(caught.value)
    assert "no partner at r2 >= 0.01 within 10,000 bp" in str(caught.value)


@pytest.mark.parametrize("spelling", ["X:5000:A:C", "chrX:5000:A:C", "23:5000:A:C"])
def test_the_x_chromosome_is_found_under_every_spelling_and_answered_as_x(panels, spelling):
    entries = scan(panels, spelling)
    assert [(e["variation1"], e["variation2"], e["r"]) for e in entries] == [
        (spelling, "X:6000:G:T", -0.7)
    ]


def test_a_chromosome_the_panel_has_no_file_for_is_not_in_the_panel(panels):
    with pytest.raises(LDVariantNotInPanel):
        scan(panels, "21:1000:A:G")


# ----------------------------------------------------------------------- the service: a pair


def test_a_pair_is_one_entry_in_either_order(panels):
    assert pair(panels, "22:1000:A:G", "22:1500:C:T") == [
        {"variation1": "22:1000:A:G", "variation2": "22:1500:C:T", "r": 0.9, "r2": 0.81}
    ]
    assert pair(panels, "22:1500:C:T", "22:1000:A:G")[0]["r"] == 0.9
    assert pair(panels, "chr22:3000:G:A", "22:1000:A:G")[0] == {
        "variation1": "chr22:3000:G:A", "variation2": "22:1000:A:G", "r": -0.5, "r2": 0.25,
    }


def test_a_pair_below_the_floor_is_an_empty_answer_because_both_variants_were_found(panels):
    # the two alleles at 22:1000 are both in the panel and share no row
    assert pair(panels, "22:1000:A:G", "22:1000:A:T") == []


def test_a_pair_below_the_threshold_is_an_empty_answer(panels):
    assert pair(panels, "22:1000:A:G", "22:3000:G:A", r2_threshold=0.3) == []


@pytest.mark.parametrize(
    "first,second,missing",
    [
        ("22:1000:A:G", "22:7000:G:C", "22:7000:G:C"),
        ("22:7000:G:C", "22:1000:A:G", "22:7000:G:C"),
        ("22:1000:A:G", "22:1500:T:C", "22:1500:T:C"),
    ],
)
def test_a_pair_with_a_variant_the_panel_lacks_is_404_naming_that_variant(panels, first, second, missing):
    """The other variant's block is read for exactly this: without it, "not in the panel"
    and "below the floor" would both be an empty list."""
    with pytest.raises(LDVariantNotInPanel) as caught:
        pair(panels, first, second)
    assert str(caught.value).startswith(f"{missing} is not in LD panel")


def test_a_variant_against_itself_is_one_by_definition(panels):
    assert pair(panels, "22:1000:A:G", "chr22:1000:A:G") == [
        {"variation1": "22:1000:A:G", "variation2": "22:1000:A:G", "r": 1.0, "r2": 1.0}
    ]
    with pytest.raises(LDVariantNotInPanel):
        pair(panels, "22:7000:G:C", "22:7000:G:C")


# ------------------------------------------------------------------------------ the router


class StubUpstream:
    def __init__(self, entries=None):
        self.entries = entries if entries is not None else []
        self.calls = []

    async def variants_in_ld(self, variant, *, window, r2_threshold, panel):
        self.calls.append(("scan", variant, window, r2_threshold, panel))
        return self.entries

    async def pair(self, variant, other, *, r2_threshold, panel):
        self.calls.append(("pair", variant, other, r2_threshold, panel))
        return self.entries


def call(panel_service, upstream, path, query=""):
    api = FastAPI()
    api.include_router(ld_router.router, prefix="/api/v1")
    api.dependency_overrides[get_ld_panel_service] = lambda: panel_service
    api.dependency_overrides[get_ld_service] = lambda: upstream

    messages, delivered = [], []

    async def receive():
        if not delivered:
            delivered.append(True)
            return {"type": "http.request", "body": b"", "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message):
        messages.append(message)

    asyncio.run(
        api(
            {
                "type": "http",
                "asgi": {"version": "3.0", "spec_version": "2.3"},
                "http_version": "1.1",
                "method": "GET",
                "scheme": "http",
                "path": path,
                "raw_path": path.encode(),
                "query_string": query.encode(),
                "root_path": "",
                "headers": [],
                "client": ("testclient", 50000),
                "server": ("testserver", 80),
                "state": {},
            },
            receive,
            send,
        )
    )
    start = next(m for m in messages if m["type"] == "http.response.start")
    body = b"".join(bytes(m.get("body", b"")) for m in messages if m["type"] == "http.response.body")
    return start["status"], json.loads(body)


def test_the_route_serves_a_local_panel_and_states_its_floor_and_cap(panels):
    upstream = StubUpstream()
    status, body = call(panels, upstream, "/api/v1/ld/22:1000:A:G", f"panel={PANEL}&r2_threshold=0")
    assert status == 200
    assert body["panel"] == PANEL
    assert body["fields"] == ["r", "r2"]
    assert body["r2_floor"] == 0.01
    assert body["max_distance"] == 10_000
    assert [e["variation2"] for e in body["ld"]] == ["22:1500:C:T", "22:3000:G:A"]
    assert body["window"] == ld_config.ld_default_window
    assert upstream.calls == [], "a local panel must not reach the upstream"


def test_any_other_panel_name_goes_to_the_upstream_with_its_own_fields(panels):
    upstream = StubUpstream(
        [{"variation1": "22:1000:A:G", "variation2": "22:1500:C:T", "r2": 0.8, "d_prime": 0.9}]
    )
    status, body = call(panels, upstream, "/api/v1/ld/22:1000:A:G", "panel=sisu42")
    assert status == 200
    assert upstream.calls == [("scan", "22:1000:A:G", ld_config.ld_default_window, 0.6, "sisu42")]
    assert body["fields"] == ["r2", "d_prime"]
    assert body["r2_floor"] is None and body["max_distance"] is None
    assert body["ld"] == [
        {"variation1": "22:1000:A:G", "variation2": "22:1500:C:T", "r2": 0.8, "d_prime": 0.9}
    ]


def test_fields_selects_a_subset_in_the_order_asked_without_duplicates(panels):
    status, body = call(
        panels, StubUpstream(), "/api/v1/ld/22:1000:A:G", f"panel={PANEL}&r2_threshold=0&fields=r2,r,r2"
    )
    assert status == 200
    assert body["fields"] == ["r2", "r"]
    assert body["ld"][0] == {"variation1": "22:1000:A:G", "variation2": "22:1500:C:T", "r2": 0.81, "r": 0.9}

    status, body = call(panels, StubUpstream(), "/api/v1/ld/22:1000:A:G", f"panel={PANEL}&fields=r")
    assert set(body["ld"][0]) == {"variation1", "variation2", "r"}


@pytest.mark.parametrize(
    "fields,detail",
    [
        ("d_prime", f"LD panel {PANEL} does not carry d_prime; it carries r, r2"),
        ("r,d_prime", "does not carry d_prime"),
        ("rsq", "Unknown LD field 'rsq'"),
        ("", "fields must name at least one of"),
        (",", "fields must name at least one of"),
    ],
)
def test_a_field_the_panel_lacks_or_nobody_has_is_422_naming_what_there_is(panels, fields, detail):
    status, body = call(panels, StubUpstream(), "/api/v1/ld/22:1000:A:G", f"panel={PANEL}&fields={fields}")
    assert status == 422
    assert detail in body["detail"]


def test_asking_the_upstream_for_signed_r_is_422_rather_than_a_null(panels):
    upstream = StubUpstream()
    status, body = call(panels, upstream, "/api/v1/ld/22:1000:A:G", "panel=sisu42&fields=r")
    assert status == 422
    assert "LD panel sisu42 does not carry r; it carries r2, d_prime" == body["detail"]
    assert upstream.calls == []


def test_the_router_turns_a_missing_variant_into_404(panels):
    status, body = call(panels, StubUpstream(), "/api/v1/ld/22:1000:G:A", f"panel={PANEL}")
    assert status == 404
    assert "22:1000:A:G" in body["detail"]


@pytest.mark.parametrize("variant", ["Y:1000:A:G", "MT:1000:A:G", "22:1000:N:A"])
def test_a_variant_the_panel_engine_cannot_parse_is_422_on_a_local_panel(panels, variant):
    status, _body = call(panels, StubUpstream(), f"/api/v1/ld/{variant}", f"panel={PANEL}")
    assert status == 422


def test_the_pair_route_answers_found_below_floor_and_missing(panels):
    status, body = call(panels, StubUpstream(), "/api/v1/ld/22:1000:A:G/22:1500:C:T", f"panel={PANEL}")
    assert status == 200
    assert body["variant"] == "22:1000:A:G" and body["other"] == "22:1500:C:T"
    assert body["r2_threshold"] == 0.0
    assert body["ld"] == [{"variation1": "22:1000:A:G", "variation2": "22:1500:C:T", "r": 0.9, "r2": 0.81}]

    status, body = call(panels, StubUpstream(), "/api/v1/ld/22:1000:A:G/22:1000:A:T", f"panel={PANEL}&fields=r")
    assert status == 200 and body["ld"] == [] and body["r2_floor"] == 0.01

    status, body = call(panels, StubUpstream(), "/api/v1/ld/22:1000:A:G/22:7000:G:C", f"panel={PANEL}")
    assert status == 404 and body["detail"].startswith("22:7000:G:C is not in LD panel")


def test_the_pair_route_refuses_what_the_panel_cannot_hold_before_reading(panels):
    status, body = call(panels, StubUpstream(), "/api/v1/ld/22:1000:A:G/X:5000:A:C", f"panel={PANEL}")
    assert status == 422 and "different chromosomes" in body["detail"]

    status, body = call(panels, StubUpstream(), "/api/v1/ld/22:1000:A:G/22:900000:T:C", f"panel={PANEL}")
    assert status == 422
    assert "899,000 bp apart" in body["detail"] and "up to 10,000 bp" in body["detail"]


def test_the_pair_route_relays_an_upstream_panel(panels):
    upstream = StubUpstream(
        [{"variation1": "22:1000:A:G", "variation2": "22:1500:C:T", "r2": 0.8, "d_prime": 0.9}]
    )
    status, body = call(panels, upstream, "/api/v1/ld/22:1000:A:G/22:1500:C:T", "panel=sisu42&r2_threshold=0.2&fields=r2")
    assert status == 200
    assert upstream.calls == [("pair", "22:1000:A:G", "22:1500:C:T", 0.2, "sisu42")]
    assert body["ld"] == [{"variation1": "22:1000:A:G", "variation2": "22:1500:C:T", "r2": 0.8}]


def test_panels_lists_the_local_panels_with_their_semantics(panels):
    status, body = call(panels, StubUpstream(), "/api/v1/ld/panels")
    assert status == 200
    assert body["panels"] == [
        {
            "panel": PANEL,
            "fields": ["r", "r2"],
            "r2_floor": 0.01,
            "max_distance": 10_000,
            "n_samples": 7,
            "description": "fixture",
        }
    ]
    assert body["default_panel"] == ld_config.ld_default_panel
    assert body["upstream_fields"] == ["r2", "d_prime"]


# ------------------------------------------------------------- the upstream's pair query


class _FakeUpstream(LDService):
    def __init__(self, entries):
        self.entries = entries
        self.calls = []

    async def variants_in_ld(self, variant, *, window, r2_threshold, panel):
        self.calls.append((variant, window, r2_threshold, panel))
        return self.entries


def test_the_upstream_pair_is_a_window_that_just_reaches_the_other_variant():
    far = {"variation1": "22:1000:A:G", "variation2": "22:200000:T:C", "r2": 0.1, "d_prime": 0.2}
    hit = {"variation1": "22:1000:A:G", "variation2": "22:150000:C:T", "r2": 0.7, "d_prime": 0.9}
    upstream = _FakeUpstream([far, hit])
    got = asyncio.run(upstream.pair("22:1000:A:G", "chr22:150000:C:T", r2_threshold=0.2, panel="sisu42"))
    assert got == [hit]
    assert upstream.calls == [("22:1000:A:G", 2 * 149_000 + 10_000, 0.2, "sisu42")]

    # never narrower than the upstream's minimum window
    upstream = _FakeUpstream([])
    asyncio.run(upstream.pair("22:1000:A:G", "22:1500:C:T", r2_threshold=0.0, panel="sisu42"))
    assert upstream.calls[0][1] == ld_config.ld_min_window


def test_the_upstream_pair_refuses_a_distance_its_window_cannot_reach():
    with pytest.raises(LDPairTooFar) as caught:
        asyncio.run(
            _FakeUpstream([]).pair("22:1000:A:G", "22:4000000:C:T", r2_threshold=0.0, panel="sisu42")
        )
    assert "3,999,000 bp apart" in str(caught.value)


def test_the_upstream_pair_answers_identity_after_proving_membership():
    upstream = _FakeUpstream([])
    got = asyncio.run(upstream.pair("22:1000:A:G", "22:1000:A:G", r2_threshold=0.0, panel="sisu42"))
    assert got == [{"variation1": "22:1000:A:G", "variation2": "22:1000:A:G", "r2": 1.0, "d_prime": 1.0}]
    assert upstream.calls[0][2] == 1.0


# -------------------------------------------------------------------------- startup check


def test_startup_holds_every_panel_file_to_its_declared_header(tmp_path, monkeypatch):
    monkeypatch.setattr(gcloud_tabix_base, "ensure_gcs_token", lambda: None)
    monkeypatch.setattr(GCloudTabixBase, "TBI_CACHE_ROOT", str(tmp_path / "tbi"))
    template = _write_panel(tmp_path)
    cfg = {
        "panel": PANEL, "data_source": "gcloud", "file_template": template,
        "chromosomes": ["22", "X"], "fields": ("r", "r2"), "r2_floor": 0.01, "max_distance": 1,
    }
    monkeypatch.setattr(ld_config, "ld_panels", [cfg])
    checks = [c for c in startup_checks._collect_tabix_files() if c[0].startswith("ld:")]
    assert [(label, path) for label, path, _ in checks] == [
        (f"ld:{PANEL}:22", template.format(chrom="22")),
        (f"ld:{PANEL}:X", template.format(chrom="X")),
    ]
    tabix = GCloudTabixBase()
    for label, path, expected in checks:
        assert expected == [b"chr", b"pos", b"variant1", b"variant2", b"r", b"r2"]
        assert startup_checks._check_tabix_header(tabix, label, path, expected) is None
    # a panel declaring a statistic the files lack is refused at startup, not served as nulls
    assert startup_checks._check_tabix_header(
        tabix, "ld:x", template.format(chrom="22"), ld_config.panel_header({**cfg, "fields": ("r", "r2", "d_prime")})
    )
