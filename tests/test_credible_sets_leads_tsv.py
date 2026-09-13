"""Offline pin: the leads endpoint's TSV carries the same columns its JSON does.

The TSV is re-serialized from the reduced rows, so its header is chosen here rather than
copied from the file; choosing the schema instead of the rows' own columns added
`resource`/`version` that every row filled with NA while JSON and X-Columns had neither.
"""

import asyncio
import json
from types import SimpleNamespace

from app.core.responses import COLUMNS_HEADER
from app.routers.credible_sets import credible_sets_by_phenotype_leads

HEADER = ["dataset", "data_type", "trait", "chr", "pos", "pip", "cs_id"]
ROWS = [
    {
        "dataset": "FinnGen_R14",
        "data_type": "GWAS",
        "trait": "K11_COELIAC",
        "chr": 6,
        "pos": 32626272,
        "pip": 0.98,
        "cs_id": "chr6:31126272-34126272_1",
    }
]


class _FakeAccess:
    async def lead_variants_phenotype_with_header(self, *_):
        return HEADER, ROWS


def _call(fmt):
    return asyncio.run(
        credible_sets_by_phenotype_leads(
            request=SimpleNamespace(url="http://t/leads"),
            resource="finngen",
            phenotype_or_study="K11_COELIAC",
            interval=95,
            format=fmt,
            data_access=_FakeAccess(),
        )
    )


async def _body(resp) -> str:
    return b"".join([chunk async for chunk in resp.body_iterator]).decode()


def test_tsv_columns_are_the_ones_json_advertises():
    json_resp = _call("json")
    lines = asyncio.run(_body(_call("tsv"))).splitlines()
    assert lines[0].split("\t") == json_resp.headers[COLUMNS_HEADER].split(",") == HEADER
    assert lines[1].split("\t") == [str(ROWS[0][c]) for c in HEADER]
    assert list(json.loads(bytes(json_resp.body))[0]) == HEADER
