"""Offline pins for the ``X-Columns`` response header on the JSON range path.

The defect this exists for (genetics-results-suite-6uk): a JSON range response is a bare
array, so a no-hit query returns ``[]`` and the schema is gone. A client building a
dataframe from that then raises ColumnNotFound on an ordinary empty result — verified
against ``hla(phenotype=...)`` in the genetics SDK.

The empty case is the one that matters, and it is the one an integration test against
real data would not naturally cover, so these drive ``range_response`` directly with no
server and no credentials.
"""

import asyncio
import json

from app.core.responses import COLUMNS_HEADER, columns_header, range_response
from app.core.streams import tsv_stream_to_list, tsv_stream_to_list_with_header

HEADER = b"#phenotype\tmlog10p\tbeta\tdata_type"
ROW = b"K11_COELIAC\t1596.65\t1.2\tgwas"
SCHEMA = {"phenotype": str, "mlog10p": float, "beta": float, "data_type": str}


async def _aiter(*chunks: bytes):
    for chunk in chunks:
        yield chunk


def _json_response(body: bytes, **kwargs):
    return asyncio.run(range_response("http://t/url", _aiter(body), SCHEMA, "json", 0.0, **kwargs))


def test_empty_json_result_still_advertises_its_columns():
    """The whole point: zero rows, schema intact."""
    resp = _json_response(HEADER + b"\n")
    assert json.loads(bytes(resp.body)) == []
    assert resp.headers[COLUMNS_HEADER] == "phenotype,mlog10p,beta,data_type"


def test_columns_come_from_the_file_header_not_the_schema():
    """``header_schema`` is a validating superset, so it is not a stand-in for the file.

    A schema carrying a column the file does not have must not make the header claim it.
    """
    resp = asyncio.run(
        range_response(
            "http://t/url",
            _aiter(HEADER + b"\n"),
            {**SCHEMA, "not_in_this_file": str},
            "json",
            0.0,
        )
    )
    assert resp.headers[COLUMNS_HEADER] == "phenotype,mlog10p,beta,data_type"


def test_columns_match_the_keys_of_the_returned_rows():
    resp = _json_response(b"\n".join([HEADER, ROW]) + b"\n")
    rows = json.loads(bytes(resp.body))
    assert resp.headers[COLUMNS_HEADER].split(",") == list(rows[0])


def test_row_filters_do_not_change_the_advertised_columns():
    """``data_types`` drops rows, never columns — including down to zero rows."""
    resp = _json_response(b"\n".join([HEADER, ROW]) + b"\n", data_types={"nosuchtype"})
    assert json.loads(bytes(resp.body)) == []
    assert resp.headers[COLUMNS_HEADER] == "phenotype,mlog10p,beta,data_type"


def test_body_is_unchanged_by_the_header():
    """Additive: the browser and the MCP server parse the body, and it is byte-identical."""
    resp = _json_response(b"\n".join([HEADER, ROW]) + b"\n")
    assert bytes(resp.body) == json.dumps(
        [{"phenotype": "K11_COELIAC", "mlog10p": 1596.65, "beta": 1.2, "data_type": "gwas"}],
        separators=(",", ":"),
    ).encode()


def test_tsv_path_sets_no_columns_header():
    """On TSV the header line is already the first line of the body (see 7yg)."""
    resp = asyncio.run(
        range_response("http://t/url", _aiter(HEADER + b"\n"), SCHEMA, "tsv", 0.0)
    )
    assert COLUMNS_HEADER not in resp.headers


def test_columns_header_drops_names_it_cannot_encode():
    """Fail-open: a name that would break the header must not break the request."""
    assert columns_header(["a", "b"]) == {COLUMNS_HEADER: "a,b"}
    assert columns_header([]) == {}
    assert columns_header(["a,b", "c"]) == {}
    assert columns_header(["\N{GREEK SMALL LETTER BETA}eta"]) == {}


def test_columns_header_drops_names_with_cr_or_lf():
    """CR/LF are ASCII, so isascii() alone lets them through — isprintable() must catch them.

    header_schema validation is what actually keeps these out today (only [A-Za-z0-9_.-]+
    reaches this function), but the guard itself must independently enforce its own
    docstring in case that upstream validation is ever loosened.
    """
    assert columns_header(["a\rb"]) == {}
    assert columns_header(["a\nb"]) == {}
    assert columns_header(["a\r\nInjected: header"]) == {}


def test_tsv_stream_to_list_wrapper_is_unchanged():
    """The old helper keeps its exact signature and return value for its other caller."""
    body = b"\n".join([HEADER, ROW]) + b"\n"
    from app.core.streams import tsv_line_iterator_str

    rows = asyncio.run(tsv_stream_to_list(tsv_line_iterator_str(_aiter(body)), SCHEMA))
    header, rows2 = asyncio.run(
        tsv_stream_to_list_with_header(tsv_line_iterator_str(_aiter(body)), SCHEMA)
    )
    assert rows == rows2
    assert header == ["phenotype", "mlog10p", "beta", "data_type"]
