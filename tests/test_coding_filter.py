"""Unit tests for the coding-only variant filter.

The filter restricts credible-set rows to coding variants using the inline
``most_severe`` consequence column, reusing the shared ``coding_set`` definition
in ``app.config.common``. These are self-contained (no live server): they drive
the stream/list helpers and ``range_response`` directly.
"""

import asyncio
import json

from app.config.common import coding_set
from app.core.responses import range_response
from app.core.streams import filter_stream_by_coding, filter_coding_rows

# header uses names the filter resolves by column name, not a fixed index
HEADER = b"dataset\tpip\tmost_severe\tgene_most_severe"
CODING_ROW = b"FinnGen_R13\t0.9\tmissense_variant\tPCSK9"
STOP_ROW = b"FinnGen_R13\t0.8\tstop_gained\tLDLR"
INTRON_ROW = b"FinnGen_R13\t0.7\tintron_variant\tLINC01141"
NA_ROW = b"FinnGen_R13\t0.6\tNA\tNA"


async def _aiter(*chunks: bytes):
    for chunk in chunks:
        yield chunk


def _run_stream(stream):
    async def run():
        return b"".join([c async for c in stream])

    return asyncio.run(run())


def test_stream_keeps_only_coding_rows():
    body = b"\n".join([HEADER, CODING_ROW, INTRON_ROW, STOP_ROW]) + b"\n"
    out = _run_stream(filter_stream_by_coding(_aiter(body), coding_set))
    lines = out.strip().split(b"\n")
    # header is always preserved
    assert lines[0] == HEADER
    kept = lines[1:]
    assert CODING_ROW in kept
    assert STOP_ROW in kept
    assert INTRON_ROW not in kept  # intron_variant is not coding


def test_stream_excludes_missing_most_severe():
    body = b"\n".join([HEADER, CODING_ROW, NA_ROW]) + b"\n"
    out = _run_stream(filter_stream_by_coding(_aiter(body), coding_set))
    kept = out.strip().split(b"\n")[1:]
    assert kept == [CODING_ROW]  # NA row dropped


def test_stream_handles_chunk_boundaries():
    # split the payload mid-line to exercise the incomplete-line buffer
    body = b"\n".join([HEADER, CODING_ROW, INTRON_ROW]) + b"\n"
    split = len(HEADER) + 5
    out = _run_stream(filter_stream_by_coding(_aiter(body[:split], body[split:]), coding_set))
    lines = out.strip().split(b"\n")
    assert lines[0] == HEADER
    assert lines[1:] == [CODING_ROW]


def test_stream_passthrough_when_no_most_severe_column():
    header = b"dataset\tpip"
    row = b"FinnGen_R13\t0.9"
    body = b"\n".join([header, row]) + b"\n"
    out = _run_stream(filter_stream_by_coding(_aiter(body), coding_set))
    assert out.strip().split(b"\n") == [header, row]


def test_filter_coding_rows_keeps_coding_and_drops_noncoding():
    rows = [
        {"pip": 0.9, "most_severe": "missense_variant"},
        {"pip": 0.7, "most_severe": "intron_variant"},
        {"pip": 0.8, "most_severe": "splice_donor_variant"},
    ]
    kept = filter_coding_rows(rows, coding_set)
    assert [r["most_severe"] for r in kept] == ["missense_variant", "splice_donor_variant"]


def test_filter_coding_rows_excludes_none_and_missing():
    rows = [
        {"pip": 0.9, "most_severe": "frameshift_variant"},
        {"pip": 0.6, "most_severe": None},  # NA parsed to None upstream
        {"pip": 0.5},  # most_severe key absent entirely
    ]
    kept = filter_coding_rows(rows, coding_set)
    assert kept == [{"pip": 0.9, "most_severe": "frameshift_variant"}]


# --- range_response wiring (JSON path) ---------------------------------------

_SCHEMA = {"dataset": str, "pip": float, "most_severe": str, "gene_most_severe": str}


def _json_rows(coding_only: bool):
    body = b"\n".join([HEADER, CODING_ROW, INTRON_ROW, STOP_ROW, NA_ROW]) + b"\n"
    resp = asyncio.run(
        range_response("http://t/url", _aiter(body), _SCHEMA, "json", 0.0, coding_only)
    )
    return json.loads(bytes(resp.body))


def test_range_response_json_coding_only_true_filters():
    rows = _json_rows(coding_only=True)
    consequences = {r["most_severe"] for r in rows}
    assert consequences == {"missense_variant", "stop_gained"}


def test_range_response_json_coding_only_false_is_unchanged():
    rows = _json_rows(coding_only=False)
    # default behavior returns every row, including non-coding and NA (->None)
    assert len(rows) == 4
    assert any(r["most_severe"] == "intron_variant" for r in rows)
    assert any(r["most_severe"] is None for r in rows)
