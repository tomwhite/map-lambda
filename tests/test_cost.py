from itertools import pairwise

import pytest

from map_lambda.cost import (
    GB_SECOND_USD,
    REQUEST_USD,
    S3_GET_USD,
    S3_LIST_USD,
    SLICE_RECORDS,
    estimate_cost,
    fetch_reports,
    query_windows,
)


class Logs:
    """Logs Insights over `lines`, one REPORT line per request id at a time."""

    def __init__(self, lines: dict[str, tuple[float, dict[str, str]]]):
        self.lines = lines
        self.queries: list[tuple[int, int]] = []

    def start_query(self, *, logGroupName, startTime, endTime, queryString):
        self.queries.append((startTime, endTime))
        return {"queryId": str(len(self.queries) - 1)}

    def get_query_results(self, *, queryId):
        start, end = self.queries[int(queryId)]
        results = [
            [{"field": "@requestId", "value": req_id}]
            + [{"field": k, "value": v} for k, v in fields.items()]
            + [{"field": "@ptr", "value": "x"}]
            for req_id, (at, fields) in self.lines.items()
            if start <= at < end
        ]
        return {"status": "Complete", "results": results}


WARM = {
    "@duration": "120.5",
    "@billedDuration": "121",
    "@memorySize": "1769000000",
    "@maxMemoryUsed": "200000000",
}


def test_windows_cover_the_run_a_slice_of_invocations_each():
    records = [{"invoke_start": 1000 + i, "invoke_end": 1001 + i} for i in range(SLICE_RECORDS + 1)]

    windows = query_windows(records)

    assert len(windows) == 2
    assert windows[0][0] == 940 and windows[-1][1] == 1001 + SLICE_RECORDS + 60
    assert all(a[1] == b[0] for a, b in pairwise(windows))


def test_records_are_joined_to_their_report_lines():
    logs = Logs({"a": (1001, WARM), "b": (1002, {**WARM, "@initDuration": "80"}), "c": (1, WARM)})
    records = [
        {"req_id": "a", "invoke_start": 1000, "invoke_end": 1001},
        {"req_id": "b", "invoke_start": 1000, "invoke_end": 1002},
    ]

    joined = fetch_reports(records, "fragment", logs=logs)

    assert [j["req_id"] for j in joined] == ["a", "b"]
    assert joined[0]["execution_ms"] == 120.5
    assert joined[0]["memory_size_mb"] == 1769
    assert joined[0]["billed_ms"] == 121
    assert (joined[0]["init_ms"], joined[1]["init_ms"]) == (None, 80)


def test_dispatchers_are_joined_to_their_report_lines():
    logs = Logs({"d": (1003, WARM)})
    dispatchers = [{"req_id": "d", "call": 0, "call_start": 1000.5, "elapsed_ms": 2500.0}]

    joined = fetch_reports(dispatchers, "dispatcher", logs=logs)

    assert [j["req_id"] for j in joined] == ["d"]
    assert logs.queries == [(940, 1063)]


def test_finding_report_lines_stops_when_a_query_finds_no_more(monkeypatch):
    monkeypatch.setattr("map_lambda.cost.time.sleep", lambda s: None)
    logs = Logs({"a": (1001, WARM)})
    records = [
        {"req_id": "a", "invoke_start": 1000, "invoke_end": 1001},
        {"req_id": "never", "invoke_start": 1000, "invoke_end": 1001},
    ]
    progress: list[str] = []

    joined = fetch_reports(records, "fragment", logs=logs, on_progress=progress.append)

    assert [j["req_id"] for j in joined] == ["a"]
    assert len(logs.queries) == 2
    assert progress[-1] == "fragment: 1 of 2 REPORT lines"


def test_no_records_need_no_query():
    assert fetch_reports([], "fragment", logs=Logs({})) == []


def test_estimate_cost():
    records = [
        {"billed_ms": 1000, "memory_size_mb": 1024},
        {"billed_ms": 3000, "memory_size_mb": 1024},
    ]

    cost = estimate_cost(records, missing=1)

    assert (cost.invocations, cost.missing) == (2, 1)
    assert cost.gb_seconds == pytest.approx(4.0)
    assert cost.total_usd == pytest.approx(4.0 * GB_SECOND_USD + 2 * REQUEST_USD)
    assert "1 invocations had no REPORT line" in str(cost)


def test_s3_requests_not_counted_are_not_shown_as_none():
    uncounted = estimate_cost([{"billed_ms": 1000, "memory_size_mb": 1024}])
    none_made = estimate_cost([{"billed_ms": 1000, "memory_size_mb": 1024, "s3_gets": 0}])

    assert (uncounted.s3_gets, uncounted.s3_lists) == (None, None)
    assert "S3      not counted" in str(uncounted)
    assert (none_made.s3_gets, none_made.s3_lists) == (0, None)
    assert "(0 GETs, 0 LISTs)" in str(none_made)


def test_estimate_cost_with_s3_requests():
    records = [
        {"billed_ms": 1000, "memory_size_mb": 512, "s3_gets": 3, "s3_lists": 1},
        {"billed_ms": 1000, "memory_size_mb": 512, "s3_gets": 2, "s3_lists": 0},
    ]

    cost = estimate_cost(records)

    assert (cost.s3_gets, cost.s3_lists) == (5, 1)
    assert cost.s3_usd == pytest.approx(5 * S3_GET_USD + 1 * S3_LIST_USD)
    assert cost.total_usd == pytest.approx(1.0 * GB_SECOND_USD + 2 * REQUEST_USD + cost.s3_usd)


def test_costs_add():
    one = estimate_cost([{"billed_ms": 1000, "memory_size_mb": 1024, "s3_gets": 1}])
    two = estimate_cost([{"billed_ms": 2000, "memory_size_mb": 1024}], missing=1)

    both = one + two

    assert (both.invocations, both.missing, both.s3_gets, both.s3_lists) == (2, 1, 1, None)
    assert both.total_usd == pytest.approx(one.total_usd + two.total_usd)
