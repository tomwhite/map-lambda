"""
Rendering a run's two kinds of record, handed over as literals:
`test_dispatch.py` is where they are built.
"""

from map_lambda.stats import (
    max_outstanding,
    read_jsonl,
    summarize_coordinators,
    summarize_invocations,
    write_jsonl,
)


def _stats(*intervals):
    return [
        {"i": [n], "req_id": f"req-{n}", "invoke_start": start, "invoke_end": end}
        for n, (start, end) in enumerate(intervals)
    ]


def test_max_outstanding_counts_overlaps():
    # three overlapping, then one on its own
    stats = _stats((0.0, 3.0), (1.0, 4.0), (2.0, 5.0), (10.0, 11.0))
    assert max_outstanding(stats) == 3


def test_max_outstanding_ignores_exact_handovers():
    # one ends exactly as the next begins
    stats = _stats((0.0, 1.0), (1.0, 2.0), (2.0, 3.0))
    assert max_outstanding(stats) == 1


def test_max_outstanding_of_one():
    assert max_outstanding(_stats((0.0, 1.0))) == 1


def test_summarize_flags_connection_queueing():
    # far more outstanding than the pool can carry
    stats = _stats(*[(0.0, 5.0)] * 100)
    summary = summarize_invocations(stats, elapsed=5.0, max_pool_connections=10)
    assert "outstanding:    100 at peak" in summary
    assert "only 10 could be on the wire" in summary
    assert "90 were queued" in summary


def test_summarize_silent_when_pool_is_not_the_limit():
    stats = _stats(*[(0.0, 1.0)] * 4)
    summary = summarize_invocations(stats, elapsed=1.0, max_pool_connections=200)
    assert "queued" not in summary
    assert "invocations:    4" in summary


def test_summarize_without_a_known_pool_limit():
    assert "queued" not in summarize_invocations(_stats((0.0, 1.0)), elapsed=1.0)


def test_summarize_empty():
    assert summarize_invocations([], elapsed=1.0) == "no invocations recorded"


def test_summarize_leaves_out_the_wall_time_it_was_not_given():
    """Not derived from the records' own span, which starts at the first invocation."""
    summary = summarize_invocations(_stats((0.0, 1.0), (0.0, 2.0)))

    assert "wall time" not in summary
    assert "invocations:    2" in summary
    assert "outstanding:    2 at peak" in summary
    assert "per invocation:" in summary


def test_write_jsonl_round_trips(tmp_path):
    import json

    stats = _stats((0.0, 1.0), (1.0, 2.5))
    path = tmp_path / "stats.jsonl"
    write_jsonl(stats, path)

    assert read_jsonl(path) == stats
    assert read_jsonl(path)[0]["req_id"] == "req-0"
    # one object per line, so a partial file is still readable
    lines = path.read_text().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["req_id"] == "req-0"


def test_summarize_counts_retries_against_what_they_absorbed():
    """The GOAWAY rate is invisible unless the retries are counted."""
    partitions = [
        {
            "call": 1.0,
            "dispatched": 500,
            "retried": 3,
            "cold": True,
            "elapsed_ms": 8000.0,
            "connections": 5,
        },
        {
            "call": 1.0,
            "dispatched": 500,
            "retried": 1,
            "cold": False,
            "elapsed_ms": 8200.0,
            "connections": 5,
        },
    ]

    summary = summarize_coordinators(partitions)

    assert "2 invocations over 1 call(s), 1 cold" in summary
    assert "1,000 invocations dispatched, 4 retried (0.40%)" in summary
    assert "slowest partition 8.2s" in summary
    assert "worst spread within a call 0.2s" in summary


def test_summarize_leaves_cold_out_when_nothing_reported_it():
    """
    A local coordinator has no shared client set to build, so it sends
    `cold` as null. "0 cold" would read as a warm deployment rather than
    as a figure nobody measured.
    """
    partitions = [{"call": 0, "dispatched": 3, "cold": None, "elapsed_ms": 1.0, "connections": 1}]

    summary = summarize_coordinators(partitions)

    assert "1 invocations over 1 call(s)\n" in summary
    assert "cold" not in summary


def _partition(call=0, connections=1, **fields):
    return {
        "call": call,
        "dispatched": 10,
        "retried": 0,
        "elapsed_ms": 1.0,
        "connections": connections,
        **fields,
    }


def test_summarize_reports_the_connections_a_partition_had():
    summary = summarize_coordinators([_partition(connections=9)])

    assert "9 HTTP/2 connection(s) per coordinator, ~1,152 in flight available" in summary


def test_the_streams_available_are_the_calls_not_one_coordinators():
    summary = summarize_coordinators([_partition(connections=2) for _ in range(8)])

    assert "~2,048 in flight available" in summary


def test_connections_are_a_range_when_the_coordinators_differ():
    """Saying "2 each" would overstate the coordinator that queues first."""
    summary = summarize_coordinators([_partition(connections=2), _partition(connections=1)])

    assert "1-2 HTTP/2 connection(s) per coordinator" in summary
    assert "~384 in flight available" in summary


def test_the_widest_call_is_what_the_connections_have_to_cover():
    """A session's calls need not ask for the same concurrency."""
    summary = summarize_coordinators(
        [_partition(call=0, connections=1), _partition(call=1, connections=4)]
    )

    assert "~512 in flight available" in summary


def test_summarize_takes_the_worst_spread_across_calls():
    partitions = [
        {"call": 1.0, "dispatched": 10, "retried": 0, "elapsed_ms": 1000.0},
        {"call": 1.0, "dispatched": 10, "retried": 0, "elapsed_ms": 1100.0},
        {"call": 2.0, "dispatched": 10, "retried": 0, "elapsed_ms": 1000.0},
        {"call": 2.0, "dispatched": 10, "retried": 0, "elapsed_ms": 6000.0},
    ]

    assert "worst spread within a call 5.0s" in summarize_coordinators(partitions)


def test_summarize_with_nothing_to_say():
    assert summarize_coordinators([]) == "coordinators: none"
