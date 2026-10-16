"""
What a run recorded about itself, and how to render it.

Two kinds of record, one per level, each collected only when a caller
passes [`Lambdas`][map_lambda.Lambdas] a list:

* [`coordinator_stats`][map_lambda.Lambdas.coordinator_stats] - one per
  coordinator invocation, i.e. per partition per call.
  [`summarize_coordinators`][map_lambda.stats.summarize_coordinators] renders them.
* [`invocation_stats`][map_lambda.Lambdas.invocation_stats] - one per worker
  invocation, built by the coordinator only when asked (`return_stats`), and
  arriving on the frames as the run goes.
  [`summarize_invocations`][map_lambda.stats.summarize_invocations] renders them.

Every record carries `call`, the 0-based index of the
[`map_unordered`][map_lambda.Lambdas.map_unordered] or
[`for_each`][map_lambda.Lambdas.for_each] call it came from, and `call_start`,
`time.time()` when that call began. Records group by `call` because it is an
index: two calls that read the same clock value would merge on a timestamp.

An invocation's duration is time queued for a connection, plus the
round trip, plus execution.
"""

import json
from pathlib import Path
from typing import Any

from .coordinator import MAX_STREAMS_PER_CONNECTION

# One record, at either level.
Stats = dict[str, Any]


def percentile(sorted_values: list[float], fraction: float) -> float:
    """Nearest-rank percentile of an already-sorted, non-empty list."""
    index = min(len(sorted_values) - 1, int(fraction * len(sorted_values)))
    return sorted_values[index]


def distribution(label: str, values_ms: list[float]) -> str:
    """
    One `min ... p50 ... p90 ... max ...` line, in milliseconds.

    The min is what tells a cost every invocation pays from a queue only
    some wait in; p50/p90/max look the same for both.
    """
    values = sorted(values_ms)
    return (
        f"{label:<16}min {values[0]:.0f}ms, "
        f"p50 {percentile(values, 0.5):.0f}ms, "
        f"p90 {percentile(values, 0.9):.0f}ms, "
        f"max {values[-1]:.0f}ms"
    )


def max_outstanding(stats: list[Stats]) -> int:
    """
    The peak number of invocations dispatched but not yet returned.

    "Outstanding", not "in flight": from these records, one queued for a
    connection is indistinguishable from one running.
    """
    events = [(s["invoke_start"], 1) for s in stats]
    events += [(s["invoke_end"], -1) for s in stats]
    # ends before starts at equal timestamps, so a handover isn't counted
    # as an overlap
    events.sort(key=lambda event: (event[0], event[1]))

    outstanding = peak = 0
    for _, delta in events:
        outstanding += delta
        peak = max(peak, outstanding)
    return peak


def summarize_invocations(
    invocation_stats: list[Stats],
    elapsed: float | None = None,
    max_pool_connections: int | None = None,
) -> str:
    """
    A short human-readable summary of a run's invocation timings.

    `elapsed` is the caller's wall clock. Without it the wall time and
    rate are left out rather than derived from the records, whose span
    starts at the first invocation rather than at the call.
    """
    if not invocation_stats:
        return "no invocations recorded"

    durations = sorted(s["invoke_end"] - s["invoke_start"] for s in invocation_stats)
    peak = max_outstanding(invocation_stats)

    lines = [f"invocations:    {len(invocation_stats)}"]
    if elapsed is not None:
        lines.append(f"wall time:      {elapsed:.2f}s ({len(invocation_stats) / elapsed:.1f}/s)")
    lines.append(f"outstanding:    {peak} at peak")
    if max_pool_connections is not None and peak > max_pool_connections:
        lines.append(
            f"                (only {max_pool_connections} could be on the wire at once, so up "
            f"to {peak - max_pool_connections} were queued waiting for a connection; the times "
            f"below include that wait - raise concurrency to test whether it's the limit)"
        )
    lines.append(distribution("per invocation:", [d * 1000 for d in durations]))
    return "\n".join(lines)


def write_jsonl(stats: list[Stats], path: str | Path) -> None:
    """Write one JSON object per record."""
    with open(path, "w") as f:
        for s in stats:
            f.write(json.dumps(s) + "\n")


def read_jsonl(path: str | Path) -> list[Stats]:
    """Read back what [`write_jsonl`][map_lambda.stats.write_jsonl] wrote."""
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def summarize_coordinators(coordinator_stats: list[Stats]) -> str:
    """
    A short human-readable summary of a run's coordinators.
    """
    if not coordinator_stats:
        return "coordinators: none"
    dispatched = sum(r.get("dispatched", 0) for r in coordinator_stats)
    retried = sum(r.get("retried", 0) for r in coordinator_stats)
    waited = sum(r.get("retry_wait_ms", 0.0) for r in coordinator_stats) / 1000
    # Absent from a local run, which has no shared client set to build.
    # Saying "0 cold" there would be a claim about something not measured.
    reported = [r["cold"] for r in coordinator_stats if r.get("cold") is not None]
    cold = f", {sum(reported)} cold" if reported else ""
    calls = {r["call"] for r in coordinator_stats}
    spreads = []
    # The widest call is what the connections have to cover: a session's
    # calls need not ask for the same concurrency.
    widest = 0
    for call in calls:
        partitions = [r for r in coordinator_stats if r["call"] == call]
        elapsed = [r.get("elapsed_ms", 0.0) for r in partitions]
        spreads.append((max(elapsed) - min(elapsed)) / 1000)
        widest = max(widest, sum(r.get("connections", 0) for r in partitions))
    # A range when they differ: `split` can push one coordinator over a
    # 100-in-flight boundary and not the rest, and the smallest queues first.
    each = sorted({r.get("connections", 0) for r in coordinator_stats})
    per_coordinator = f"{each[0]}" if len(each) == 1 else f"{each[0]}-{each[-1]}"
    slowest = max(r.get("elapsed_ms", 0.0) for r in coordinator_stats) / 1000
    share = f" ({100 * retried / dispatched:.2f}%)" if dispatched else ""
    # Only when it happened: backoff turns throttling into a slower run
    # rather than a failed one, and nothing else here would say so.
    waiting = f", {waited:.1f}s of backoff" if waited else ""
    return (
        f"coordinators:   {len(coordinator_stats)} invocations over "
        f"{len(calls)} call(s){cold}\n"
        # Invocations, not work items: a retry re-sends an invocation.
        f"                {dispatched:,} invocations dispatched, "
        f"{retried:,} retried{share}{waiting}\n"
        f"                {per_coordinator} HTTP/2 connection(s) per coordinator, "
        f"~{widest * MAX_STREAMS_PER_CONNECTION:,} in flight available\n"
        f"                slowest partition {slowest:.1f}s, worst spread within a "
        f"call {max(spreads):.1f}s"
    )
