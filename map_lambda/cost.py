"""
Estimate what a call cost, at list prices, from the REPORT lines Lambda
writes to CloudWatch Logs for each invocation: its billed duration and
memory size.

[`fetch_reports`][map_lambda.cost.fetch_reports] joins a call's records,
from [`invocation_stats`][map_lambda.Session.invocation_stats] or
[`dispatcher_stats`][map_lambda.Session.dispatcher_stats], with their
REPORT lines, and [`estimate_cost`][map_lambda.cost.estimate_cost] prices
the joined records. [`generate_timeline`][map_lambda.timeline.generate_timeline]
also draws when each function ran from them.

CloudWatch takes some seconds to ingest REPORT lines, so fetching them waits.
It needs `logs:StartQuery` and `logs:GetQueryResults` on the functions' log
groups, and boto3, which the `diagnostics` extra installs.
"""

import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from itertools import pairwise
from typing import Any

from .stats import Stats

__docformat__ = "google"

# arm64 Lambda and S3 Standard list prices, in standard US and EU regions
PRICING_AS_OF = "2026-09-08"

GB_SECOND_USD = 0.0000133334
REQUEST_USD = 0.20 / 1_000_000
S3_GET_USD = 0.0004 / 1000
# LIST is billed with PUT, COPY and POST
S3_LIST_USD = 0.005 / 1000

# Lambda bills memory in GB of 1024 MB.
MEGABYTES_PER_GIGABYTE = 1024

# @initDuration is absent on warm invocations, which is how a cold start is detected.
REPORT_QUERY = """
fields @requestId, @duration, @billedDuration, @initDuration
| fields @maxMemoryUsed, @memorySize
| filter @type = "REPORT"
| limit 10000
"""

# Logs Insights' row cap per query, as in REPORT_QUERY's `limit`. It truncates
# silently, and the subset left is not a random one.
REPORT_LIMIT = 10_000

# Well under REPORT_LIMIT: a window also holds REPORT lines from other runs.
SLICE_RECORDS = 5_000

# The REPORT line is written after the invocation returns, on Lambda's clock.
WINDOW_PADDING_SECONDS = 60

# Insights gives @memorySize and @maxMemoryUsed in bytes of decimal megabytes.
MEGABYTE = 1_000_000


@dataclass(frozen=True)
class Cost:
    """What a call's invocations cost, estimated at list prices."""

    invocations: int
    """Invocations priced: those whose REPORT line was found."""
    missing: int
    """Invocations whose REPORT line wasn't found, so aren't priced."""
    gb_seconds: float
    compute_usd: float
    request_usd: float
    s3_gets: int | None
    """S3 GET requests the records counted, or `None` if none counted them."""
    s3_lists: int | None
    """S3 LIST requests the records counted, or `None` if none counted them."""
    s3_usd: float
    total_usd: float

    @property
    def lambda_usd(self) -> float:
        return self.compute_usd + self.request_usd

    def __add__(self, other: "Cost") -> "Cost":
        return Cost(
            invocations=self.invocations + other.invocations,
            missing=self.missing + other.missing,
            gb_seconds=self.gb_seconds + other.gb_seconds,
            compute_usd=self.compute_usd + other.compute_usd,
            request_usd=self.request_usd + other.request_usd,
            s3_gets=add_counts(self.s3_gets, other.s3_gets),
            s3_lists=add_counts(self.s3_lists, other.s3_lists),
            s3_usd=self.s3_usd + other.s3_usd,
            total_usd=self.total_usd + other.total_usd,
        )

    def __str__(self) -> str:
        if self.s3_gets is None and self.s3_lists is None:
            s3 = "not counted"
        else:
            s3 = f"${self.s3_usd:.4f} ({self.s3_gets or 0:,} GETs, {self.s3_lists or 0:,} LISTs)"
        lines = [
            f"cost:           ${self.total_usd:.4f} total (approximate)",
            f"                  lambda  ${self.lambda_usd:.4f} "
            f"({self.gb_seconds:.1f} GB-s, {self.invocations:,} requests)",
            f"                  S3      {s3}",
            f"                (arm64 and S3 Standard list prices as of {PRICING_AS_OF};",
            "                 excludes the free tier, retries and the client's own requests)",
        ]
        if self.missing:
            lines.append(f"                {self.missing:,} invocations had no REPORT line")
        return "\n".join(lines)


def gigabytes(memory_size_mb: float) -> float:
    """A function's memory size in the gigabytes Lambda bills in."""
    return memory_size_mb / MEGABYTES_PER_GIGABYTE


def add_counts(a: int | None, b: int | None) -> int | None:
    return None if a is None and b is None else (a or 0) + (b or 0)


def count(records: list[Stats], key: str) -> int | None:
    """The sum of `key` over the records that carry it, or `None` if none do."""
    counted = [r[key] for r in records if key in r]
    return round(sum(counted)) if counted else None


def estimate_cost(records: list[Stats], *, missing: int = 0) -> Cost:
    """
    Estimate the cost of `records` at list prices.

    `records` are as [`fetch_reports`][map_lambda.cost.fetch_reports]
    returns them, each with its REPORT line's `billed_ms` and
    `memory_size_mb`. `missing` is how many invocations had no REPORT line,
    to carry into the estimate.

    map-lambda doesn't see the S3 requests a function makes, so S3 is priced
    only if the caller adds `s3_gets` and `s3_lists` to the records, counted
    by its own function. If no record carries them, the estimate's S3 counts
    are `None`, and it prints S3 as not counted.
    """
    gb_seconds = sum(gigabytes(r["memory_size_mb"]) * r["billed_ms"] / 1000 for r in records)
    compute_usd = gb_seconds * GB_SECOND_USD
    request_usd = len(records) * REQUEST_USD
    gets = count(records, "s3_gets")
    lists = count(records, "s3_lists")
    s3_usd = (gets or 0) * S3_GET_USD + (lists or 0) * S3_LIST_USD
    return Cost(
        invocations=len(records),
        missing=missing,
        gb_seconds=gb_seconds,
        compute_usd=compute_usd,
        request_usd=request_usd,
        s3_gets=gets,
        s3_lists=lists,
        s3_usd=s3_usd,
        total_usd=compute_usd + request_usd + s3_usd,
    )


def fetch_reports(
    records: list[Stats],
    function: str,
    *,
    logs: Any = None,
    wait_s: float = 300,
    on_progress: Callable[[str], None] | None = None,
) -> list[Stats]:
    """
    Join each record with its invocation's REPORT line from CloudWatch Logs.

    `records` are map-lambda's, from `invocation_stats` or `dispatcher_stats`.
    Each is returned with its REPORT line's `execution_ms`, `billed_ms`,
    `memory_size_mb`, `max_memory_mb`, and `init_ms`, which is `None` unless
    the invocation started cold. Records whose REPORT line isn't found are
    left out.

    `function` is the deployed name, as
    [`Session.function_name`][map_lambda.Session.function_name] gives it.
    `logs` is a boto3 CloudWatch Logs client, by default one for
    `AWS_REGION`. It waits while CloudWatch is still adding REPORT lines,
    until every one is found, a query finds no more, or `wait_s` passes;
    Lambda occasionally never writes one. `on_progress` is called with a
    line saying how many it has found.
    """
    if not records:
        return []
    if logs is None:
        import boto3

        # boto3 reads only AWS_DEFAULT_REGION
        logs = boto3.client("logs", region_name=os.environ.get("AWS_REGION"))
    found = _query_all_reports(
        logs,
        function,
        {r["req_id"] for r in records},
        query_windows(records),
        wait_s,
        on_progress,
    )
    return join(records, found)


def span(record: Stats) -> tuple[float, float]:
    """When a record's invocation ran, in epoch seconds."""
    if "invoke_start" in record:
        return record["invoke_start"], record["invoke_end"]
    # a dispatcher's record has no invoke times, but is invoked as its call starts
    return record["call_start"], record["call_start"] + record["elapsed_ms"] / 1000


def query_windows(records: list[Stats]) -> list[tuple[int, int]]:
    """
    Contiguous epoch-second windows over the run, one per `SLICE_RECORDS`
    invocations, padded because the client's and Lambda's clocks only
    roughly agree.
    """
    spans = [span(r) for r in records]
    start = int(min(s for s, _ in spans)) - WINDOW_PADDING_SECONDS
    end = int(max(e for _, e in spans)) + WINDOW_PADDING_SECONDS
    ends = sorted(e for _, e in spans)
    cuts = {int(ends[i]) for i in range(SLICE_RECORDS, len(ends), SLICE_RECORDS)}
    bounds = sorted({start, end, *(c for c in cuts if start < c < end)})
    return list(pairwise(bounds))


def _query_reports(logs: Any, function: str, start: int, end: int) -> dict[str, dict[str, float]]:
    """`{req_id: report}` for every REPORT line of `function` in the window."""
    query_id = logs.start_query(
        logGroupName=f"/aws/lambda/{function}",
        startTime=start,
        endTime=end,
        queryString=REPORT_QUERY,
    )["queryId"]
    while (response := logs.get_query_results(queryId=query_id))["status"] in (
        "Scheduled",
        "Running",
    ):
        time.sleep(1)
    if response["status"] != "Complete":
        raise RuntimeError(f"Logs Insights query {response['status'].lower()} ({query_id})")
    found = {}
    for row in response["results"]:
        fields = {f["field"]: f["value"] for f in row}
        if (req_id := fields.pop("@requestId", None)) is not None:
            found[req_id] = {k: float(v) for k, v in fields.items() if k != "@ptr"}
    if len(found) >= REPORT_LIMIT:
        raise RuntimeError(
            f"{function}'s REPORT lines from {start} to {end} hit Logs Insights' row limit"
        )
    return found


def _query_all_reports(
    logs: Any,
    function: str,
    ids: set[str],
    windows: list[tuple[int, int]],
    wait_s: float,
    on_progress: Callable[[str], None] | None,
) -> dict[str, dict[str, float]]:
    deadline = time.monotonic() + wait_s
    matched = 0
    while True:
        found: dict[str, dict[str, float]] = {}
        for start, end in windows:
            found.update(_query_reports(logs, function, start, end))
        before, matched = matched, len(ids & found.keys())
        if on_progress is not None:
            on_progress(f"{function}: {matched:,} of {len(ids):,} REPORT lines")
        if matched == len(ids) or 0 < matched == before or time.monotonic() > deadline:
            return found
        time.sleep(10)


def join(records: list[Stats], found: dict[str, dict[str, float]]) -> list[Stats]:
    """
    Each of `records` with its REPORT line's numbers from `found`, by
    `req_id`. Records with no REPORT line in `found` are dropped.
    """
    return [
        {
            **r,
            "execution_ms": line["@duration"],
            "billed_ms": line["@billedDuration"],
            "memory_size_mb": line["@memorySize"] / MEGABYTE,
            "max_memory_mb": line["@maxMemoryUsed"] / MEGABYTE,
            "init_ms": line.get("@initDuration"),
        }
        for r in records
        if (line := found.get(r["req_id"])) is not None
    ]
