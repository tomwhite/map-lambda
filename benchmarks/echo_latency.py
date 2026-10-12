"""
How long a round of `echo` invocations takes, and what one costs.

    uv run python benchmarks/echo_latency.py
    uv run python benchmarks/echo_latency.py --invocations 1000 --concurrency 200
    uv run python benchmarks/echo_latency.py --local
    uv run python benchmarks/echo_latency.py --endpoint-url http://localhost:9000

`echo` does nothing but read its input back, so what this measures is
map-lambda's own overhead: the coordinator's dispatch, the invoke round
trip, and the cold starts in the first wave. A worker doing real work
adds its own time to the per-invocation figures and changes nothing else.

**This invokes real Lambdas and is billed.** The default is 1,000
invocations of a function doing nothing, which is a fraction of a cent,
but it is a real deployment: see the Deployment section of README.md.

Run it twice. The first round pays a cold start per concurrent execution
and is the number to quote for a cold fleet; the second is the warm one.
"""

import argparse
import asyncio
import time
from typing import Any

from map_lambda import LOCAL_ENDPOINT_URL, Lambdas
from map_lambda import stats as stats_module

ECHO_FUNCTION = "echo"

# Lambda's only writable directory, where `echo` keeps its per-input
# invocation counts.
ECHO_PATH = "/tmp/map-lambda-echo"


async def run(args: argparse.Namespace) -> tuple[float, list[dict], list[Any]]:
    """One round of invocations, returning the wall clock, the records and the answers."""
    invocations: list[dict] = []
    async with Lambdas(
        endpoint_url=args.endpoint_url or (LOCAL_ENDPOINT_URL if args.local else None),
        coordinators=args.coordinators,
        concurrency=args.concurrency,
        invocation_stats=invocations,
    ) as lambdas:
        started = time.monotonic()
        answers = [
            answer
            async for _, answer in lambdas.map_unordered(
                ECHO_FUNCTION,
                list(range(args.invocations)),
                input_key="i",
                # 0.0 rather than echo's default 10ms, so the figures are
                # map-lambda's overhead rather than a sleep it was told to do
                constant_args={"path": ECHO_PATH, "default_sleep": 0.0},
            )
        ]
        return time.monotonic() - started, invocations, answers


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--invocations", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--coordinators", type=int, default=1)
    parser.add_argument(
        "--local", action="store_true", help=f"against {LOCAL_ENDPOINT_URL} rather than AWS"
    )
    parser.add_argument(
        "--endpoint-url", default=None, help="a `cargo lambda watch` on another port"
    )
    parser.add_argument(
        "--jsonl",
        default=None,
        help="write the invocation records here. Striding makes `index %% coordinators` "
        "the coordinator that ran an invocation, so the records carry which one",
    )
    args = parser.parse_args()

    elapsed, invocations, answers = asyncio.run(run(args))
    where = args.endpoint_url or ("local" if args.local else "AWS")
    print(
        f"{args.invocations} echo invocations, {where}, concurrency {args.concurrency}, "
        f"{args.coordinators} coordinator(s)"
    )
    print(stats_module.summarize_invocations(invocations, elapsed=elapsed))
    if args.jsonl:
        stats_module.write_jsonl(invocations, args.jsonl)
        print(f"wrote {len(invocations)} records to {args.jsonl}")
    # echo returns its input
    assert sorted(answer["i"] for answer in answers) == list(range(args.invocations))


if __name__ == "__main__":
    main()
