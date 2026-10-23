import asyncio
from typing import Any

import pytest

from map_lambda import LOCAL_ENDPOINT_URL, Lambdas

from .utils import check_invocation_counts


async def echo(
    path: str,
    timing_map: dict[int, list[float]],
    i: list[int],
    **settings: Any,
):

    async with Lambdas(coordinator_stats=[], invocation_stats=[], **settings) as lambdas:
        constant_args: dict[str, Any] = {
            "path": path,
            "timing_map": timing_map,
            "default_sleep": 0.0,
        }
        outputs = [
            answer
            async for _, answer in lambdas.map_unordered(
                "echo", i, input_key="i", constant_args=constant_args
            )
        ]
        return outputs, lambdas.coordinator_stats, lambdas.invocation_stats


@pytest.mark.lambda_integration
def test_echo(tmp_path):
    path = str(tmp_path)
    timing_map: dict[int, list[float]] = {}
    outputs, partitions, records = asyncio.run(
        echo(path, timing_map, list(range(3)), endpoint_url=LOCAL_ENDPOINT_URL)
    )

    # a real coordinator finishes them in whatever order they come back
    assert sorted(answer["i"] for answer in outputs) == [0, 1, 2]

    assert len(partitions) == 1
    assert partitions[0]["dispatched"] == 3
    assert partitions[0]["succeeded"] == 3
    assert partitions[0]["function_errors"] == partitions[0]["transport_errors"] == 0
    assert partitions[0]["first_error"] is None

    assert sorted(record["index"] for record in records) == [0, 1, 2]
    # Present but empty: `cargo lambda watch` sets no `x-amzn-requestid`.
    assert all("req_id" in record for record in records)

    check_invocation_counts(path, timing_map, 3)


@pytest.mark.lambda_integration
def test_a_real_coordinator_sends_every_field_this_side_reads(tmp_path):
    """
    The one check that spans both languages.

    Both Rust ends share rust/protocol, but `stats.py`, `timeline.py` and
    every caller read these records by key, so a rename there parses fine
    and goes missing quietly. The sets are pinned against a coordinator
    that really ran.
    """
    _, partitions, records = asyncio.run(
        echo(str(tmp_path), {}, [0], endpoint_url=LOCAL_ENDPOINT_URL)
    )

    assert set(partitions[0]) == {
        "req_id",
        "dispatched",
        "succeeded",
        "function_errors",
        "transport_errors",
        "retried",
        "retry_wait_ms",
        "first_error",
        "cold",
        "connections",
        "elapsed_ms",
        "call",
        "call_start",
    }
    assert set(records[0]) == {
        "index",
        "req_id",
        "invoke_start",
        "invoke_end",
        "call",
        "call_start",
    }


@pytest.mark.parametrize(
    ("timing_map", "n_tasks", "retries", "total_expected_retries"),
    [
        # no failures
        ({}, 3, 2, 0),
        # first invocation fails
        ({0: [-0.1], 1: [-0.1], 2: [-0.1]}, 3, 2, 3),
        # first two invocations fail
        ({0: [-0.1, -0.1], 1: [-0.1, -0.1], 2: [-0.1, -0.1]}, 3, 2, 6),
    ],
)  # fmt: off
@pytest.mark.lambda_integration
def test_echo_success(tmp_path, timing_map, n_tasks, retries, total_expected_retries):
    path = str(tmp_path)
    outputs, partitions, _ = asyncio.run(
        echo(
            path, timing_map, list(range(n_tasks)), endpoint_url=LOCAL_ENDPOINT_URL, retries=retries
        )
    )

    assert len(partitions) == 1
    assert partitions[0]["retried"] == total_expected_retries

    assert sorted(output["i"] for output in outputs) == list(range(n_tasks))
    check_invocation_counts(tmp_path, timing_map, n_tasks)


@pytest.mark.lambda_integration
def test_echo_failure(tmp_path):
    path = str(tmp_path)
    with pytest.raises(RuntimeError) as excinfo:
        asyncio.run(echo(path, {0: [-0.1]}, [0], endpoint_url=LOCAL_ENDPOINT_URL, retries=0))
    assert "Deliberately fail on invocation number 1 for input 0" in str(excinfo.value)


@pytest.mark.lambda_integration
def test_invoke_runs_the_worker_without_waiting_for_it(tmp_path):
    async def invoke_and_wait() -> None:
        async with Lambdas(endpoint_url=LOCAL_ENDPOINT_URL) as lambdas:
            await lambdas.invoke("echo", path=str(tmp_path), i=0, timing_map={}, default_sleep=0.0)
        # Returned once submitted, so the worker may not have run yet.
        for _ in range(100):
            if (tmp_path / "0").exists():
                return
            await asyncio.sleep(0.05)

    asyncio.run(invoke_and_wait())

    check_invocation_counts(tmp_path, {}, 1)
