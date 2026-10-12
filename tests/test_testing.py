"""
The in-process coordinator, tested against the real Python half it is
meant to stand in for.

The tests worth having are the ones that catch the fake and the real
coordinator drifting apart.
"""

import asyncio
from typing import Any

import pytest

from map_lambda import coordinator
from map_lambda.coordinator import Answer
from map_lambda.testing import InProcessCoordinator, dispatching_to


def run(fake: InProcessCoordinator, inputs: list[Any], **kwargs: Any) -> list[Answer]:
    """Every answer the stream yielded, labelled as it labelled them."""
    collected: list[Answer] = []

    async def go() -> None:
        async for batch in coordinator.stream(
            fake,
            inputs,
            constant_args=kwargs.pop("constant_args", {"prefix": "p"}),
            coordinator_function="coordinator",
            target_function="worker",
            input_key="i",
            coordinators=kwargs.pop("coordinators", 2),
            concurrency=kwargs.pop("concurrency", 4),
            retries=kwargs.pop("retries", 0),
            **kwargs,
        ):
            collected.extend(batch)

    asyncio.run(go())
    return collected


def test_the_worker_is_handed_the_template_with_its_input_added():
    """As `payload_for` in rust/coordinator/src/invoke.rs builds it."""
    seen: list[dict] = []
    run(InProcessCoordinator(seen.append), [0, 1, 2], coordinators=1)

    assert seen == [
        {"prefix": "p", "i": 0},
        {"prefix": "p", "i": 1},
        {"prefix": "p", "i": 2},
    ]


def test_an_input_wins_over_a_template_key_of_the_same_name():
    """As `payload_for` in rust/coordinator/src/invoke.rs does."""
    seen: list[dict] = []
    run(
        InProcessCoordinator(seen.append),
        [7],
        coordinators=1,
        constant_args={"i": "from the template"},
    )

    assert seen == [{"i": 7}]


def test_a_list_input_reaches_the_worker_whole():
    """Including a one-item list, which is not unwrapped."""
    seen: list[dict] = []
    run(InProcessCoordinator(seen.append), [[0, 1, 2], [3]], coordinators=1)

    assert seen == [{"prefix": "p", "i": [0, 1, 2]}, {"prefix": "p", "i": [3]}]


def test_a_clean_run_passes_check_and_records_every_partition():
    """The fake's result frames have to pass `check`, `connections` included."""
    partitions: list[dict] = []
    run(InProcessCoordinator(lambda payload: None), list(range(6)), coordinator_stats=partitions)

    assert len(partitions) == 2
    assert sum(r["dispatched"] for r in partitions) == 6
    assert sum(r["succeeded"] for r in partitions) == 6
    assert all(r["first_error"] is None for r in partitions)


def test_answers_carry_what_the_worker_returned():
    """Verbatim, with no envelope, as the real coordinator does."""
    collected = run(
        InProcessCoordinator(lambda payload: {"doubled": payload["i"] * 2}),
        [1, 2, 3],
        coordinators=1,
    )

    assert [set(o["response"]) for o in collected] == [{"doubled"}] * 3
    assert {o["index"]: o["response"]["doubled"] for o in collected} == {0: 2, 1: 4, 2: 6}


def test_a_worker_that_returns_nothing_produces_no_answers():
    """As `Tally::observe` in rust/coordinator/src/tally.rs drops them."""
    assert run(InProcessCoordinator(lambda payload: None), [1, 2, 3], coordinators=1) == []


def test_a_raising_worker_is_not_tried_again_however_large_the_budget():
    """
    The same payload gets the same answer, so a retry only bills for a run
    that has already failed.
    """
    attempts: dict[int, int] = {}

    def flaky(payload: dict) -> None:
        i = payload["i"]
        attempts[i] = attempts.get(i, 0) + 1
        if attempts[i] == 1:
            raise RuntimeError(f"raised on {i}")

    with pytest.raises(RuntimeError, match="raised on 0"):
        run(InProcessCoordinator(flaky), [0, 1], coordinators=1, retries=5)

    assert attempts == {0: 1, 1: 1}


def test_a_failure_that_outlives_the_budget_raises_with_the_workers_message():

    def always_fails(payload: dict) -> None:
        raise RuntimeError("worker said no")

    with pytest.raises(RuntimeError) as excinfo:
        run(InProcessCoordinator(always_fails), [0, 1], coordinators=1)

    assert "cannot succeed" in str(excinfo.value)
    assert "worker said no" in str(excinfo.value)


def test_dispatching_to_reaches_code_that_builds_its_own_session():
    import map_lambda

    async def a_clients_own_function(values):
        return [
            answer
            async for _, answer in map_lambda.map_unordered(
                "double", values, input_key="i", coordinators=2
            )
        ]

    with dispatching_to(lambda payload: {"doubled": payload["i"] * 2}) as coordinator:
        answers = asyncio.run(a_clients_own_function([1, 2, 3]))

    assert sorted(a["doubled"] for a in answers) == [2, 4, 6]
    assert len(coordinator.requests) == 2


def test_dispatching_to_puts_the_real_client_back():
    """A leak would fail elsewhere, later, in a test that thought it was talking to AWS."""
    from map_lambda import api

    before = api.lambda_client

    with pytest.raises(RuntimeError, match="boom"), dispatching_to(lambda payload: None):
        assert api.lambda_client is not before
        raise RuntimeError("boom")

    assert api.lambda_client is before
