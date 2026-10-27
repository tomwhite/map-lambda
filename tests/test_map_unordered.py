"""
`map_unordered`: answers handed over as they arrive.

`test_dispatch.py` covers the dispatch itself, which `for_each` shares -
everything up to where the answers go. What is only true here is the
timing, an answer reaching the caller while the run is still going, and
what a run nobody finishes iterating does and does not do.
"""

import asyncio
from typing import Any

import pytest

from map_lambda import Lambdas

from .fakes import (
    RecordingClient,
    completed,
    dispatcher_body,
    dispatcher_result,
    lines,
)


def _session(client: Any, **settings: Any) -> Lambdas:
    """A `Lambdas` whose Lambda client answers without invoking anything."""
    lambdas = Lambdas(**{"dispatchers": 1, "concurrency": 4, **settings})
    lambdas._client = client
    return lambdas


def _answers(client: Any, inputs: list[Any], **settings: Any) -> list[tuple[int, Any]]:
    """Everything one `map_unordered` yielded, in the order it came."""

    async def drain() -> list[tuple[int, Any]]:
        lambdas = _session(client, **settings)
        return [a async for a in lambdas.map_unordered("worker", inputs, input_key="i")]

    return asyncio.run(drain())


def test_every_answer_carries_the_position_of_its_input_across_partitions():
    """
    Partitions are strided, so a dispatcher's own 0, 1, 2 are inputs
    0, 2, 4 - and unordered yields are joinable only by that number.
    """
    client = RecordingClient(
        [
            [
                completed(
                    done=3,
                    outputs=[{"index": 0, "response": "a"}, {"index": 2, "response": "e"}],
                ),
                {"Result": dispatcher_result(3)},
            ],
            [
                completed(done=2, outputs=[{"index": 1, "response": "d"}]),
                {"Result": dispatcher_result(2)},
            ],
        ]
    )

    answers = _answers(client, list("abcde"), dispatchers=2, concurrency=8)

    assert sorted(answers) == [(0, "a"), (3, "d"), (4, "e")]


def test_an_answer_reaches_the_caller_before_the_run_has_finished():
    """
    The dispatcher's result frame is not written until the test has been
    given the answer, so an implementation that collected first would
    never get here.
    """
    client = RecordingClient()
    body = client.respond()
    body.write(dispatcher_body(completed(done=1, outputs=[{"index": 0, "response": "early"}])))

    async def drain() -> list[tuple[int, Any]]:
        lambdas = _session(client)
        answers = []
        async for answer in lambdas.map_unordered("worker", ["a"], input_key="i"):
            answers.append(answer)
            body.write(lines({"Result": dispatcher_result(1)}))
            body.close()
        return answers

    answers = asyncio.run(asyncio.wait_for(drain(), timeout=5))

    assert answers == [(0, "early")]


def test_a_losing_run_keeps_the_answers_it_had_already_handed_over():
    """
    The frames before the failure were real work, and the caller has
    already acted on them. The failing frame's own answers are not
    yielded: the loop raises instead of going round again.
    """
    client = RecordingClient(
        [
            [
                completed(done=1, outputs=[{"index": 0, "response": "a"}]),
                completed(done=1, outputs=[{"index": 1, "response": "b"}], first_error="boom"),
                {
                    "Result": dispatcher_result(
                        2, dispatched=3, transport_errors=1, first_error="boom"
                    )
                },
            ]
        ]
    )
    seen: list[tuple[int, Any]] = []

    async def drain() -> None:
        lambdas = _session(client)
        async for answer in lambdas.map_unordered("worker", list("abc"), input_key="i"):
            seen.append(answer)

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(drain())
    assert seen == [(0, "a")]


def test_an_input_whose_worker_returned_nothing_is_never_yielded():
    """
    There is no place to leave empty, so the input is simply never heard
    from - which is why a count of answers is not a count of work done.
    """
    client = RecordingClient(
        [
            [
                completed(done=2, outputs=[{"index": 1, "response": "b"}]),
                {"Result": dispatcher_result(2)},
            ]
        ]
    )

    assert _answers(client, ["a", "b"]) == [(1, "b")]


def test_stopping_early_tears_down_the_dispatchers_still_running():
    client = RecordingClient()
    body = client.respond()
    body.write(dispatcher_body(completed(done=1, outputs=[{"index": 0, "response": "first"}])))

    async def take_one() -> None:
        lambdas = _session(client)
        answers = lambdas.map_unordered("worker", ["a", "b"], input_key="i")
        async for _ in answers:
            break
        assert not body.closed
        await answers.aclose()

    asyncio.run(take_one())

    # The rest of the partition was never written, so only the client
    # letting go of the stream can have closed it.
    assert body.closed


def test_an_abandoned_run_raises_nothing_however_it_went():
    """
    The paired half of the losing run above: the same stream, the same
    failure on the wire, and taking one answer instead of draining gets
    the answer and no exception. Stopping early asks for some answers,
    not for the run's verdict.

    What the stats hold afterwards is deliberately not asserted - it
    depends on whether the partition had settled yet, which is timing.
    """
    client = RecordingClient(
        [
            [
                completed(done=1, outputs=[{"index": 0, "response": "first"}]),
                {
                    "Result": dispatcher_result(
                        1, dispatched=2, transport_errors=1, first_error="boom"
                    )
                },
            ]
        ]
    )

    async def take_one() -> list[tuple[int, Any]]:
        lambdas = _session(client)
        answers = lambdas.map_unordered("worker", ["a", "b"], input_key="i")
        taken = []
        async for answer in answers:
            taken.append(answer)
            break
        await answers.aclose()
        return taken

    assert asyncio.run(asyncio.wait_for(take_one(), timeout=5)) == [(0, "first")]
