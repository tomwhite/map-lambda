"""
`map_unordered`: outputs handed over as they arrive.

`test_dispatch.py` covers the dispatch itself, which `for_each` shares -
everything up to where the outputs go. What is only true here is the
timing, an output reaching the caller while the run is still going, and
what a run nobody finishes iterating does and does not do.
"""

import asyncio
from typing import Any

import pytest

from map_lambda import Session

from .fakes import (
    RecordingInvoker,
    completed,
    dispatcher_body,
    dispatcher_summary,
    lines,
)


def _session(invoker: Any, **settings: Any) -> Session:
    """A `Session` whose invoker responds without invoking anything."""
    session = Session(**{"dispatchers": 1, "concurrency": 4, **settings})
    session._invoker = invoker
    return session


def _outputs(invoker: Any, inputs: list[Any], **settings: Any) -> list[tuple[int, Any]]:
    """Everything one `map_unordered` yielded, in the order it came."""

    async def drain() -> list[tuple[int, Any]]:
        session = _session(invoker, **settings)
        return [a async for a in session.map_unordered("worker", inputs, input_key="i")]

    return asyncio.run(drain())


def test_an_answer_reaches_the_caller_before_the_run_has_finished():
    """
    The dispatcher's summary frame is not written until the test has been
    given the output, so an implementation that collected first would
    never get here.
    """
    invoker = RecordingInvoker()
    body = invoker.respond()
    body.write(dispatcher_body(completed(succeeded=1, outputs=[{"index": 0, "value": "early"}])))

    async def drain() -> list[tuple[int, Any]]:
        session = _session(invoker)
        outputs = []
        async for output in session.map_unordered("worker", ["a"], input_key="i"):
            outputs.append(output)
            body.write(lines({"Summary": dispatcher_summary(1)}))
            body.close()
        return outputs

    outputs = asyncio.run(asyncio.wait_for(drain(), timeout=5))

    assert outputs == [(0, "early")]


def test_a_losing_run_keeps_the_answers_it_had_already_handed_over():
    """
    The frames before the failure were real work, and the caller has
    already acted on them. The failing frame's own outputs are not
    yielded: the loop raises instead of going round again.
    """
    invoker = RecordingInvoker(
        [
            [
                completed(succeeded=1, outputs=[{"index": 0, "value": "a"}]),
                completed(succeeded=1, outputs=[{"index": 1, "value": "b"}], first_error="boom"),
                {"Summary": dispatcher_summary(3)},
            ]
        ]
    )
    seen: list[tuple[int, Any]] = []

    async def drain() -> None:
        session = _session(invoker)
        async for output in session.map_unordered("worker", list("abc"), input_key="i"):
            seen.append(output)

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(drain())
    assert seen == [(0, "a")]


def test_an_input_whose_worker_returned_nothing_is_never_yielded():
    """
    There is no place to leave empty, so the input is simply never heard
    from - which is why a count of outputs is not a count of work done.
    """
    invoker = RecordingInvoker(
        [
            [
                completed(succeeded=2, outputs=[{"index": 1, "value": "b"}]),
                {"Summary": dispatcher_summary(2)},
            ]
        ]
    )

    assert _outputs(invoker, ["a", "b"]) == [(1, "b")]


def test_stopping_early_closes_the_streams_of_dispatchers_still_running():
    invoker = RecordingInvoker()
    body = invoker.respond()
    body.write(dispatcher_body(completed(succeeded=1, outputs=[{"index": 0, "value": "first"}])))

    async def take_one() -> None:
        session = _session(invoker)
        outputs = session.map_unordered("worker", ["a", "b"], input_key="i")
        async for _ in outputs:
            break
        assert not body.closed
        await outputs.aclose()

    asyncio.run(take_one())

    # The rest of the partition was never written, so only the client
    # letting go of the stream can have closed it.
    assert body.closed


def test_an_abandoned_run_raises_nothing_however_it_went():
    """
    The paired half of the losing run above: the same stream, the same
    failure on the wire, and taking one output instead of draining gets
    the output and no exception. Stopping early asks for some outputs,
    not for the run's verdict.

    What the stats hold afterwards is deliberately not asserted - it
    depends on whether the partition had settled yet, which is timing.
    """
    invoker = RecordingInvoker(
        [
            [
                completed(succeeded=1, outputs=[{"index": 0, "value": "first"}]),
                {"Summary": dispatcher_summary(2)},
            ]
        ]
    )

    async def take_one() -> list[tuple[int, Any]]:
        session = _session(invoker)
        outputs = session.map_unordered("worker", ["a", "b"], input_key="i")
        taken = []
        async for output in outputs:
            taken.append(output)
            break
        await outputs.aclose()
        return taken

    assert asyncio.run(asyncio.wait_for(take_one(), timeout=5)) == [(0, "first")]
