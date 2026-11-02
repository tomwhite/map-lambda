"""
The whole client-side path with the invocation faked: how a call is
partitioned into requests, what a dispatcher's stream is allowed to
say, and what a client refuses to believe.

The pieces are unit-tested in rust/client; this is what only shows up
when they run together, behind the Python API.
"""

import asyncio
from typing import Any

import pytest

from map_lambda import Session

from .fakes import RecordingInvoker, completed, dispatcher_summary
from .utils import drain


def _session(invoker: Any, **settings: Any) -> Session:
    """A `Session` whose invoker responds without invoking anything."""
    session = Session(**{"dispatchers": 2, "concurrency": 8, **settings})
    session._invoker = invoker
    return session


def _map(invoker: Any, inputs: list[Any] | None = None, **settings: Any) -> list[tuple[int, Any]]:
    """
    One `map_unordered` over `inputs` through `client`, drained, with
    nothing else going on.
    """
    session = _session(invoker, **settings)
    return drain(
        session.map_unordered(
            "worker",
            list(range(10)) if inputs is None else inputs,
            input_key="i",
            constant_args={"prefix": "p"},
        )
    )  # fmt: skip


def test_every_dispatcher_is_invoked_with_its_own_partition_and_its_share():
    """10 inputs over 2 dispatchers, 8 in flight split 4/4."""
    invoker = RecordingInvoker([dispatcher_summary(5), dispatcher_summary(5)])

    _map(invoker)

    assert [p["inputs"] for p in invoker.payloads] == [[0, 2, 4, 6, 8], [1, 3, 5, 7, 9]]
    assert [p["max_in_flight"] for p in invoker.payloads] == [4, 4]
    assert all(p["worker_function"] == "worker" for p in invoker.payloads)


def test_the_reported_increments_add_up_to_every_input():
    """
    Each partition counts only its own and each frame only what is new,
    so a bar is the running sum of every report. Which partition's frame
    lands first is not fixed, so only the sum is asserted.
    """
    invoker = RecordingInvoker(
        [
            [
                completed(succeeded=2),
                completed(succeeded=3),
                {"Summary": dispatcher_summary(5)},
            ],
            [
                completed(succeeded=4),
                completed(succeeded=1),
                {"Summary": dispatcher_summary(5)},
            ],
        ]
    )
    steps: list[int] = []

    _map(invoker, on_progress=steps.append)

    assert sum(steps) == 10
    # Increments, so a report is what one frame finished - never a total.
    assert sorted(steps) == [1, 2, 3, 4]


def test_increments_need_no_session_to_add_up_across_calls():
    """
    A caller spanning several calls - a rechunk is one per copy
    operation - just keeps adding. Nothing restarts at zero, so there is
    no call boundary for it to know about.
    """
    invoker = RecordingInvoker([dispatcher_summary(5)] * 4)
    steps: list[int] = []
    session = _session(invoker, on_progress=steps.append)

    async def two_calls() -> None:
        for _ in range(2):
            await session.for_each("worker", list(range(10)), input_key="i")

    asyncio.run(two_calls())

    assert sum(steps) == 20


def test_a_failed_invocation_raises_on_the_frame_that_reports_it():
    """
    A failure is terminal - retries are already spent - so the call
    cannot succeed and there is nothing to gain by reading on. The
    second partition's frames are never reached, and the message is the
    worker's own.
    """
    invoker = RecordingInvoker(
        [
            [
                completed(succeeded=1, first_error="worker said no (req_id=w-7)"),
                completed(succeeded=2),
                {
                    "Summary": dispatcher_summary(
                        3, dispatched=5, transport_errors=2, first_error="worker said no"
                    )
                },
            ],
            [
                completed(succeeded=5),
                {"Summary": dispatcher_summary(5)},
            ],
        ]
    )
    steps: list[int] = []

    with pytest.raises(RuntimeError, match=r"cannot succeed: worker said no \(req_id=w-7\)"):
        _map(invoker, on_progress=steps.append)

    # Raised on the frame rather than after it, so the one done
    # alongside the failures never reached the bar.
    assert steps == []


def test_for_each_asks_for_no_answers_and_map_unordered_asks_for_them():
    """
    A worker's body would be streamed back only to be dropped here, which
    at a partition's width is one per input.
    """
    invoker = RecordingInvoker([dispatcher_summary(10), dispatcher_summary(10)])
    session = _session(invoker, dispatchers=1)

    asyncio.run(session.for_each("worker", list(range(10)), input_key="i"))
    drain(session.map_unordered("worker", list(range(10)), input_key="i"))

    assert [payload["return_outputs"] for payload in invoker.payloads] == [False, True]


def test_a_dispatcher_record_per_partition_is_stamped_with_its_call():
    invoker = RecordingInvoker([dispatcher_summary(5), dispatcher_summary(5)])
    collected: list[dict] = []

    _map(invoker, dispatcher_stats=collected)

    assert len(collected) == 2
    assert len({r["call"] for r in collected}) == 1


def test_every_partitions_record_is_kept_when_one_of_them_fails():
    """A losing run is when the records are most wanted."""
    invoker = RecordingInvoker([dispatcher_summary(4), dispatcher_summary(5)])
    collected: list[dict] = []

    with pytest.raises(RuntimeError, match="completed 4 of 5"):
        _map(invoker, dispatcher_stats=collected)

    assert len(collected) == 2


def test_a_dispatcher_record_does_not_carry_its_partitions_invocations():
    """
    They come back through `invocation_stats`, numbered against the
    caller's inputs. Nested in the partition's record they would be a
    second copy, still numbered against the partition.
    """
    stats = [{"index": 0, "req_id": "w-0", "invoke_start": 1.0, "invoke_end": 2.0}]
    invoker = RecordingInvoker(
        [
            [completed(succeeded=5, invocation_stats=stats), {"Summary": dispatcher_summary(5)}],
            dispatcher_summary(5),
        ]
    )
    partitions: list[dict] = []
    invocations: list[dict] = []

    _map(invoker, dispatcher_stats=partitions, invocation_stats=invocations)

    assert all("invocation_stats" not in record for record in partitions)
    assert len(invocations) == 1


def test_invocation_records_are_stamped_with_the_call_they_belong_to():
    records = [{"index": 0, "req_id": "w-0", "invoke_start": 1.0, "invoke_end": 2.0}]
    invoker = RecordingInvoker(
        [
            [completed(succeeded=5, invocation_stats=records), {"Summary": dispatcher_summary(5)}],
            dispatcher_summary(5),
        ]
    )
    collected: list[dict] = []

    _map(invoker, invocation_stats=collected)

    assert len(collected) == 1
    assert "call" in collected[0]
    assert collected[0]["req_id"] == "w-0"


def test_invocation_records_are_not_requested_with_nowhere_to_put_them():
    """Otherwise the dispatcher still builds and sends them."""
    invoker = RecordingInvoker([dispatcher_summary(5), dispatcher_summary(5)])

    _map(invoker, invocation_stats=None)

    assert all(p["return_stats"] is False for p in invoker.payloads)


def test_a_partitions_own_numbering_is_read_back_as_a_place_in_the_callers_inputs():
    """A dispatcher's `index` is partition-local: partition 1's third input is `inputs[5]`."""
    records = [
        {"index": 2, "req_id": "w-0", "invoke_start": 1.0, "invoke_end": 2.0},
        {"index": 4, "req_id": "w-1", "invoke_start": 1.0, "invoke_end": 2.0},
    ]
    sent = [
        {"index": 2, "value": {"ok": True}},
        {"index": 4, "value": {"ok": True}},
    ]
    # partition 0 returns nothing; partition 1 holds inputs[1::2] == [1, 3, 5, 7, 9]
    invoker = RecordingInvoker(
        [
            dispatcher_summary(5),
            [
                completed(succeeded=5, outputs=sent, invocation_stats=records),
                {"Summary": dispatcher_summary(5)},
            ],
        ]
    )
    collected: list[dict] = []

    outputs = _map(invoker, invocation_stats=collected)

    assert [r["index"] for r in collected] == [5, 9]
    # the same conversion reaches the outputs, which are all there is to
    # say which input each one ran
    assert [index for index, _ in outputs] == [5, 9]


def test_equal_inputs_are_still_told_apart_by_index():
    """Each output's place comes from its frame, not from matching inputs."""
    sent = [{"index": i, "value": {"seen": 7}} for i in range(3)]
    records = [
        {"index": i, "req_id": f"w-{i}", "invoke_start": 1.0, "invoke_end": 2.0} for i in range(3)
    ]
    invoker = RecordingInvoker(
        [
            [
                completed(succeeded=3, outputs=sent, invocation_stats=records),
                {"Summary": dispatcher_summary(3)},
            ]
        ]
    )
    collected: list[dict] = []

    outputs = _map(invoker, [7, 7, 7], dispatchers=1, invocation_stats=collected)

    assert sorted(index for index, _ in outputs) == [0, 1, 2]
    assert sorted(r["index"] for r in collected) == [0, 1, 2]


def test_auto_sizes_the_session_and_an_explicit_count_still_wins():
    # `dispatcher_summary` reports one connection, so the concurrency here
    # stays inside what one of them carries; the Rust `dispatchers_for`
    # tests cover the arithmetic above 1,600.
    invoker = RecordingInvoker([dispatcher_summary(10)])
    session = Session(concurrency=100)
    session._invoker = invoker
    asyncio.run(session.for_each("worker", list(range(10)), input_key="i"))
    assert len(invoker.payloads) == 1

    invoker = RecordingInvoker([dispatcher_summary(5), dispatcher_summary(5)])
    session = Session(dispatchers=2, concurrency=100)
    session._invoker = invoker
    asyncio.run(session.for_each("worker", list(range(10)), input_key="i"))
    assert len(invoker.payloads) == 2


@pytest.mark.parametrize("constant_args", [[("prefix", "p")], []])
def test_constant_args_that_are_not_a_dict_are_refused_before_dispatching(constant_args):
    """
    The dispatcher builds the worker's payload on them, so
    anything else fails inside AWS with every invocation already billed
    for. The annotation says `dict` and mypy agrees, which is why the
    call below has to be hidden from it - the guard is for callers built
    at runtime, where nothing checked. An empty one is no exception.
    """
    invoker = RecordingInvoker([dispatcher_summary(10)])
    session = _session(invoker, dispatchers=1)

    with pytest.raises(TypeError, match="constant_args must be a dict, not list"):
        asyncio.run(
            session.for_each(
                "worker",
                [1, 2],
                input_key="i",
                constant_args=constant_args,
            )
        )

    assert invoker.payloads == [], "nothing should reach the wire"


def test_a_dispatcher_that_stops_partway_raises_rather_than_looking_done():
    """
    A stream with no summary frame means work was left undone and
    unreported, which no count would show.
    """
    invoker = RecordingInvoker([[completed(succeeded=1)]])
    session = _session(invoker, dispatchers=1)

    with pytest.raises(RuntimeError, match="no summary frame"):
        asyncio.run(session.for_each("worker", [1, 2], input_key="i"))


def test_a_partition_over_lambdas_request_limit_raises_before_dispatching():
    """
    Lambda would refuse it as RequestTooLargeException with nothing run,
    naming neither the partition nor what to do about it.
    """
    invoker = RecordingInvoker([dispatcher_summary(2)])
    session = Session(dispatchers=1)
    session._invoker = invoker
    oversized = ["x" * 4_000_000, "y" * 4_000_000]

    with pytest.raises(ValueError, match="over Lambda's 6 MiB request limit"):
        asyncio.run(session.for_each("worker", oversized, input_key="i"))

    assert invoker.payloads == [], "nothing should reach the wire"
