"""
`coordinator.stream`: the whole client-side path with the invocation
faked, rather than its pieces one at a time.

`test_coordinator.py` tests the pieces; this is what only shows up when
they run together.
"""

import asyncio
from typing import Any

import pytest

from map_lambda import Lambdas, coordinator
from map_lambda.testing import RecordingClient, completed, coordinator_result

from .utils import drain


def _session(client: RecordingClient, **settings: Any) -> Lambdas:
    """A `Lambdas` whose Lambda client answers without invoking anything."""
    lambdas = Lambdas(**{"coordinators": 2, "concurrency": 8, **settings})
    lambdas._client = client
    return lambdas


def _map(
    client: RecordingClient, inputs: list[Any] | None = None, **settings: Any
) -> list[tuple[int, Any]]:
    """
    One `map_unordered` over `inputs` through `client`, drained, with
    nothing else going on.
    """
    lambdas = _session(client, **settings)
    return drain(
        lambdas.map_unordered(
            "worker",
            list(range(10)) if inputs is None else inputs,
            input_key="i",
            constant_args={"prefix": "p"},
        )
    )  # fmt: skip


def test_every_coordinator_is_invoked_with_its_own_partition_and_its_share():
    """10 inputs over 2 coordinators, 8 in flight split 4/4."""
    client = RecordingClient([coordinator_result(5), coordinator_result(5)])

    _map(client)

    assert [p["inputs"] for p in client.payloads] == [[0, 2, 4, 6, 8], [1, 3, 5, 7, 9]]
    assert [p["concurrency"] for p in client.payloads] == [4, 4]
    assert all(p["target_function"] == "worker" for p in client.payloads)


def test_the_reported_increments_add_up_to_every_input():
    """
    Each partition counts only its own and each frame only what is new,
    so a bar is the running sum of every report. Which partition's frame
    lands first is not fixed, so only the sum is asserted.
    """
    client = RecordingClient(
        [
            [
                completed(done=2),
                completed(done=3),
                {"Result": coordinator_result(5)},
            ],
            [
                completed(done=4),
                completed(done=1),
                {"Result": coordinator_result(5)},
            ],
        ]
    )
    steps: list[int] = []

    _map(client, on_progress=steps.append)

    assert sum(steps) == 10
    # Increments, so a report is what one frame finished - never a total.
    assert sorted(steps) == [1, 2, 3, 4]


def test_increments_need_no_session_to_add_up_across_calls():
    """
    A caller spanning several calls - a rechunk is one per copy
    operation - just keeps adding. Nothing restarts at zero, so there is
    no call boundary for it to know about.
    """
    client = RecordingClient([coordinator_result(5)] * 4)
    steps: list[int] = []
    lambdas = _session(client, on_progress=steps.append)

    async def two_calls() -> None:
        for _ in range(2):
            await lambdas.for_each("worker", list(range(10)), input_key="i")

    asyncio.run(two_calls())

    assert sum(steps) == 20


def test_a_failed_invocation_raises_on_the_frame_that_reports_it():
    """
    A failure is terminal - retries are already spent - so the call
    cannot succeed and there is nothing to gain by reading on. The
    second partition's frames are never reached, and the message is the
    worker's own.
    """
    client = RecordingClient(
        [
            [
                completed(done=1, first_error="worker said no (req_id=w-7)"),
                completed(done=2),
                {
                    "Result": coordinator_result(
                        3, dispatched=5, transport_errors=2, first_error="worker said no"
                    )
                },
            ],
            [
                completed(done=5),
                {"Result": coordinator_result(5)},
            ],
        ]
    )
    steps: list[int] = []

    with pytest.raises(RuntimeError, match=r"cannot succeed: worker said no \(req_id=w-7\)"):
        _map(client, on_progress=steps.append)

    # Raised on the frame rather than after it, so the one done
    # alongside the failures never reached the bar.
    assert steps == []


def test_for_each_asks_for_no_answers_and_map_unordered_asks_for_them():
    """
    A worker's body would be streamed back only to be dropped here, which
    at a partition's width is one per input.
    """
    client = RecordingClient([coordinator_result(10), coordinator_result(10)])
    lambdas = _session(client, coordinators=1)

    asyncio.run(lambdas.for_each("worker", list(range(10)), input_key="i"))
    drain(lambdas.map_unordered("worker", list(range(10)), input_key="i"))

    assert [payload["return_outputs"] for payload in client.payloads] == [False, True]


def test_a_coordinator_record_per_partition_is_stamped_with_its_call():
    client = RecordingClient([coordinator_result(5), coordinator_result(5)])
    collected: list[dict] = []

    _map(client, coordinator_stats=collected)

    assert len(collected) == 2
    assert len({r["call"] for r in collected}) == 1


def test_every_partitions_record_is_kept_when_one_of_them_fails():
    """A losing run is when the records are most wanted."""
    client = RecordingClient([coordinator_result(5, dispatched=6), coordinator_result(5)])
    collected: list[dict] = []

    with pytest.raises(RuntimeError, match="did not arrive intact"):
        _map(client, coordinator_stats=collected)

    assert len(collected) == 2


def test_a_coordinator_record_does_not_carry_its_partitions_invocations():
    """
    They come back through `invocation_stats`, numbered against the
    caller's inputs. Nested in the partition's record they would be a
    second copy, still numbered against the partition.
    """
    stats = [{"index": 0, "req_id": "w-0", "invoke_start": 1.0, "invoke_end": 2.0}]
    client = RecordingClient(
        [
            [completed(done=5, stats=stats), {"Result": coordinator_result(5)}],
            coordinator_result(5),
        ]
    )
    partitions: list[dict] = []
    invocations: list[dict] = []

    _map(client, coordinator_stats=partitions, invocation_stats=invocations)

    assert all("stats" not in record for record in partitions)
    assert len(invocations) == 1


def test_invocation_records_are_stamped_with_the_call_they_belong_to():
    records = [{"index": 0, "req_id": "w-0", "invoke_start": 1.0, "invoke_end": 2.0}]
    client = RecordingClient(
        [
            [completed(done=5, stats=records), {"Result": coordinator_result(5)}],
            coordinator_result(5),
        ]
    )
    collected: list[dict] = []

    _map(client, invocation_stats=collected)

    assert len(collected) == 1
    assert "call" in collected[0]
    assert collected[0]["req_id"] == "w-0"


def test_invocation_records_are_not_requested_with_nowhere_to_put_them():
    """Otherwise the coordinator still builds and sends them."""
    client = RecordingClient([coordinator_result(5), coordinator_result(5)])

    _map(client, invocation_stats=None)

    assert all(p["return_stats"] is False for p in client.payloads)


def test_a_partitions_own_numbering_is_read_back_as_a_place_in_the_callers_inputs():
    """A coordinator's `index` is partition-local: partition 1's third input is `inputs[5]`."""
    records = [
        {"index": 2, "req_id": "w-0", "invoke_start": 1.0, "invoke_end": 2.0},
        {"index": 4, "req_id": "w-1", "invoke_start": 1.0, "invoke_end": 2.0},
    ]
    sent = [
        {"index": 2, "response": {"ok": True}},
        {"index": 4, "response": {"ok": True}},
    ]
    # partition 0 answers nothing; partition 1 holds inputs[1::2] == [1, 3, 5, 7, 9]
    client = RecordingClient(
        [
            coordinator_result(5),
            [
                completed(done=5, outputs=sent, stats=records),
                {"Result": coordinator_result(5)},
            ],
        ]
    )
    collected: list[dict] = []

    answers = _map(client, invocation_stats=collected)

    assert [r["index"] for r in collected] == [5, 9]
    # the same conversion reaches the answers, which are all there is to
    # say which input each one ran
    assert [index for index, _ in answers] == [5, 9]


def test_equal_inputs_are_still_told_apart_by_index():
    from map_lambda.testing import session

    records: list[dict] = []
    lambdas = session(
        lambda payload: {"seen": payload["i"]}, coordinators=1, invocation_stats=records
    )

    answers = drain(lambdas.map_unordered("worker", [7, 7, 7], input_key="i"))

    # the answers are indistinguishable; the indices are what tell them apart
    assert all(answer == {"seen": 7} for _, answer in answers)
    assert sorted(index for index, _ in answers) == [0, 1, 2]
    assert sorted(r["index"] for r in records) == [0, 1, 2]


def test_auto_takes_the_fewest_coordinators_that_hold_the_concurrency():
    assert coordinator.coordinators_for(1) == 1
    assert coordinator.coordinators_for(900) == 1
    assert coordinator.coordinators_for(coordinator.MAX_IN_FLIGHT_PER_COORDINATOR) == 1
    assert coordinator.coordinators_for(coordinator.MAX_IN_FLIGHT_PER_COORDINATOR + 1) == 2
    assert coordinator.coordinators_for(5000) == 4
    # a local coordinator carries one connection's worth, so it needs more
    assert coordinator.coordinators_for(500, local=True) == 5


def test_auto_sizes_the_session_and_an_explicit_count_still_wins():
    # `coordinator_result` reports one connection, so the concurrency here
    # stays inside what one of them carries; `coordinators_for` covers the
    # arithmetic above 1,600.
    client = RecordingClient([coordinator_result(10)])
    lambdas = Lambdas(concurrency=100)
    lambdas._client = client
    asyncio.run(lambdas.for_each("worker", list(range(10)), input_key="i"))
    assert len(client.payloads) == 1

    client = RecordingClient([coordinator_result(5), coordinator_result(5)])
    lambdas = Lambdas(coordinators=2, concurrency=100)
    lambdas._client = client
    asyncio.run(lambdas.for_each("worker", list(range(10)), input_key="i"))
    assert len(client.payloads) == 2


def test_constant_args_that_are_not_a_dict_are_refused_before_dispatching():
    """
    The coordinator reads them as the worker's payload template, so
    anything else fails inside AWS with every invocation already billed
    for. The annotation says `dict` and mypy agrees, which is why the
    call below has to be hidden from it - the guard is for callers built
    at runtime, where nothing checked.
    """
    client = RecordingClient([coordinator_result(10)])
    lambdas = _session(client, coordinators=1)

    with pytest.raises(TypeError, match="constant_args must be a dict, not list"):
        asyncio.run(
            lambdas.for_each(
                "worker",
                [1, 2],
                input_key="i",
                constant_args=[("prefix", "p")],  # type: ignore[arg-type]
            )
        )

    assert client.payloads == [], "nothing should reach the wire"


def test_a_coordinator_that_stops_partway_raises_rather_than_looking_done():
    """
    A stream with no result frame means work was left undone and
    unreported, which no count would show.
    """
    client = RecordingClient([[completed(done=1)]])
    lambdas = _session(client, coordinators=1)

    with pytest.raises(RuntimeError, match="no result frame"):
        asyncio.run(lambdas.for_each("worker", [1, 2], input_key="i"))


def test_a_partition_over_lambdas_request_limit_raises_before_dispatching():
    """
    Lambda would refuse it as RequestTooLargeException with nothing run,
    naming neither the partition nor what to do about it.
    """
    client = RecordingClient([coordinator_result(2)])
    lambdas = Lambdas(coordinators=1)
    lambdas._client = client
    oversized = ["x" * 4_000_000, "y" * 4_000_000]

    with pytest.raises(ValueError, match="over Lambda's 6 MiB request limit"):
        asyncio.run(lambdas.for_each("worker", oversized, input_key="i"))

    assert client.payloads == [], "nothing should reach the wire"
