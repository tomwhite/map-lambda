"""
The wire, from this side: how work is partitioned into requests, what a
coordinator's stream is allowed to say, and what a client refuses to
believe.
"""

import asyncio
import json

import pytest

from map_lambda import DEFAULT_CONCURRENCY, coordinator
from map_lambda.testing import completed, coordinator_body, coordinator_stream


class RecordingClient:
    """A Lambda client that records payloads and replays canned responses.

    Each canned response is either a result dict - streamed as a prelude
    and one result frame, which is the minimum a coordinator can send -
    or a list of frames to stream in order.
    """

    def __init__(self, responses):
        self.responses = list(responses)
        self.payloads: list[dict] = []

    async def invoke_with_response_stream(self, *, FunctionName, Payload):
        self.payloads.append(json.loads(Payload))
        canned = self.responses.pop(0)
        frames = canned if isinstance(canned, list) else [{"Result": canned}]
        return {"EventStream": coordinator_stream([coordinator_body(*frames)])}


def _response(succeeded, stats=(), **kwargs):
    return {
        "req_id": "coord-0",
        "dispatched": succeeded,
        "succeeded": succeeded,
        "function_errors": 0,
        "transport_errors": 0,
        "retried": 0,
        "first_error": None,
        "cold": False,
        "connections": 1,
        "elapsed_ms": 1.0,
        "stats": list(stats),
        **kwargs,
    }


def test_partitions_are_strided_not_contiguous():
    assert coordinator.partition(list(range(10)), 0, 3) == [0, 3, 6, 9]
    assert coordinator.partition(list(range(10)), 1, 3) == [1, 4, 7]
    assert coordinator.partition(list(range(10)), 2, 3) == [2, 5, 8]


def test_partitions_cover_the_inputs_exactly_once():
    """An input in no partition is a run that succeeds with a hole in its output."""
    inputs = list(range(10))
    seen = [x for index in range(3) for x in coordinator.partition(inputs, index, 3)]
    assert sorted(seen) == inputs


def test_more_coordinators_than_inputs_leaves_some_with_nothing():
    assert coordinator.partition([0, 1], 0, 5) == [0]
    assert coordinator.partition([0, 1], 2, 5) == []


def test_concurrency_splits_as_evenly_as_possible():
    assert coordinator.split(500, 8) == [63, 63, 63, 63, 62, 62, 62, 62]
    assert sum(coordinator.split(500, 8)) == 500
    assert coordinator.split(4, 4) == [1, 1, 1, 1]


def test_a_concurrency_too_small_to_split_is_refused():
    # Silently giving a coordinator zero in flight would hang it.
    with pytest.raises(ValueError, match="too small to split"):
        coordinator.split(0, 4)


def test_a_share_no_coordinator_could_carry_is_refused():
    """
    Before anything is invoked: past this a coordinator has no more
    connections to open, so the surplus queues and the run is correct and
    several times slower. `check` catches the same thing from the far end,
    once that run has already happened.
    """
    limit = coordinator.MAX_IN_FLIGHT_PER_COORDINATOR
    assert max(coordinator.split(limit, 1)) == limit
    assert max(coordinator.split(limit * 2, 2)) == limit

    with pytest.raises(ValueError, match="more than the 1600 one can carry"):
        coordinator.split(limit + 1, 1)
    # the remainder lands on the earliest share, so this is over by one there
    with pytest.raises(ValueError, match="Use at least 3 coordinators"):
        coordinator.split(limit * 2 + 1, 2)


def test_a_local_coordinator_carries_one_connections_worth():
    """
    A local run builds a single throwaway client whatever the concurrency,
    so its ceiling is far below a deployed one's - and `DEFAULT_CONCURRENCY`
    sits exactly on it.
    """
    limit = coordinator.MAX_IN_FLIGHT_PER_LOCAL_COORDINATOR
    assert max(coordinator.split(limit, 1, local=True)) == limit
    assert coordinator.split(DEFAULT_CONCURRENCY, 1, local=True) == [DEFAULT_CONCURRENCY]

    with pytest.raises(ValueError, match="more than the 100 a local coordinator can carry"):
        coordinator.split(limit + 1, 1, local=True)
    # the same share is fine against real Lambda, which opens 16 connections
    assert max(coordinator.split(limit + 1, 1)) == limit + 1


def test_at_least_one_coordinator_is_needed():
    with pytest.raises(ValueError, match="at least one coordinator"):
        coordinator.split(500, 0)


def test_check_passes_a_clean_full_partition():
    coordinator.check(_response(42), expected=42, in_flight=1, counted=42)


def test_check_raises_on_a_reported_failure():
    response = _response(41, function_errors=1, first_error="boom")
    with pytest.raises(RuntimeError, match="1 function"):
        coordinator.check(response, expected=42, in_flight=1, counted=41)


def test_check_raises_on_a_short_partition_with_no_error():
    """The dangerous case: work silently skipped."""
    with pytest.raises(RuntimeError, match="completed 41 of 42"):
        coordinator.check(_response(41, dispatched=42), expected=42, in_flight=1, counted=41)


def test_check_raises_when_the_coordinator_got_fewer_inputs_than_were_sent():
    """A request that did not arrive whole: a wire problem, unlike a short partition."""
    with pytest.raises(RuntimeError, match="did not arrive intact"):
        coordinator.check(_response(41), expected=42, in_flight=1, counted=41)


def test_check_raises_when_the_frames_do_not_account_for_the_whole_partition():
    """
    A coordinator can only report what it sent; this is the one check
    that is about what *arrived*, so a frame lost between the two has
    somewhere to show up.
    """
    with pytest.raises(RuntimeError, match="read incompletely"):
        coordinator.check(_response(42), expected=42, in_flight=1, counted=40)


def test_check_raises_when_a_coordinator_has_fewer_streams_than_it_was_asked_for():
    """The dangerous shape of failure: correct, complete, and several times too slow."""
    with pytest.raises(RuntimeError, match="would queue the rest"):
        coordinator.check(_response(42, connections=1), expected=42, in_flight=900, counted=42)


def test_check_passes_when_the_streams_exactly_cover_the_request():
    coordinator.check(
        _response(42, connections=1),
        expected=42,
        in_flight=coordinator.MAX_STREAMS_PER_CONNECTION,
        counted=42,
    )


async def _frames(payload_chunks):
    return [f async for f in coordinator.frames(coordinator_stream(payload_chunks))]


def test_the_metadata_prelude_is_not_parsed_as_a_frame():
    got = asyncio.run(_frames([coordinator_body({"Completed": {"done": 1}})]))
    assert got == [{"Completed": {"done": 1}}]


def test_a_frame_split_across_payload_chunks_is_reassembled():
    body = coordinator_body({"Completed": {"done": 7}})
    got = asyncio.run(_frames([body[:20], body[20:35], body[35:]]))
    assert got == [{"Completed": {"done": 7}}]


def test_several_frames_in_one_payload_chunk_all_arrive():
    body = coordinator_body({"Completed": {"done": 1}}, {"Completed": {"done": 2}})
    got = asyncio.run(_frames([body]))
    assert [f["Completed"]["done"] for f in got] == [1, 2]


def test_a_stream_with_no_prelude_is_read_too():
    """As on a local run. Waiting for a prelude would never yield."""
    body = b"".join(
        json.dumps(f).encode() + b"\n"
        for f in ({"Completed": {"done": 1}}, {"Result": {"succeeded": 1}})
    )
    got = asyncio.run(_frames([body]))
    assert got == [{"Completed": {"done": 1}}, {"Result": {"succeeded": 1}}]


def test_a_prelude_split_before_its_separator_is_still_waited_for():
    """A payload chunk that is all prelude, no NUL yet."""
    body = coordinator_body({"Completed": {"done": 1}})
    cut = body.index(b"\x00") - 3
    got = asyncio.run(_frames([body[:cut], body[cut:]]))
    assert got == [{"Completed": {"done": 1}}]


def test_a_payload_chunk_boundary_on_the_prelude_separator_still_works():
    body = coordinator_body({"Completed": {"done": 3}})
    cut = body.index(coordinator.PRELUDE_SEPARATOR) + 4
    got = asyncio.run(_frames([body[:cut], body[cut:]]))
    assert got == [{"Completed": {"done": 3}}]


def test_an_error_mid_stream_raises():
    """The HTTP status was 200 long before the failure, so this is the only signal."""

    async def events():
        yield {"PayloadChunk": {"Payload": coordinator_body({"Completed": {"done": 1}})}}
        yield {"InvokeComplete": {"ErrorCode": "Runtime.ExitError", "ErrorDetails": "killed"}}

    async def drain():
        return [f async for f in coordinator.frames(events())]

    with pytest.raises(RuntimeError, match=r"Runtime\.ExitError"):
        asyncio.run(drain())


def _partition(client, **kwargs):
    """
    Every frame one partition yielded, and the `(response, counted)` it
    reported once its result arrived.
    """
    frames: list[coordinator.Completed] = []
    reported: list[tuple[dict, int]] = []

    async def go():
        async for batch in coordinator.stream_partition(
            client,
            "coordinator",
            "{}",
            lambda response, counted: reported.append((response, counted)),
            **kwargs,
        ):
            frames.append(batch)

    asyncio.run(go())
    return frames, reported


def test_a_stream_with_no_result_frame_raises():
    """A coordinator that stopped partway leaves work undone and says nothing."""
    client = RecordingClient([[completed(done=4)]])
    with pytest.raises(RuntimeError, match="no result frame"):
        _partition(client)


def test_an_unknown_frame_kind_raises_rather_than_being_ignored():
    client = RecordingClient([[{"something-new": {}}]])
    with pytest.raises(RuntimeError, match="unknown frame kind"):
        _partition(client)


def test_every_frame_is_yielded_as_it_arrives_and_the_result_reported():
    """
    Including the empty one opening the stream, and the total reported
    at the end is what `check` holds the result against.
    """
    client = RecordingClient(
        [
            [
                completed(),
                completed(done=3),
                {"Result": _response(5)},
            ]
        ]
    )

    frames, reported = _partition(client)

    assert [c.done for c in frames] == [0, 3]
    # The body verbatim: it goes on to be a coordinator_stats record, so
    # the tag must not travel with it and no field may be lost.
    assert reported == [(_response(5), 3)]


def test_the_answers_and_records_on_a_frame_come_through_together():
    """
    A partition sends what finished since its last frame, so the two
    lists on one frame describe the same invocations.
    """
    client = RecordingClient(
        [
            [
                completed(
                    done=1,
                    outputs=[{"index": 0, "response": {"chunks": 1}}],
                    stats=[{"index": 0, "req_id": "w-1"}],
                ),
                completed(done=1, outputs=[{"index": 1, "response": {"chunks": 2}}]),
                {"Result": _response(2)},
            ]
        ]
    )

    frames, _ = _partition(client)

    assert [[a["index"] for a in c.outputs] for c in frames] == [[0], [1]]
    assert [[a["response"]["chunks"] for a in c.outputs] for c in frames] == [[1], [2]]
    assert [[s["req_id"] for s in c.stats] for c in frames] == [["w-1"], []]


def test_an_input_can_be_a_plain_value():
    payload = json.loads(
        coordinator.partition_payload(
            {"path": "/tmp/x"},
            [0, 1, 2],
            4,
            target_function="echo",
            input_key="i",
            retries=0,
            return_stats=True,
            return_outputs=True,
        )
    )

    assert payload["inputs"] == [0, 1, 2]
