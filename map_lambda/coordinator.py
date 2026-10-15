"""
Dispatching work through coordinator lambdas: the client half.

Python invokes N `coordinator` lambdas, each of which dispatches a
partition of the inputs from inside AWS and streams back
completed frames, then one result frame. The wire contract is
rust/protocol/src/lib.rs.
"""

import contextlib
import dataclasses
import json
import time
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass
from typing import Any, TypedDict

import aiostream


class Answer(TypedDict):
    """
    One invocation's answer, from a completed frame (`Output` in
    rust/protocol/src/lib.rs).

    `index` is the position in the caller's `inputs`, converted from
    partition-local by `stream_partition`. `response` is the worker's
    body, verbatim.
    """

    index: int
    response: Any


@dataclass(frozen=True, slots=True)
class Completed:
    """
    One `Completed` frame (`Completed` in
    rust/protocol/src/lib.rs): what one partition finished since
    its last frame.
    """

    done: int
    """Invocations that came back successfully since the last frame."""
    outputs: list[Answer]
    """Empty unless the call asked for the workers' answers."""
    stats: list[dict[str, Any]]
    """Empty unless the call asked for per-invocation records."""
    first_error: str | None
    """
    The first invocation of this frame to fail every attempt, as its own
    message.
    """


@dataclass(frozen=True, slots=True)
class Response:
    """
    One `Result` frame (`Response` in rust/protocol/src/lib.rs): how one
    partition went, as its coordinator saw it.

    Read field by field rather than kept as a dict, so a field renamed on
    either side fails to parse instead of reading as missing.
    """

    req_id: str
    dispatched: int
    succeeded: int
    function_errors: int
    transport_errors: int
    retried: int
    retry_wait_ms: float
    first_error: str | None
    cold: bool | None
    connections: int
    elapsed_ms: float


# Told one partition's result frame, and how many its frames counted done.
type Outcome = Callable[[Response, int], None]


# Lambda's maximum function duration.
COORDINATOR_READ_TIMEOUT_SECONDS = 900

# Lambda's SETTINGS_MAX_CONCURRENT_STREAMS: the hard limit, past which
# requests queue.
MAX_STREAMS_PER_CONNECTION = 128

# What a coordinator actually puts through one connection: the limit above, less
# headroom. Keep in step with STREAMS_PER_CONNECTION in
# rust/protocol/src/lib.rs.
STREAMS_PER_CONNECTION = 100

# MAX_IN_FLIGHT in rust/protocol/src/lib.rs, which sizes a coordinator's
# clients from it. Keep the two in step.
MAX_IN_FLIGHT_PER_COORDINATOR = 1_600

# A local run builds one throwaway client whatever the concurrency, so
# what it carries is a single connection's worth.
MAX_IN_FLIGHT_PER_LOCAL_COORDINATOR = STREAMS_PER_CONNECTION

# Lambda's hard cap on a synchronous request payload, which the whole
# request has to fit: inputs, one copy of constant_args, and framing.
MAX_REQUEST_BYTES = 6 * 1024 * 1024

DEFAULT_COORDINATOR_FUNCTION = "coordinator"


def coordinators_for(concurrency: int, *, local: bool = False) -> int:
    """
    The fewest coordinators that can hold `concurrency` in flight.
    """
    carries = MAX_IN_FLIGHT_PER_LOCAL_COORDINATOR if local else MAX_IN_FLIGHT_PER_COORDINATOR
    return max(1, -(-concurrency // carries))


def partition(inputs: list[Any], index: int, count: int) -> list[Any]:
    """
    The slice of `inputs` coordinator `index` of `count` is responsible
    for, strided.

    Strided because inputs often get cheaper towards the end (a grid
    enumerated in C order puts its edge cases last), and contiguous
    blocks would hand the final partition a lighter slice.
    """
    return inputs[index::count]


def partition_payload(
    constant_args: dict[str, Any],
    inputs: list[Any],
    concurrency: int,
    *,
    target_function: str,
    input_key: str,
    retries: int,
    return_stats: bool,
    return_outputs: bool,
    endpoint_url: str | None = None,
) -> str:
    """One coordinator's whole request, as it goes on the wire."""
    request: dict[str, Any] = {
        "target_function": target_function,
        "template": constant_args,
        "inputs": inputs,
        "input_key": input_key,
        "concurrency": concurrency,
        "retries": retries,
        "return_stats": return_stats,
        "return_outputs": return_outputs,
    }
    if endpoint_url is not None:
        request["endpoint_url"] = endpoint_url
    return json.dumps(request)


def split(concurrency: int, count: int, *, local: bool = False) -> list[int]:
    """
    `concurrency` in flight, split as evenly as possible across `count`
    coordinators. Remainders go to the earliest partitions, which are also
    the ones striding gives an extra input.

    Raises if a share is more than one coordinator can carry, which a
    local one can far less of. Here rather than on the way back, so that a
    run nothing could have dispatched at the concurrency asked for costs
    nothing: `check` catches the same thing afterwards, from the
    connections a coordinator reports, by which time the slow run has
    happened.
    """
    if count <= 0:
        raise ValueError("a coordinator run needs at least one coordinator")
    base, extra = divmod(concurrency, count)
    if base == 0 and extra == 0:
        raise ValueError(f"concurrency {concurrency} is too small to split {count} ways")
    carries = MAX_IN_FLIGHT_PER_LOCAL_COORDINATOR if local else MAX_IN_FLIGHT_PER_COORDINATOR
    widest = base + (1 if extra else 0)
    if widest > carries:
        where = "a local coordinator" if local else "one"
        raise ValueError(
            f"concurrency {concurrency} over {count} coordinator(s) is {widest} in flight "
            f"each, more than the {carries} {where} can carry: it would queue the rest and "
            f"the run would just be slow. Use at least {-(-concurrency // carries)} "
            f"coordinators, or lower concurrency"
        )
    return [base + (1 if i < extra else 0) for i in range(count)]


def check(response: Response, expected: int, in_flight: int, counted: int) -> None:
    """
    Raise unless a coordinator did all of its partition, did it cleanly,
    and this side saw every frame it sent.

    A short partition is silent - skipped work just never happens - so the
    counts are checked as well as the errors, against `expected`, the
    number of inputs this side sent. `counted` is what the frames added
    up to here, which the result frame's own total has to agree with.
    """
    failures = response.function_errors + response.transport_errors
    if failures:
        raise RuntimeError(
            f"coordinator {response.req_id} reported {failures} failed invocation(s) "
            f"({response.function_errors} function, "
            f"{response.transport_errors} transport): {response.first_error}"
        )
    # Before `succeeded`, to tell inputs lost on the wire from work lost
    # while running.
    if response.dispatched != expected:
        raise RuntimeError(
            f"coordinator {response.req_id} dispatched {response.dispatched} "
            f"invocation(s) from the {expected} input group(s) it was sent - its request "
            f"did not arrive intact"
        )
    if response.succeeded != expected:
        raise RuntimeError(
            f"coordinator {response.req_id} completed {response.succeeded} "
            f"of {expected} invocation(s) in its partition with no error reported"
        )
    # Last, because a frame lost behind a real failure is the lesser
    # news: the counts above say what the coordinator believes, and
    # these say what arrived to be read.
    if counted != response.succeeded:
        raise RuntimeError(
            f"coordinator {response.req_id} reported "
            f"{response.succeeded} succeeded, but the frames that arrived account "
            f"for {counted} - its stream was read incompletely, so some answers or "
            f"records are missing"
        )
    # Too few streams doesn't fail, it queues: correct and slow. `split`
    # refuses this before dispatching, so reaching it means the two sides
    # disagree about how much a coordinator carries - the count below is
    # the one it really opened, against a limit it may no longer share.
    capacity = response.connections * MAX_STREAMS_PER_CONNECTION
    if capacity < in_flight:
        raise RuntimeError(
            f"coordinator {response.req_id} opened "
            f"{response.connections} connection(s), carrying {capacity} "
            f"streams, but was asked for {in_flight} in flight - it would queue the "
            f"rest and the run would just be slow. Raise MAX_IN_FLIGHT in "
            f"rust/protocol/src/lib.rs, or use more coordinators"
        )


# Lambda's response stream opens with a metadata prelude ending in eight
# NUL bytes, which the SDK does not strip. `cargo lambda watch` strips it
# itself.
PRELUDE_SEPARATOR = b"\x00" * 8


async def frames(event_stream: Any) -> Any:
    """
    The JSON objects a coordinator wrote, as they arrive.

    - The prelude is present on a deployed run and gone on a local one.
      Whichever of a NUL (prelude) or a newline (frame) comes first says
      which; guessing wrong either fails to parse or never yields.
    - A frame split across payload chunks is carried over.
    - A mid-stream failure arrives only in `InvokeComplete`: the HTTP
      status was 200 before the function had done anything.
    """
    buffered, prelude_settled = b"", False
    async for event in event_stream:
        payload_chunk = event.get("PayloadChunk", {}).get("Payload")
        if payload_chunk is None:
            complete = event.get("InvokeComplete", {})
            if complete.get("ErrorCode"):
                raise RuntimeError(
                    f"coordinator failed mid-stream: {complete['ErrorCode']} "
                    f"{complete.get('ErrorDetails', '')}".strip()
                )
            continue
        buffered += payload_chunk
        if not prelude_settled:
            nul, newline = buffered.find(b"\x00"), buffered.find(b"\n")
            if nul == -1 and newline == -1:
                continue
            if nul != -1 and (newline == -1 or nul < newline):
                if PRELUDE_SEPARATOR not in buffered:
                    continue
                _, buffered = buffered.split(PRELUDE_SEPARATOR, 1)
            prelude_settled = True
        *lines, buffered = buffered.split(b"\n")
        for line in lines:
            if line.strip():
                yield json.loads(line)


async def stream_partition(
    client: Any,
    function_name: str,
    payload: str,
    outcome: Outcome,
    partition: int = 0,
    count: int = 1,
) -> AsyncGenerator[Completed, None]:
    """
    Invoke one coordinator and yield each of its frames as it arrives,
    then hand `outcome` the result frame and what those frames counted.

    A frame counts only this partition's own work, and counts what
    happened since the one before it, so the total kept here is the
    partition's own - which is what `check` holds its result against.
    The partition is number `partition` of `count`, which is how its own
    numbering is read back as an index into the caller's `inputs`.

    **A stream that ends without a result frame raises**: the
    coordinator stopped partway, leaving work undone and unreported.
    """
    response = await client.invoke_with_response_stream(FunctionName=function_name, Payload=payload)
    if response.get("FunctionError"):
        raise RuntimeError(f"coordinator invocation failed: {response['FunctionError']}")

    done = 0
    result = None
    # Closed rather than left to the interpreter: an async generator has
    # no `yield from`, so iterating one does not close it.
    async with contextlib.aclosing(frames(response["EventStream"])) as arriving:
        async for frame in arriving:
            kind, body = frame.popitem()
            if kind == "Completed":
                # Partition k holds inputs[k::count].
                for entry in (*body["outputs"], *body["stats"]):
                    entry["index"] = partition + entry["index"] * count
                batch = Completed(**body)
                # Before yielding: a failure is terminal, so the call
                # cannot succeed and this frame's answers are moot.
                if batch.first_error is not None:
                    raise RuntimeError(
                        f"an invocation failed its last attempt, so the call cannot "
                        f"succeed: {batch.first_error}"
                    )
                done += batch.done
                yield batch
            elif kind == "Result":
                result = Response(**body)
            else:
                raise RuntimeError(f"coordinator sent an unknown frame kind: {kind!r}")
    if result is None:
        raise RuntimeError(
            "coordinator stream ended with no result frame - it stopped partway "
            "through its partition, so some of its work was never done"
        )
    outcome(result, done)


async def stream(
    client: Any,
    inputs: list[Any],
    *,
    constant_args: dict[str, Any],
    coordinator_function: str,
    target_function: str,
    input_key: str,
    coordinators: int,
    concurrency: int,
    retries: int,
    endpoint_url: str | None = None,
    on_done: Callable[[int], None] | None = None,
    coordinator_stats: list[dict[str, Any]] | None = None,
    invocation_stats: list[dict[str, Any]] | None = None,
    return_outputs: bool = True,
    call: int = 0,
) -> AsyncGenerator[list[Answer], None]:
    """
    Put `inputs` through `coordinators` coordinators, yielding each
    frame's worth of answers as it arrives. A coordinator that fails
    fails the run; nothing re-invokes it.

    Answers come in the order the workers finished, from every partition
    interleaved, and carry `index` into the caller's `inputs`. A worker
    that returned nothing sends no answer, so a run can yield fewer
    answers than it had inputs - and none at all when `return_outputs`
    is false, which tells the coordinators not to send them.

    `on_done` fires as each frame arrives, with what that one partition
    finished. `coordinator_stats` gets one record per partition, stamped
    with `call`, this call's index within its session.

    Each partition is checked only once every one of them has finished,
    so a losing run yields what did come back and then raises. An
    iterator abandoned part-way raises nothing, whatever the checking
    would have found, and keeps only the stats already recorded; closing
    it tears the partitions down with it.
    """
    # The coordinator reads it as the worker's payload template, so
    # anything else fails inside AWS with every invocation already billed
    # for.
    if not isinstance(constant_args, dict):
        raise TypeError(f"constant_args must be a dict, not {type(constant_args).__name__}")
    partitions = [partition(inputs, index, coordinators) for index in range(coordinators)]
    return_stats = invocation_stats is not None
    created = time.time()
    shares = split(concurrency, coordinators, local=endpoint_url is not None)

    payloads = [
        partition_payload(
            constant_args,
            partition_inputs,
            in_flight,
            target_function=target_function,
            input_key=input_key,
            retries=retries,
            return_stats=return_stats,
            return_outputs=return_outputs,
            endpoint_url=endpoint_url,
        )
        for partition_inputs, in_flight in zip(partitions, shares, strict=True)
    ]

    for index, oversized in enumerate(payloads):
        if len(oversized) > MAX_REQUEST_BYTES:
            need = -(-len(oversized) * coordinators // MAX_REQUEST_BYTES)
            raise ValueError(
                f"partition {index} is {len(oversized) / 1024 / 1024:.1f} MiB, over Lambda's "
                f"{MAX_REQUEST_BYTES // 1024 // 1024} MiB request limit: it would be refused "
                f"as RequestTooLargeException with nothing run. Use at least {need} "
                f"coordinators, or send smaller inputs"
            )

    settled: list[tuple[Response, int, int, int]] = []

    def outcome_for(expected: int, in_flight: int) -> Outcome:
        """What a partition is checked against, waiting for its result."""

        def record(response: Response, counted: int) -> None:
            settled.append((response, expected, in_flight, counted))

        return record

    partition_streams = [
        stream_partition(
            client,
            coordinator_function,
            payload,
            outcome_for(len(group), share),
            index,
            coordinators,
        )
        for index, (payload, group, share) in enumerate(
            zip(payloads, partitions, shares, strict=True)
        )
    ]

    # `merge` starts them together and interleaves what they yield, so a
    # frame reaches the caller as it lands rather than when its partition
    # finishes.
    async with aiostream.stream.merge(*partition_streams).stream() as arriving:
        async for batch in arriving:
            if invocation_stats is not None:
                for record in batch.stats:
                    record["call"] = call
                    record["call_start"] = created
                    invocation_stats.append(record)
            # The frame opening the stream carries nothing, and a bar
            # advanced by zero is just a flicker.
            if on_done is not None and batch.done:
                on_done(batch.done)
            if batch.outputs:
                yield batch.outputs

    if coordinator_stats is not None:
        for response, *_ in settled:
            record = dataclasses.asdict(response)
            coordinator_stats.append({**record, "call": call, "call_start": created})

    # Last, so that a losing run has already yielded what did come back.
    for response, expected, in_flight, counted in settled:
        check(response, expected, in_flight, counted)
