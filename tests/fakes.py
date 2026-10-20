"""
A Lambda client that answers however a test says, standing in for AWS
behind the real client: `RecordingClient`, and the frames it replays.
"""

import json
from collections.abc import Sequence
from typing import Any

# The metadata prelude a deployed coordinator's stream opens with, which
# the client skips.
COORDINATOR_PRELUDE = b'{"statusCode":200,"headers":{}}' + b"\x00" * 8


def coordinator_body(*frames: Any) -> bytes:
    """The bytes a coordinator writes: a prelude, then one JSON line each."""
    return COORDINATOR_PRELUDE + b"".join(json.dumps(f).encode() + b"\n" for f in frames)


def coordinator_stream(chunks: list[bytes]) -> Any:
    """An EventStream over raw byte chunks, as aiobotocore yields them."""

    async def events() -> Any:
        for chunk in chunks:
            yield {"PayloadChunk": {"Payload": chunk}}
        yield {"InvokeComplete": {}}

    return events()


def completed(
    done: int = 0, outputs: Any = (), stats: Any = (), first_error: str | None = None
) -> dict[str, Any]:
    """
    One `Completed` frame: what a partition finished since its last one,
    so `done` is a delta and the lists hold only what is new.

    `first_error` is how a frame reports a failure, and the call raises
    on it.
    """
    return {
        "Completed": {
            "done": done,
            "outputs": list(outputs),
            "stats": list(stats),
            "first_error": first_error,
        }
    }


def coordinator_result(succeeded: int, **overrides: Any) -> dict[str, Any]:
    """
    One coordinator's result frame, with every field the client checks.

    `succeeded` sets `dispatched` too, because the ordinary case is a
    partition that did all of its work; `**overrides` is how a test says
    otherwise - `dispatched=2` for a short partition, `transport_errors=1`
    for a failed one.

    Answers and records are not here: they ride on the `Completed`
    frames, and a result frame repeating them would be a second copy of
    the partition.
    """
    return {
        "req_id": "coord-0",
        "dispatched": succeeded,
        "succeeded": succeeded,
        "function_errors": 0,
        "transport_errors": 0,
        "retried": 0,
        "retry_wait_ms": 0.0,
        "first_error": None,
        "cold": False,
        "connections": 1,
        "elapsed_ms": 1.0,
        **overrides,
    }


class RecordingClient:
    """
    A Lambda client that records the requests it was sent and replays
    canned responses. Use it to assert **what went on the wire**, and to
    put a coordinator into a state a real one is hard to provoke, like a
    short partition or a transport error.

    Each canned response is either a result dict (`coordinator_result`),
    streamed as the frames accounting for its work and then the result,
    or a list of frames to stream verbatim - which is how a test sends a
    stream that does *not* add up.
    """

    def __init__(
        self, responses: Sequence[Any] = (), invoke_error: Exception | None = None
    ) -> None:
        self.responses = list(responses)
        # One per coordinator invocation, i.e. per partition.
        self.payloads: list[dict] = []
        # One `(FunctionName, payload)` per `Lambdas.invoke`.
        self.invoked: list[tuple[str, dict]] = []
        self.invoke_error = invoke_error

    async def invoke(self, *, FunctionName: str, InvocationType: str, Payload: str) -> Any:
        """
        What `Lambdas.invoke` reaches, recorded rather than sent.

        `invoke_error` makes it raise instead, as a missing function or
        a role that may not invoke it would.
        """
        if self.invoke_error is not None:
            raise self.invoke_error
        self.invoked.append((FunctionName, json.loads(Payload)))
        return {"StatusCode": 202}

    async def invoke_with_response_stream(self, *, FunctionName: str, Payload: str) -> Any:
        self.payloads.append(json.loads(Payload))
        canned = self.responses.pop(0)
        if isinstance(canned, list):
            frames = canned
        else:
            frames = [
                completed(done=canned["succeeded"], first_error=canned.get("first_error")),
                {"Result": canned},
            ]
        return {"EventStream": coordinator_stream([coordinator_body(*frames)])}
