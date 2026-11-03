"""
An invoker that responds however a test says, standing in for AWS
behind the real client: `RecordingInvoker`, and the frames it replays.
"""

import json
from collections.abc import Sequence
from typing import Any

from map_lambda import _native

# The metadata prelude a deployed dispatcher's stream opens with, which
# the client skips.
DISPATCHER_PRELUDE = b'{"statusCode":200,"headers":{}}' + b"\x00" * 8


def lines(*frames: Any) -> bytes:
    """Frames as a dispatcher writes them, one JSON line each."""
    return b"".join(json.dumps(f).encode() + b"\n" for f in frames)


def dispatcher_body(*frames: Any) -> bytes:
    """A whole response body: the prelude, then the frames."""
    return DISPATCHER_PRELUDE + lines(*frames)


def completed(
    succeeded: int = 0,
    outputs: Any = (),
    invocation_stats: Any = (),
    first_error: str | None = None,
) -> dict[str, Any]:
    """
    One `Completed` frame: what a partition finished since its last one,
    so `succeeded` is a delta and the lists hold only what is new.

    `first_error` is how a frame reports a failure, and the call raises
    on it.
    """
    return {
        "Completed": {
            "succeeded": succeeded,
            "outputs": list(outputs),
            "invocation_stats": list(invocation_stats),
            "first_error": first_error,
        }
    }


def dispatcher_summary(dispatched: int, **overrides: Any) -> dict[str, Any]:
    """
    One dispatcher's summary frame, for a partition of `dispatched` inputs.

    Outputs and records are not here: they ride on the `Completed`
    frames, and a summary frame repeating them would be a second copy of
    the partition.
    """
    return {
        "req_id": "dispatcher-0",
        "dispatched": dispatched,
        "retried": 0,
        "retry_wait_ms": 0.0,
        "cold": False,
        "connections": 1,
        "elapsed_ms": 1.0,
        **overrides,
    }


class RecordingInvoker(_native.Stub):
    """
    A Lambda client that records the requests it was sent and replays
    canned responses. Use it to assert **what went on the wire**, and to
    put a dispatcher into a state a real one is hard to provoke, like a
    short partition or a failed invoke.

    Each canned response is either a summary dict (`dispatcher_summary`),
    streamed as the frames accounting for its work and then the summary,
    or a list of frames to stream verbatim - which is how a test sends a
    stream that does *not* add up. For a body written as the test goes,
    take one from `respond()` instead.
    """

    def __init__(self, responses: Sequence[Any] = ()) -> None:
        for canned in responses:
            if isinstance(canned, list):
                frames = canned
            else:
                frames = [
                    completed(succeeded=canned["dispatched"]),
                    {"Summary": canned},
                ]
            body = self.respond()
            body.write(dispatcher_body(*frames))
            body.close()

    @property
    def payloads(self) -> list[dict]:
        """One request per dispatcher invocation, i.e. per partition."""
        return [json.loads(request) for request in self.requests]

    @property
    def invoked(self) -> list[tuple[str, dict]]:
        """One `(function_name, payload)` per `Session.invoke_detached`."""
        return [(function_name, json.loads(payload)) for function_name, payload in self.invocations]
