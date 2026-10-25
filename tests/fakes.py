"""
A Lambda client that answers however a test says, standing in for AWS
behind the real client: `RecordingClient`, and the frames it replays.
"""

import json
from collections.abc import Sequence
from typing import Any

from map_lambda import _native

# The metadata prelude a deployed coordinator's stream opens with, which
# the client skips.
COORDINATOR_PRELUDE = b'{"statusCode":200,"headers":{}}' + b"\x00" * 8


def lines(*frames: Any) -> bytes:
    """Frames as a coordinator writes them, one JSON line each."""
    return b"".join(json.dumps(f).encode() + b"\n" for f in frames)


def coordinator_body(*frames: Any) -> bytes:
    """A whole response body: the prelude, then the frames."""
    return COORDINATOR_PRELUDE + lines(*frames)


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
    One coordinator's result frame.

    `succeeded` sets `dispatched` too, because the ordinary case is a
    partition that did all of its work; `**overrides` is how a test says
    otherwise, e.g. `transport_errors=1` for a failed one.

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


class RecordingClient(_native.Stub):
    """
    A Lambda client that records the requests it was sent and replays
    canned responses. Use it to assert **what went on the wire**, and to
    put a coordinator into a state a real one is hard to provoke, like a
    short partition or a transport error.

    Each canned response is either a result dict (`coordinator_result`),
    streamed as the frames accounting for its work and then the result,
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
                    completed(done=canned["succeeded"], first_error=canned.get("first_error")),
                    {"Result": canned},
                ]
            body = self.respond()
            body.write(coordinator_body(*frames))
            body.close()

    @property
    def payloads(self) -> list[dict]:
        """One request per coordinator invocation, i.e. per partition."""
        return [json.loads(request) for request in self.requests]

    @property
    def invoked(self) -> list[tuple[str, dict]]:
        """One `(function, payload)` per `Lambdas.invoke`."""
        return [(function, json.loads(payload)) for function, payload in self.invocations]
