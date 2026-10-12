"""
Fakes of what a client cannot reach from a test: a coordinator that runs
its workers in process (`InProcessCoordinator`), and a client that
answers however the test says (`RecordingClient`).

`session` hands back a `Lambdas` to pass in; `dispatching_to` patches
client construction, for code that calls `map_lambda.map_unordered` and
so never takes a session to swap.

Shipped rather than kept in tests/ so that clients share one fake of the
protocol instead of each writing their own, and drifting.

Timing is not reproduced: the real coordinator reports on a timer
and finishes invocations out of order, while this runs them in order and
sends one frame per invocation. A test that pinned either would be
pinning the fake.
"""

import contextlib
import json
import time
from collections.abc import Iterator, Sequence
from typing import Any

from . import api, coordinator

# What `coordinator.PRELUDE_SEPARATOR` exists to skip, with a plausible
# set of metadata in front of it.
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
    One coordinator's result frame, with every field `check` reads.

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

    def __init__(self, responses: Sequence[Any]) -> None:
        self.responses = list(responses)
        # One per coordinator invocation, i.e. per partition.
        self.payloads: list[dict] = []

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


class InProcessCoordinator:
    """
    The coordinator lambda, in this process. Pass it where a Lambda
    client would go.

    `worker` is handed the assembled payload - the template with the
    input added **verbatim** under `input_key` - exactly the JSON the
    real worker deserialises. It raises to fail, and whatever it returns
    is the response body.

    A raising worker is a function error, which is never retried, so
    `retries` changes nothing here: it covers transport failures, which an
    in-process worker cannot have. The failure is counted rather than
    raised, so it is `check` that raises, as with a real coordinator.
    """

    def __init__(
        self, worker: Any, req_id: str = "coord-local", invoke_error: Exception | None = None
    ) -> None:
        self.worker = worker
        self.req_id = req_id
        # One per coordinator invocation, i.e. per partition per call.
        self.requests: list[dict] = []
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
        request = json.loads(Payload)
        self.requests.append(request)

        started = time.time()
        succeeded = function_errors = 0
        first_error = None
        # As the real coordinator does, before any work.
        frames: list[dict] = [completed()]

        for index, input_value in enumerate(request["inputs"]):
            payload = {**request["template"], request["input_key"]: input_value}
            returned = None
            invoke_start = time.time()
            try:
                returned = self.worker(payload)
                error: Exception | None = None
            # Any exception, as the Lambda error channel makes no
            # distinction. Never retried: a raising worker is a function
            # error, and `retries` covers transport failures, which a fake
            # running the worker in-process cannot have.
            except Exception as e:
                error = e
            invoke_end = time.time()
            stats: list[dict] = []
            outputs: list[dict] = []
            if error is None:
                succeeded += 1
                # The real one comes off the invoke response, not the
                # body, so it exists whatever the worker returned.
                req_id = f"{self.req_id}-w{succeeded}"
                if request["return_stats"]:
                    stats.append(
                        {
                            "index": index,
                            "req_id": req_id,
                            "invoke_start": invoke_start,
                            "invoke_end": invoke_end,
                        }
                    )
                # `Tally::observe` sends no entry for a null answer, and
                # none at all when the request asked for no outputs.
                if request["return_outputs"] and returned is not None:
                    outputs.append({"index": index, "response": returned})
            else:
                function_errors += 1
                if first_error is None:
                    first_error = str(error)
            frames.append(
                completed(
                    done=1 if error is None else 0,
                    outputs=outputs,
                    stats=stats,
                    first_error=None if error is None else str(error),
                )
            )

        frames.append(
            {
                "Result": {
                    "req_id": self.req_id,
                    "dispatched": len(request["inputs"]),
                    "succeeded": succeeded,
                    "function_errors": function_errors,
                    "transport_errors": 0,
                    # Nothing here retries: only a transport failure is,
                    # and an in-process worker has none.
                    "retried": 0,
                    "retry_wait_ms": 0.0,
                    "first_error": first_error,
                    # As a local coordinator reports it: nothing to say.
                    "cold": None,
                    "connections": coordinator.connections_for(request["concurrency"]),
                    "elapsed_ms": (time.time() - started) * 1000.0,
                }
            }
        )
        return {"EventStream": coordinator_stream([coordinator_body(*frames)])}


def session(
    worker: Any, *, invoke_error: Exception | None = None, **settings: Any
) -> "api.Lambdas":
    """
    A `Lambdas` that dispatches through `InProcessCoordinator(worker)`
    instead of a deployed coordinator.

    The seam is the *Lambda client*, not `coordinator.stream`, so the
    whole Python half of the wire runs for real.

    `coordinators` and `concurrency` default to 2 and 4, small enough to
    reason about which partition got what. `invoke_error` makes
    `Lambdas.invoke` raise rather than record.
    """
    lambdas = api.Lambdas(
        **{"coordinators": 2, "concurrency": 4, **settings},
    )
    # Not via `__aenter__`, which would build a real client.
    lambdas._client = InProcessCoordinator(worker, invoke_error=invoke_error)
    return lambdas


class _ReadyClient:
    """
    An already-built client, wearing the async context manager
    `Lambdas.__aenter__` expects from `lambda_client`.
    """

    def __init__(self, client: Any) -> None:
        self.client = client

    async def __aenter__(self) -> Any:
        return self.client

    async def __aexit__(self, *exc_info: Any) -> None:
        return None


@contextlib.contextmanager
def dispatching_to(
    worker: Any, *, req_id: str = "coord-local", invoke_error: Exception | None = None
) -> Iterator[InProcessCoordinator]:
    """
    Run everything inside this block through `InProcessCoordinator`,
    and yield it so a test can assert on what went out.

        with dispatching_to(lambda payload: {"doubled": payload["i"] * 2}) as coordinator:
            answers = await a_clients_own_function([1, 2, 3])

        assert coordinator.requests[0]["input_key"] == "i"

    **For code that calls `map_lambda.map_unordered`**, which builds its own
    session. Where a function takes a `Lambdas`, prefer `session`:
    nothing is patched.

    It rebinds `api.lambda_client` for the duration - the same seam as
    `session`, so it fakes no more. Being a module-level rebind:

    * It catches **every** session built in the block, not only the ones
      `map_lambda.map_unordered` makes for itself.
    * It is not safe with other tests running concurrently *in the same
      process*. Separate processes - which is what pytest-xdist gives
      you - are fine.
    * `endpoint_url` stops meaning anything, because nothing reaches
      the code that would have read it.
    """
    coordinator = InProcessCoordinator(worker, req_id=req_id, invoke_error=invoke_error)
    original = api.lambda_client
    api.lambda_client = lambda *args, **kwargs: _ReadyClient(coordinator)
    try:
        yield coordinator
    finally:
        api.lambda_client = original
