import contextlib
import json
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass, field
from typing import Any, Literal, Self

from . import coordinator
from .client import DEFAULT_CONCURRENCY, lambda_client

__docformat__ = "google"

__all__ = ["Lambdas", "for_each", "map_unordered"]


@dataclass
class Lambdas:
    """
    The deployed lambdas, holding settings and stats for a session.
    Use as `async with`.
    """

    endpoint_url: str | None = None
    """
    `None` uses AWS with credentials and region picked up from the local
    environment; [`LOCAL_ENDPOINT_URL`][map_lambda.LOCAL_ENDPOINT_URL] uses a local cargo
    lambda watch instead.
    """

    coordinators: int | Literal["auto"] = "auto"
    """
    The number of coordinators the inputs are partitioned across.

    `"auto"` uses the fewest number of coordinators that respects AWS Lambda
    limits.
    """

    concurrency: int = DEFAULT_CONCURRENCY
    """
    The total number of worker lambdas in flight for the whole run, split
    across coordinators.
    """

    retries: int = 2
    """
    How many times to retry on a transport failure (per invocation). The
    default is 2, which is 3 attempts in total. A worker that ran and raised
    is never retried: the same payload gets the same answer.
    """

    on_progress: Callable[[int], None] | None = None
    """
    Called as invocations complete, with how many more have since the
    last call.
    """

    coordinator_stats: list[dict[str, Any]] | None = None
    """
    Pass a list to collect stats for the coordinators - one record per
    coordinator invocation, holding:

    * `req_id` - AWS's id for the coordinator's own invocation, for finding
      its run in CloudWatch
    * `dispatched` - invocations it sent, which is its partition's size
    * `succeeded` - invocations that answered
    * `function_errors` - the worker ran and failed
    * `transport_errors` - the request never got a usable response
    * `retried` - attempts beyond the first, across the partition
    * `retry_wait_ms` - summed wait before those attempts
    * `first_error` - the first failed invocation's message, or `None`
    * `cold` - whether the execution environment was cold, or `None` for a
      local run, which has no shared client set to build
    * `connections` - HTTP/2 connections the invocations were spread over
    * `elapsed_ms` - how long the partition took
    * `call` - which [`map_unordered`][map_lambda.Lambdas.map_unordered] or
      [`for_each`][map_lambda.Lambdas.for_each] of the session this was
    * `call_start` - when that call began, as epoch seconds
    """

    invocation_stats: list[dict[str, Any]] | None = None
    """
    Pass a list to collect stats for the function invocations - one record per
    worker function invocation, holding:

    * `index` - the position in `inputs` of the input it ran
    * `req_id` - AWS's id for the worker invocation, for joining to CloudWatch.
      Empty against `cargo lambda watch`, which assigns none
    * `invoke_start`, `invoke_end` - epoch seconds, spanning every attempt
    * `call`, `call_start` - as in [`coordinator_stats`][map_lambda.Lambdas.coordinator_stats]
    """

    _calls: int = field(default=0, init=False, repr=False)
    _client: Any = field(default=None, init=False, repr=False)
    _cm: Any = field(default=None, init=False, repr=False)

    def _partitions(self) -> int:
        """How many coordinators this session dispatches through."""
        if self.coordinators == "auto":
            return coordinator.coordinators_for(
                self.concurrency, local=self.endpoint_url is not None
            )
        return self.coordinators

    async def __aenter__(self) -> Self:
        # One connection per coordinator: the fan-out happens inside AWS.
        self._cm = lambda_client(
            self.endpoint_url,
            max_pool_connections=self._partitions(),
            read_timeout=coordinator.COORDINATOR_READ_TIMEOUT_SECONDS,
        )
        self._client = await self._cm.__aenter__()
        return self

    async def __aexit__(self, *exc_info: Any) -> Any:
        cm, self._cm, self._client = self._cm, None, None
        return await cm.__aexit__(*exc_info)

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError("Lambdas has no client: use `async with Lambdas(...) as lambdas:`")
        return self._client

    def map_unordered(
        self,
        function_name: str,
        inputs: list[Any],
        /,
        *,
        input_key: str,
        constant_args: dict[str, Any] | None = None,
    ) -> AsyncGenerator[tuple[int, Any], None]:
        """
        Invoke `function_name` once per value of `inputs`, yielding
        `(index, return value)` as each answer comes back. If return
        values are not needed, use [`for_each`][map_lambda.Lambdas.for_each].

        Each worker's payload is a dict with the input value
        under `input_key`, plus a copy of `constant_args` (if supplied).

        Args:
            function_name: The name of the deployed lambda function.
            inputs: The values to pass to the function, one entry per invocation.
            input_key: The name of the key for the input in each worker's
                payload. Must match the field the worker deserialises it into:
                for a Rust worker, a field of its `Request` struct.
            constant_args: Optional extra arguments to pass to each function invocation.

        Yields:
            `(index, return value)`, in the order the invocations finished.
        """
        return self._stream(
            self._require_client(),
            function_name,
            inputs,
            input_key=input_key,
            constant_args=constant_args,
            return_outputs=True,
        )

    async def _stream(
        self,
        client: Any,
        function_name: str,
        inputs: list[Any],
        /,
        *,
        input_key: str,
        constant_args: dict[str, Any] | None,
        return_outputs: bool,
    ) -> AsyncGenerator[tuple[int, Any], None]:
        """
        Labelled answers as they arrive, or none at all when
        `return_outputs` is false.
        """
        call, self._calls = self._calls, self._calls + 1
        batches = coordinator.stream(
            client,
            inputs,
            constant_args={} if constant_args is None else constant_args,
            coordinator_function=coordinator.DEFAULT_COORDINATOR_FUNCTION,
            target_function=function_name,
            input_key=input_key,
            coordinators=self._partitions(),
            concurrency=self.concurrency,
            retries=self.retries,
            endpoint_url=self.endpoint_url,
            on_done=self.on_progress,
            coordinator_stats=self.coordinator_stats,
            invocation_stats=self.invocation_stats,
            return_outputs=return_outputs,
            call=call,
        )
        # Closed explicitly: an async generator has no `yield from`, so a
        # plain `async for` would leave the cancellation to the interpreter.
        async with contextlib.aclosing(batches):
            async for batch in batches:
                for answer in batch:
                    yield answer["index"], answer["response"]

    async def for_each(
        self,
        function_name: str,
        inputs: list[Any],
        /,
        *,
        input_key: str,
        constant_args: dict[str, Any] | None = None,
    ) -> None:
        """
        Invoke `function_name` once for each value of `inputs`. This is
        the same as [`map_unordered`][map_lambda.Lambdas.map_unordered] except nothing is
        returned, and it
        waits for the whole run.

        Refer to [`map_unordered`][map_lambda.Lambdas.map_unordered] for argument definitions.
        """
        stream = self._stream(
            self._require_client(),
            function_name,
            inputs,
            input_key=input_key,
            constant_args=constant_args,
            return_outputs=False,
        )
        # Nothing to iterate - the answers were never asked for - but
        # draining it is what settles the partitions and raises.
        async for _ in stream:
            pass

    async def invoke(self, function_name: str, /, **payload: Any) -> None:
        """
        Invoke `function_name` once with the given payload and do not wait for it.

        Useful for "fire and forget" semantics, where failing after submission
        and not learning about the failure is acceptable - failing to submit
        raises. For a synchronous one-off, use
        [`for_each`][map_lambda.Lambdas.for_each] with a single input.
        """
        await self._require_client().invoke(
            FunctionName=function_name,
            InvocationType="Event",
            Payload=json.dumps(payload),
        )


async def map_unordered(
    function_name: str,
    inputs: list[Any],
    /,
    *,
    input_key: str,
    constant_args: dict[str, Any] | None = None,
    endpoint_url: str | None = None,
    coordinators: int | Literal["auto"] = "auto",
    concurrency: int = DEFAULT_CONCURRENCY,
    retries: int = 2,
    on_progress: Callable[[int], None] | None = None,
    coordinator_stats: list[dict[str, Any]] | None = None,
    invocation_stats: list[dict[str, Any]] | None = None,
) -> AsyncGenerator[tuple[int, Any], None]:
    """
    Invoke `function_name` once per value of `inputs`, yielding
    `(index, return value)` as each answer comes back. If return values
    are not needed, use [`for_each`][map_lambda.for_each].

    Refer to [`Lambdas.map_unordered`][map_lambda.Lambdas.map_unordered] for argument
    definitions.

    `endpoint_url` onwards are the dispatch settings documented on
    [`Lambdas`][map_lambda.Lambdas], applied to a session opened for this call alone - which
    closes when the iterator does, so stopping early still shuts the
    client down.

    Yields:
        `(index, return value)`, in the order the invocations finished.
    """
    async with Lambdas(
        endpoint_url=endpoint_url,
        coordinators=coordinators,
        concurrency=concurrency,
        retries=retries,
        on_progress=on_progress,
        coordinator_stats=coordinator_stats,
        invocation_stats=invocation_stats,
    ) as lambdas:
        async for answer in lambdas.map_unordered(
            function_name,
            inputs,
            input_key=input_key,
            constant_args=constant_args,
        ):
            yield answer


async def for_each(
    function_name: str,
    inputs: list[Any],
    /,
    *,
    input_key: str,
    constant_args: dict[str, Any] | None = None,
    endpoint_url: str | None = None,
    coordinators: int | Literal["auto"] = "auto",
    concurrency: int = DEFAULT_CONCURRENCY,
    retries: int = 2,
    on_progress: Callable[[int], None] | None = None,
    coordinator_stats: list[dict[str, Any]] | None = None,
    invocation_stats: list[dict[str, Any]] | None = None,
) -> None:
    """
    Invoke `function_name` once for each value of `inputs`. This is
    the same as [`map_unordered`][map_lambda.map_unordered] except nothing is returned.

    Refer to [`Lambdas.map_unordered`][map_lambda.Lambdas.map_unordered] for argument
    definitions.
    """
    async with Lambdas(
        endpoint_url=endpoint_url,
        coordinators=coordinators,
        concurrency=concurrency,
        retries=retries,
        on_progress=on_progress,
        coordinator_stats=coordinator_stats,
        invocation_stats=invocation_stats,
    ) as lambdas:
        await lambdas.for_each(
            function_name,
            inputs,
            input_key=input_key,
            constant_args=constant_args,
        )
