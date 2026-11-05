import json
from collections.abc import AsyncGenerator, Callable
from dataclasses import dataclass, field
from typing import Any, Literal, Self

from . import _native

# Worker invocations in flight per call, split across its dispatchers.
DEFAULT_CONCURRENCY: int = _native.DEFAULT_CONCURRENCY

# A local `cargo lambda watch` server, started from rust/.
LOCAL_ENDPOINT_URL = _native.LOCAL_ENDPOINT_URL

# The dispatcher's name, before a project prefixes it.
DISPATCHER: str = _native.DISPATCHER

__docformat__ = "google"

__all__ = ["Session", "for_each", "map_unordered"]


@dataclass
class Session:
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

    project: str | None = None
    """
    The project the lambdas were deployed as, which prefixes every name on AWS:
    the dispatcher is `<project>-dispatcher`, and a worker `"copy"` is
    `<project>-copy`. `None` uses the names as given. Against a local cargo lambda
    watch, names are never prefixed, as it serves each function under its binary's
    name.
    """

    dispatchers: int | Literal["auto"] = "auto"
    """
    The number of dispatchers the inputs are partitioned across.

    `"auto"` uses the fewest number of dispatchers that respects AWS Lambda
    limits.
    """

    concurrency: int = DEFAULT_CONCURRENCY
    """
    The total number of worker lambdas in flight for the whole call, split
    across dispatchers.
    """

    retries: int = 2
    """
    How many times to retry a failed request to Lambda (per invocation). The
    default is 2, which is 3 attempts in total. A worker that ran and raised
    is never retried: the same payload gets the same output.
    """

    on_progress: Callable[[int], None] | None = None
    """
    Called as invocations complete, with how many more have since the
    last call.
    """

    dispatcher_stats: list[dict[str, Any]] | None = None
    """
    Pass a list to collect stats for the dispatchers - one record per
    dispatcher invocation. A call stops at its first failed invocation
    without waiting for these, so it collects none. Each record holds:

    * `req_id` - AWS's id for the dispatcher's own invocation, for finding
      its run in CloudWatch
    * `dispatched` - invocations it sent, which is its partition's size
    * `retried` - attempts beyond the first, across the partition
    * `retry_wait_ms` - summed wait before those attempts
    * `cold` - whether the execution environment was cold, or `None` for a
      local run, which has no shared client set to build
    * `connections` - HTTP/2 connections the invocations were spread over
    * `elapsed_ms` - how long the partition took
    * `call` - which [`map_unordered`][map_lambda.Session.map_unordered] or
      [`for_each`][map_lambda.Session.for_each] of the session this was
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
    * `call`, `call_start` - as in [`dispatcher_stats`][map_lambda.Session.dispatcher_stats]
    """

    _invoker: Any = field(default=None, init=False, repr=False)
    _native_session: Any = field(default=None, init=False, repr=False)

    async def __aenter__(self) -> Self:
        self._invoker = await _native.connect(self.endpoint_url)
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        self._invoker = None
        self._native_session = None

    def _require_native_session(self) -> Any:
        if self._invoker is None:
            raise RuntimeError("Session is not open: use `async with Session(...) as session:`")
        if self._native_session is None:
            self._native_session = _native.Session(self._invoker, self.endpoint_url)
        return self._native_session

    def function_name(self, name: str, /, *, memory: int | None = None) -> str:
        """
        The name a call to `name` invokes, such as `<project>-copy-3538` on AWS, for
        finding the function's logs or metrics. Works whether or not the session is open.
        """
        return _native.function_name(
            name, memory=memory, project=self.project, local=self.endpoint_url is not None
        )

    def map_unordered(
        self,
        function_name: str,
        inputs: list[Any],
        /,
        *,
        input_key: str,
        constant_args: dict[str, Any] | None = None,
        memory: int | None = None,
    ) -> AsyncGenerator[tuple[int, Any], None]:
        """
        Invoke `function_name` once per value of `inputs`, yielding
        `(index, output)` as each output comes back. If outputs
        are not needed, use [`for_each`][map_lambda.Session.for_each].

        Each worker's payload is a dict with the input value
        under `input_key`, plus a copy of `constant_args` (if supplied).

        Args:
            function_name: The worker's name in its project, such as `"copy"`.
            inputs: The values to pass to the function, one entry per invocation.
            input_key: The name of the key for the input in each worker's
                payload. Must match the field the worker deserialises it into:
                for a Rust worker, a field of its `Request` struct.
            constant_args: Optional extra arguments to pass to each function invocation.
            memory: Which of the worker's deployed memory sizes to use, in MB, or
                `None` for its default. On AWS this calls `<name>-<memory>`.

        Yields:
            `(index, output)`, in the order the invocations finished.
        """
        return self._stream(
            self._require_native_session(),
            function_name,
            inputs,
            input_key=input_key,
            constant_args=constant_args,
            memory=memory,
            return_outputs=True,
        )

    async def _stream(
        self,
        native_session: Any,
        function_name: str,
        inputs: list[Any],
        /,
        *,
        input_key: str,
        constant_args: dict[str, Any] | None,
        memory: int | None,
        return_outputs: bool,
    ) -> AsyncGenerator[tuple[int, Any], None]:
        """
        Labelled outputs as they arrive, or none at all when
        `return_outputs` is false.
        """
        batches = native_session.start(
            function_name,
            inputs,
            memory=memory,
            input_key=input_key,
            constant_args={} if constant_args is None else constant_args,
            project=self.project,
            dispatchers=None if self.dispatchers == "auto" else self.dispatchers,
            concurrency=self.concurrency,
            retries=self.retries,
            return_outputs=return_outputs,
            return_stats=self.invocation_stats is not None,
        )
        # Closed explicitly so that stopping early closes the dispatchers'
        # streams now, rather than whenever `batches` is collected.
        try:
            async for batch in batches:
                if self.invocation_stats is not None:
                    self.invocation_stats.extend(batch.invocation_stats)
                if self.dispatcher_stats is not None:
                    self.dispatcher_stats.extend(batch.dispatcher_stats)
                if self.on_progress is not None and batch.succeeded:
                    self.on_progress(batch.succeeded)
                for output in batch.outputs:
                    yield output
        finally:
            batches.close()

    async def for_each(
        self,
        function_name: str,
        inputs: list[Any],
        /,
        *,
        input_key: str,
        constant_args: dict[str, Any] | None = None,
        memory: int | None = None,
    ) -> None:
        """
        Invoke `function_name` once for each value of `inputs`. This is
        the same as [`map_unordered`][map_lambda.Session.map_unordered] except nothing is
        returned, and it
        waits for the whole call.

        Refer to [`map_unordered`][map_lambda.Session.map_unordered] for argument definitions.
        """
        stream = self._stream(
            self._require_native_session(),
            function_name,
            inputs,
            input_key=input_key,
            constant_args=constant_args,
            memory=memory,
            return_outputs=False,
        )
        # Nothing to iterate - the outputs were never asked for - but
        # draining it is what settles the partitions and raises.
        async for _ in stream:
            pass

    async def invoke_detached(self, function_name: str, /, **payload: Any) -> None:
        """
        Invoke `function_name` once with the given payload and do not wait for it.

        Useful for "fire and forget" semantics, where failing after submission
        and not learning about the failure is acceptable - failing to submit
        raises. For a synchronous one-off, use
        [`for_each`][map_lambda.Session.for_each] with a single input.
        """
        await self._require_native_session().invoke_detached(
            function_name, json.dumps(payload), project=self.project
        )


async def map_unordered(
    function_name: str,
    inputs: list[Any],
    /,
    *,
    input_key: str,
    constant_args: dict[str, Any] | None = None,
    memory: int | None = None,
    endpoint_url: str | None = None,
    project: str | None = None,
    dispatchers: int | Literal["auto"] = "auto",
    concurrency: int = DEFAULT_CONCURRENCY,
    retries: int = 2,
    on_progress: Callable[[int], None] | None = None,
    dispatcher_stats: list[dict[str, Any]] | None = None,
    invocation_stats: list[dict[str, Any]] | None = None,
) -> AsyncGenerator[tuple[int, Any], None]:
    """
    Invoke `function_name` once per value of `inputs`, yielding
    `(index, output)` as each output comes back. If outputs
    are not needed, use [`for_each`][map_lambda.for_each].

    Refer to [`Session.map_unordered`][map_lambda.Session.map_unordered] for argument
    definitions.

    `endpoint_url` onwards are the dispatch settings documented on
    [`Session`][map_lambda.Session], applied to a session opened for this call alone - which
    closes when the iterator does, so stopping early still shuts the
    client down.

    Yields:
        `(index, output)`, in the order the invocations finished.
    """
    async with Session(
        endpoint_url=endpoint_url,
        project=project,
        dispatchers=dispatchers,
        concurrency=concurrency,
        retries=retries,
        on_progress=on_progress,
        dispatcher_stats=dispatcher_stats,
        invocation_stats=invocation_stats,
    ) as session:
        async for output in session.map_unordered(
            function_name,
            inputs,
            input_key=input_key,
            constant_args=constant_args,
            memory=memory,
        ):
            yield output


async def for_each(
    function_name: str,
    inputs: list[Any],
    /,
    *,
    input_key: str,
    constant_args: dict[str, Any] | None = None,
    memory: int | None = None,
    endpoint_url: str | None = None,
    project: str | None = None,
    dispatchers: int | Literal["auto"] = "auto",
    concurrency: int = DEFAULT_CONCURRENCY,
    retries: int = 2,
    on_progress: Callable[[int], None] | None = None,
    dispatcher_stats: list[dict[str, Any]] | None = None,
    invocation_stats: list[dict[str, Any]] | None = None,
) -> None:
    """
    Invoke `function_name` once for each value of `inputs`. This is
    the same as [`map_unordered`][map_lambda.map_unordered] except nothing is returned.

    Refer to [`Session.map_unordered`][map_lambda.Session.map_unordered] for argument
    definitions.
    """
    async with Session(
        endpoint_url=endpoint_url,
        project=project,
        dispatchers=dispatchers,
        concurrency=concurrency,
        retries=retries,
        on_progress=on_progress,
        dispatcher_stats=dispatcher_stats,
        invocation_stats=invocation_stats,
    ) as session:
        await session.for_each(
            function_name,
            inputs,
            input_key=input_key,
            constant_args=constant_args,
            memory=memory,
        )
