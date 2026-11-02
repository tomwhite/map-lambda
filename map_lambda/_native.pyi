from collections.abc import AsyncIterator
from typing import Any, final

DEFAULT_CONCURRENCY: int
DEFAULT_DISPATCHER_FUNCTION: str
LOCAL_ENDPOINT_URL: str
MAX_STREAMS_PER_CONNECTION: int

@final
class Invoker: ...

class Stub:
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def respond(self) -> Body: ...
    @property
    def requests(self) -> list[str]: ...
    @property
    def invocations(self) -> list[tuple[str, str]]: ...

@final
class Body:
    def write(self, chunk: bytes) -> None: ...
    def close(self) -> None: ...
    @property
    def closed(self) -> bool: ...

@final
class Batch:
    succeeded: int
    outputs: list[tuple[int, Any]]
    invocation_stats: list[dict[str, Any]]
    dispatcher_stats: list[dict[str, Any]]

@final
class Batches(AsyncIterator[Batch]):
    def __aiter__(self) -> Batches: ...
    async def __anext__(self) -> Batch: ...
    def close(self) -> None: ...

class Session:
    def __init__(self, invoker: Invoker | Stub, endpoint_url: str | None = None) -> None: ...
    def start(
        self,
        worker_function: str,
        inputs: list[Any],
        *,
        input_key: str,
        constant_args: dict[str, Any],
        dispatcher_function: str,
        dispatchers: int | None,
        concurrency: int,
        retries: int,
        return_outputs: bool,
        return_stats: bool,
    ) -> Batches: ...
    async def invoke_detached(self, function_name: str, payload: str) -> None: ...

async def connect(endpoint_url: str | None = None) -> Invoker: ...
