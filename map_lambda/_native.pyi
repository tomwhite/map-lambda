from collections.abc import AsyncIterator
from typing import Any, final

DEFAULT_CONCURRENCY: int
DEFAULT_DISPATCHER_FUNCTION: str
LOCAL_ENDPOINT_URL: str
MAX_STREAMS_PER_CONNECTION: int

@final
class Client: ...

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
    done: int
    answers: list[tuple[int, Any]]
    invocation_stats: list[dict[str, Any]]
    dispatcher_stats: list[dict[str, Any]]

@final
class Run(AsyncIterator[Batch]):
    def __aiter__(self) -> Run: ...
    async def __anext__(self) -> Batch: ...
    def close(self) -> None: ...

class Session:
    def __init__(self, client: Client | Stub, endpoint_url: str | None = None) -> None: ...
    def dispatch(
        self,
        function: str,
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
    ) -> Run: ...
    async def invoke(self, function: str, payload: str) -> None: ...

async def connect(endpoint_url: str | None = None) -> Client: ...
