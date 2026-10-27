from collections.abc import AsyncIterator
from typing import Any, final

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

async def connect(endpoint_url: str | None = None) -> Client: ...
async def invoke(client: Client | Stub, function: str, payload: str) -> None: ...
def dispatch(
    client: Client | Stub,
    inputs: list[Any],
    *,
    constant_args: dict[str, Any],
    dispatcher_function: str,
    target_function: str,
    input_key: str,
    dispatchers: int | None,
    concurrency: int,
    retries: int,
    endpoint_url: str | None,
    return_outputs: bool,
    return_stats: bool,
    call: int,
) -> Run: ...
