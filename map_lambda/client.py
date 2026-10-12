from typing import Any

import aioboto3
from botocore.config import Config

# A local `cargo lambda watch` server, started from rust/.
LOCAL_ENDPOINT_URL = "http://localhost:9000"

# Only local runs and tests take this; `Lambdas` passes
# `COORDINATOR_READ_TIMEOUT_SECONDS`. It needn't cover queueing: aiohttp's
# `sock_read` timer starts once the request body has been written.
DEFAULT_READ_TIMEOUT_SECONDS = 60

# Worker invocations in flight per run, split across its coordinators.
DEFAULT_CONCURRENCY = 100

# Real Lambda serves `InvokeWithResponseStream` under its own API version;
# `cargo lambda watch` serves it beside the buffered invoke, so locally the
# documented path is a 404.
STREAMING_PATH = "/2021-11-15/functions/"
LOCAL_STREAMING_PATH = "/2015-03-31/functions/"


def _use_local_streaming_path(request: Any, **kwargs: Any) -> None:
    """
    Point a streaming invoke at the path the emulator serves.

    Registered only on a client with an `endpoint_url`, so it never
    rewrites a request to real Lambda, which serves only `STREAMING_PATH`.
    """
    request.url = request.url.replace(STREAMING_PATH, LOCAL_STREAMING_PATH, 1)


def lambda_client(
    endpoint_url: str | None = None,
    session: aioboto3.Session | None = None,
    max_pool_connections: int = DEFAULT_CONCURRENCY,
    read_timeout: int = DEFAULT_READ_TIMEOUT_SECONDS,
) -> Any:
    """
    An aioboto3 Lambda client (an async context manager) for either a
    real deployment or a local `cargo lambda watch` server.

    With `endpoint_url` set, credentials and region are stubbed out (the
    emulator doesn't check them) and the connect timeout is short, so a
    connectivity check fails fast rather than hanging.

    `max_pool_connections` is a hard ceiling: it becomes the
    `aiohttp.TCPConnector` `limit`, and anything beyond it queues.
    botocore's own default of 10 would silently cap a run at ten
    coordinators.

    botocore's retries are disabled: retries are the coordinator's,
    where they are counted, and one underneath would be an invocation
    nothing counted.
    """
    session = session or aioboto3.Session()

    if endpoint_url is None:
        return session.client(
            "lambda",
            config=Config(
                read_timeout=read_timeout,
                retries={"max_attempts": 1},
                max_pool_connections=max_pool_connections,
            ),
        )

    # On the session because `session.client` returns a context manager,
    # not a client.
    register = getattr(session, "_session", session).register
    register("before-send.lambda.InvokeWithResponseStream", _use_local_streaming_path)

    return session.client(
        "lambda",
        endpoint_url=endpoint_url,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(
            connect_timeout=2,
            read_timeout=read_timeout,
            retries={"max_attempts": 1},
            max_pool_connections=max_pool_connections,
        ),
    )
