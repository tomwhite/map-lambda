"""
What the Lambda client is configured with: the read timeout and the
pool size are both settings a run has been lost to.
"""

import asyncio
import json
import os
from typing import Any

import pytest
from botocore.exceptions import NoCredentialsError, NoRegionError

from map_lambda.client import (
    DEFAULT_CONCURRENCY,
    DEFAULT_READ_TIMEOUT_SECONDS,
    LOCAL_ENDPOINT_URL,
    lambda_client,
)

# The payload below is echo's; an override needs its own function's
# payload too.
REAL_LAMBDA_FUNCTION_NAME = os.environ.get("MAP_LAMBDA_REAL_LAMBDA_FUNCTION_NAME", "echo")


class RecordingSession:
    """A stand-in for aioboto3.Session that captures the client kwargs."""

    def __init__(self) -> None:
        self.kwargs: dict[str, Any] = {}
        self.handlers: list[str] = []

    def client(self, service_name: str, **kwargs: Any) -> Any:
        self.kwargs = {"service_name": service_name, **kwargs}
        return object()

    def register(self, event_name: str, handler: Any) -> None:
        self.handlers.append(event_name)


def session_for(endpoint_url: str | None, **kwargs: Any) -> RecordingSession:
    session = RecordingSession()
    lambda_client(endpoint_url, session=session, **kwargs)
    return session


def config_for(endpoint_url: str | None, **kwargs: Any):
    return session_for(endpoint_url, **kwargs).kwargs["config"]


@pytest.mark.parametrize("endpoint_url", [None, LOCAL_ENDPOINT_URL])
def test_the_default_read_timeout_suits_a_single_worker_invocation(endpoint_url):
    """
    Sized to one worker invocation (~5x the longest recorded), not to
    Lambda's 900-second maximum. A run overrides it.
    """
    config = config_for(endpoint_url)

    assert config.read_timeout == DEFAULT_READ_TIMEOUT_SECONDS
    assert config.read_timeout < 900


@pytest.mark.parametrize("endpoint_url", [None, LOCAL_ENDPOINT_URL])
def test_the_read_timeout_can_be_overridden(endpoint_url):
    assert config_for(endpoint_url, read_timeout=7).read_timeout == 7


@pytest.mark.parametrize("endpoint_url", [None, LOCAL_ENDPOINT_URL])
def test_botocores_own_retries_stay_off(endpoint_url):
    """Retries are the coordinator's job, where they are counted."""
    assert config_for(endpoint_url).retries["max_attempts"] == 1


@pytest.mark.parametrize("endpoint_url", [None, LOCAL_ENDPOINT_URL])
def test_the_connection_pool_can_be_sized_by_the_caller(endpoint_url):
    config = config_for(endpoint_url)

    assert config.max_pool_connections == DEFAULT_CONCURRENCY
    assert config_for(endpoint_url, max_pool_connections=250).max_pool_connections == 250


def test_only_a_local_client_rewrites_the_streaming_path():
    """
    `cargo lambda watch` serves a streaming invoke on an older API path;
    real Lambda serves only the documented one.
    """
    assert session_for(LOCAL_ENDPOINT_URL).handlers == [
        "before-send.lambda.InvokeWithResponseStream"
    ]
    assert session_for(None).handlers == []


@pytest.mark.real_lambda
def test_the_responses_request_id_is_the_workers_own():
    """
    The claim the coordinator's request ids rest on: the invoke
    response's `x-amzn-requestid`, documented as the API request id, is
    the worker's own for a synchronous invoke - the id CloudWatch's
    REPORT line prints. Checked on a success, whose body carries the
    worker's `event.context.request_id`, so no log query is needed.

    Opt-in (pyproject.toml's addopts excludes real_lambda), needs a
    hand-made deployment and `lambda:InvokeFunction`:

        uv run pytest -m real_lambda tests/test_client.py
    """

    async def run() -> tuple[str, bytes]:
        async with lambda_client() as client:
            response = await client.invoke(
                FunctionName=REAL_LAMBDA_FUNCTION_NAME,
                Payload=json.dumps(
                    {
                        "path": "/tmp/map-lambda-request-id-check",
                        "i": 42,
                        "timing_map": {},
                        "default_sleep": 0.0,
                    }
                ),
            )
            return response["ResponseMetadata"]["RequestId"], await response["Payload"].read()

    try:
        api_request_id, payload = asyncio.run(run())
    except (NoCredentialsError, NoRegionError) as e:
        pytest.skip(f"AWS not configured: {e}")

    body = json.loads(payload)
    assert body["status"] == "success", body
    assert api_request_id == body["req_id"], (
        "the response header and the worker's own request id differ, so a failure's "
        "req_id is not the CloudWatch join key - fix read_error_channel in "
        "rust/coordinator/src/invoke.rs"
    )
