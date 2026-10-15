"""
The session: what a `Lambdas` sends its coordinators, and what
`map_unordered`, `for_each` and `invoke` give back.
"""

import asyncio
import contextlib
from typing import Any

import pytest

from map_lambda import api

from .fakes import RecordingClient, completed, coordinator_result
from .utils import drain, mapped


def _session(client: RecordingClient, **settings: Any) -> api.Lambdas:
    """A `Lambdas` whose Lambda client answers without invoking anything."""
    lambdas = api.Lambdas(**{"coordinators": 1, "concurrency": 4, **settings})
    lambdas._client = client
    return lambdas


def _dispatching_to(monkeypatch: pytest.MonkeyPatch, client: RecordingClient) -> None:
    """Have every session built from here on use `client`."""

    @contextlib.asynccontextmanager
    async def ready(*args: Any, **kwargs: Any) -> Any:
        yield client

    monkeypatch.setattr(api, "lambda_client", ready)


def _answering(*answers: Any) -> RecordingClient:
    """One coordinator answering input `i` with `answers[i]`."""
    outputs = [{"index": i, "response": answer} for i, answer in enumerate(answers)]
    return RecordingClient(
        [
            [
                completed(done=len(answers), outputs=outputs),
                {"Result": coordinator_result(len(answers))},
            ]
        ]
    )


def test_a_session_without_a_client_says_so_rather_than_failing_later():
    with pytest.raises(RuntimeError, match="async with"):
        api.Lambdas().map_unordered("worker", [1], input_key="i")


def test_map_yields_each_answer_and_records_the_partition():
    partitions: list[dict] = []
    lambdas = _session(_answering({"doubled": 2}, {"doubled": 4}), coordinator_stats=partitions)

    answers = mapped(lambdas, "worker", [1, 2], input_key="i")

    assert answers == [{"doubled": 2}, {"doubled": 4}]
    assert len(partitions) == 1
    assert partitions[0]["succeeded"] == 2


def test_map_passes_the_template_and_the_session_settings_through():
    client = RecordingClient([coordinator_result(1)])
    lambdas = _session(client, concurrency=6, retries=3)

    mapped(lambdas, "worker", [[0, 1]], input_key="items", constant_args={"prefix": "p"})

    request = client.payloads[0]
    assert request["target_function"] == "worker"
    assert request["template"] == {"prefix": "p"}
    assert request["input_key"] == "items"
    assert request["retries"] == 3
    assert request["concurrency"] == 6


def test_a_list_input_is_sent_whole_as_one_input():
    """However many items a worker reads out of it."""
    client = RecordingClient([coordinator_result(2)])

    mapped(_session(client), "worker", [[0, 1, 2], [3]], input_key="items")

    assert client.payloads[0]["inputs"] == [[0, 1, 2], [3]]


def test_for_each_dispatches_every_input_and_returns_nothing():
    client = RecordingClient([coordinator_result(2), coordinator_result(2)])
    lambdas = _session(client, coordinators=2)

    result = asyncio.run(lambdas.for_each("worker", [1, 2, 3, 4], input_key="i"))

    assert result is None
    assert [r["inputs"] for r in client.payloads] == [[1, 3], [2, 4]]


def test_for_each_still_collects_invocation_records():
    stats = [
        {"index": i, "req_id": f"w-{i}", "invoke_start": 1.0, "invoke_end": 2.0} for i in (0, 1)
    ]
    client = RecordingClient([[completed(done=2, stats=stats), {"Result": coordinator_result(2)}]])
    records: list[dict] = []
    lambdas = _session(client, invocation_stats=records)

    asyncio.run(lambdas.for_each("worker", [1, 2], input_key="i"))

    assert [r["req_id"] for r in records] == ["w-0", "w-1"]


def test_a_worker_may_have_fields_named_after_maps_own_arguments():
    client = RecordingClient([coordinator_result(1)])

    mapped(
        _session(client),
        "worker",
        [1],
        input_key="i",
        constant_args={"input_key": "not mine", "grouped": "nor this", "inputs": 7},
    )

    assert client.payloads[0]["template"] == {
        "input_key": "not mine",
        "grouped": "nor this",
        "inputs": 7,
    }
    assert client.payloads[0]["input_key"] == "i"


def test_mistyping_one_of_maps_own_arguments_raises():
    lambdas = _session(RecordingClient())

    with pytest.raises(TypeError, match="groupd"):
        lambdas.map_unordered("worker", [[0, 1]], input_key="items", groupd=True)  # type: ignore[call-arg]


def test_the_module_level_map_runs_one_call_through_a_session_of_its_own(monkeypatch):
    client = _answering({"doubled": 2}, {"doubled": 4})
    _dispatching_to(monkeypatch, client)

    answers = drain(
        api.map_unordered(
            "worker", [1, 2], input_key="i", constant_args={"prefix": "p"}, coordinators=1
        )
    )

    assert answers == [(0, {"doubled": 2}), (1, {"doubled": 4})]
    assert client.payloads[0]["template"] == {"prefix": "p"}
    assert client.payloads[0]["target_function"] == "worker"


def test_the_module_level_map_collects_stats_into_the_caller_s_own_lists(monkeypatch):
    """The session closes before this returns, so only the lists outlive it."""
    _dispatching_to(monkeypatch, RecordingClient([coordinator_result(2)]))
    partitions: list[dict] = []

    drain(
        api.map_unordered(
            "worker", [1, 2], input_key="i", coordinators=1, coordinator_stats=partitions
        )
    )

    assert len(partitions) == 1
    assert partitions[0]["succeeded"] == 2
    assert partitions[0]["call"] == 0


def test_the_module_level_for_each_runs_one_call_through_a_session_of_its_own(monkeypatch):
    client = RecordingClient([coordinator_result(2), coordinator_result(1)])
    _dispatching_to(monkeypatch, client)

    asyncio.run(
        api.for_each(
            "worker",
            [1, 2, 3],
            input_key="i",
            constant_args={"prefix": "p"},
            # None of these is the default, so one that never reaches
            # the session shows up here.
            coordinators=2,
            concurrency=50,
            retries=3,
        )
    )

    assert [r["inputs"] for r in client.payloads] == [[1, 3], [2]]
    assert [r["concurrency"] for r in client.payloads] == [25, 25]
    assert client.payloads[0]["retries"] == 3
    assert client.payloads[0]["template"] == {"prefix": "p"}
    assert client.payloads[0]["target_function"] == "worker"
    # The one thing that distinguishes `for_each` on the wire: a worker's
    # body is dropped at the coordinator, not carried back and discarded.
    assert client.payloads[0]["return_outputs"] is False


def test_the_module_level_map_offers_every_session_setting_under_the_same_default():
    """
    `map_unordered` spells `Lambdas`' settings out rather than taking
    `**kwargs`, so a setting added to one and forgotten in the other
    shows up here.
    """
    import dataclasses
    import inspect

    settings = {f.name: f.default for f in dataclasses.fields(api.Lambdas) if f.init}
    params = inspect.signature(api.map_unordered).parameters

    assert set(settings) <= set(params), "a Lambdas setting map_unordered cannot pass"
    assert {name: params[name].default for name in settings} == settings


def test_invoke_dispatches_once_without_waiting_for_the_answer():
    client = RecordingClient()
    lambdas = _session(client)

    asyncio.run(lambdas.invoke("delete-scratch", store="s3://bucket/scratch"))

    assert client.invoked == [("delete-scratch", {"store": "s3://bucket/scratch"})]
    # not dispatch: no partition went out, and nothing was counted
    assert client.payloads == []
    assert lambdas._calls == 0


def test_invoke_raises_when_it_could_not_be_submitted():
    """Submission is the only failure `invoke` can see, so it has to raise."""
    lambdas = _session(RecordingClient(invoke_error=RuntimeError("no such function")))

    with pytest.raises(RuntimeError, match="no such function"):
        asyncio.run(lambdas.invoke("delete-scratch", store="s3://bucket/scratch"))


def test_invoke_without_a_client_says_so_rather_than_failing_later():
    with pytest.raises(RuntimeError, match="async with"):
        asyncio.run(api.Lambdas().invoke("delete-scratch", store="s3://bucket/scratch"))
