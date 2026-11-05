"""
The session: what a `Session` sends its dispatchers, and what
`map_unordered`, `for_each` and `invoke_detached` give back.
"""

import asyncio
from typing import Any

import pytest

from map_lambda import api

from .fakes import RecordingInvoker, completed, dispatcher_summary
from .utils import drain, mapped


def _session(invoker: RecordingInvoker, **settings: Any) -> api.Session:
    """A `Session` whose invoker responds without invoking anything."""
    session = api.Session(**{"dispatchers": 1, "concurrency": 4, **settings})
    session._invoker = invoker
    return session


def _dispatching_to(monkeypatch: pytest.MonkeyPatch, invoker: RecordingInvoker) -> None:
    """Have every session built from here on use `client`."""

    async def ready(*args: Any, **kwargs: Any) -> Any:
        return invoker

    monkeypatch.setattr(api._native, "connect", ready)


def _returning(*outputs: Any) -> RecordingInvoker:
    """One dispatcher returning for input `i` with `outputs[i]`."""
    frame_outputs = [{"index": i, "value": output} for i, output in enumerate(outputs)]
    return RecordingInvoker(
        [
            [
                completed(succeeded=len(outputs), outputs=frame_outputs),
                {"Summary": dispatcher_summary(len(outputs))},
            ]
        ]
    )


def test_a_session_without_a_client_says_so_rather_than_failing_later():
    with pytest.raises(RuntimeError, match="async with"):
        api.Session().map_unordered("worker", [1], input_key="i")


def test_map_yields_each_answer_and_records_the_partition():
    partitions: list[dict] = []
    session = _session(_returning({"doubled": 2}, {"doubled": 4}), dispatcher_stats=partitions)

    outputs = mapped(session, "worker", [1, 2], input_key="i")

    assert outputs == [{"doubled": 2}, {"doubled": 4}]
    assert len(partitions) == 1
    assert partitions[0]["dispatched"] == 2


def test_map_passes_the_template_and_the_session_settings_through():
    invoker = RecordingInvoker([dispatcher_summary(1)])
    session = _session(invoker, concurrency=6, retries=3)

    mapped(session, "worker", [[0, 1]], input_key="items", constant_args={"prefix": "p"})

    request = invoker.payloads[0]
    assert request["worker_function"] == "worker"
    assert request["constant_args"] == {"prefix": "p"}
    assert request["input_key"] == "items"
    assert request["retries"] == 3
    assert request["max_in_flight"] == 6


def test_a_project_prefixes_the_dispatcher_and_the_worker():
    invoker = RecordingInvoker([dispatcher_summary(1)])

    mapped(_session(invoker, project="proj"), "copy", [1], input_key="i")

    assert invoker.dispatched_to == ["proj-dispatcher"]
    assert invoker.payloads[0]["worker_function"] == "proj-copy"


def test_a_memory_size_names_the_worker_deployed_at_it():
    invoker = RecordingInvoker([dispatcher_summary(1), dispatcher_summary(1)])
    session = _session(invoker, project="proj")

    mapped(session, "copy", [1], input_key="i", memory=3538)
    asyncio.run(session.for_each("copy", [1], input_key="i", memory=1024))

    assert [r["worker_function"] for r in invoker.payloads] == ["proj-copy-3538", "proj-copy-1024"]


def test_without_a_project_the_names_are_used_as_they_are():
    invoker = RecordingInvoker([dispatcher_summary(1)])

    mapped(_session(invoker), "copy", [1], input_key="i")

    assert invoker.dispatched_to == ["dispatcher"]
    assert invoker.payloads[0]["worker_function"] == "copy"


def test_a_session_gives_the_names_its_calls_invoke():
    session = api.Session(project="proj")

    assert session.function_name("copy") == "proj-copy"
    assert session.function_name("copy", memory=3538) == "proj-copy-3538"
    assert session.function_name(api.DISPATCHER) == "proj-dispatcher"
    assert api.Session().function_name("copy", memory=3538) == "copy-3538"


def test_a_local_session_gives_the_plain_names():
    session = api.Session(endpoint_url=api.LOCAL_ENDPOINT_URL, project="proj")

    assert session.function_name("copy", memory=3538) == "copy"


def test_a_list_input_is_sent_whole_as_one_input():
    """However many items a worker reads out of it."""
    invoker = RecordingInvoker([dispatcher_summary(2)])

    mapped(_session(invoker), "worker", [[0, 1, 2], [3]], input_key="items")

    assert invoker.payloads[0]["inputs"] == [[0, 1, 2], [3]]


def test_for_each_dispatches_every_input_and_returns_nothing():
    invoker = RecordingInvoker([dispatcher_summary(2), dispatcher_summary(2)])
    session = _session(invoker, dispatchers=2)

    result = asyncio.run(session.for_each("worker", [1, 2, 3, 4], input_key="i"))

    assert result is None
    assert [r["inputs"] for r in invoker.payloads] == [[1, 3], [2, 4]]


def test_for_each_still_collects_invocation_records():
    stats = [
        {"index": i, "req_id": f"w-{i}", "invoke_start": 1.0, "invoke_end": 2.0} for i in (0, 1)
    ]
    invoker = RecordingInvoker(
        [[completed(succeeded=2, invocation_stats=stats), {"Summary": dispatcher_summary(2)}]]
    )
    records: list[dict] = []
    session = _session(invoker, invocation_stats=records)

    asyncio.run(session.for_each("worker", [1, 2], input_key="i"))

    assert [r["req_id"] for r in records] == ["w-0", "w-1"]


def test_a_worker_may_have_fields_named_after_maps_own_arguments():
    invoker = RecordingInvoker([dispatcher_summary(1)])

    mapped(
        _session(invoker),
        "worker",
        [1],
        input_key="i",
        constant_args={"input_key": "not mine", "grouped": "nor this", "inputs": 7},
    )

    assert invoker.payloads[0]["constant_args"] == {
        "input_key": "not mine",
        "grouped": "nor this",
        "inputs": 7,
    }
    assert invoker.payloads[0]["input_key"] == "i"


def test_mistyping_one_of_maps_own_arguments_raises():
    session = _session(RecordingInvoker())

    with pytest.raises(TypeError, match="groupd"):
        session.map_unordered("worker", [[0, 1]], input_key="items", groupd=True)  # type: ignore[call-arg]


def test_the_module_level_map_runs_one_call_through_a_session_of_its_own(monkeypatch):
    invoker = _returning({"doubled": 2}, {"doubled": 4})
    _dispatching_to(monkeypatch, invoker)

    outputs = drain(
        api.map_unordered(
            "worker", [1, 2], input_key="i", constant_args={"prefix": "p"}, dispatchers=1
        )
    )

    assert outputs == [(0, {"doubled": 2}), (1, {"doubled": 4})]
    assert invoker.payloads[0]["constant_args"] == {"prefix": "p"}
    assert invoker.payloads[0]["worker_function"] == "worker"


def test_the_module_level_map_collects_stats_into_the_caller_s_own_lists(monkeypatch):
    """The session closes before this returns, so only the lists outlive it."""
    _dispatching_to(monkeypatch, RecordingInvoker([dispatcher_summary(2)]))
    partitions: list[dict] = []

    drain(
        api.map_unordered(
            "worker", [1, 2], input_key="i", dispatchers=1, dispatcher_stats=partitions
        )
    )

    assert len(partitions) == 1
    assert partitions[0]["dispatched"] == 2
    assert partitions[0]["call"] == 0


def test_the_module_level_for_each_runs_one_call_through_a_session_of_its_own(monkeypatch):
    invoker = RecordingInvoker([dispatcher_summary(2), dispatcher_summary(1)])
    _dispatching_to(monkeypatch, invoker)

    asyncio.run(
        api.for_each(
            "worker",
            [1, 2, 3],
            input_key="i",
            constant_args={"prefix": "p"},
            # None of these is the default, so one that never reaches
            # the session shows up here.
            dispatchers=2,
            concurrency=50,
            retries=3,
        )
    )

    assert [r["inputs"] for r in invoker.payloads] == [[1, 3], [2]]
    assert [r["max_in_flight"] for r in invoker.payloads] == [25, 25]
    assert invoker.payloads[0]["retries"] == 3
    assert invoker.payloads[0]["constant_args"] == {"prefix": "p"}
    assert invoker.payloads[0]["worker_function"] == "worker"
    # The one thing that distinguishes `for_each` on the wire: a worker's
    # body is dropped at the dispatcher, not carried back and discarded.
    assert invoker.payloads[0]["return_outputs"] is False


def test_the_module_level_map_offers_every_session_setting_under_the_same_default():
    """
    `map_unordered` spells `Session`' settings out rather than taking
    `**kwargs`, so a setting added to one and forgotten in the other
    shows up here.
    """
    import dataclasses
    import inspect

    settings = {f.name: f.default for f in dataclasses.fields(api.Session) if f.init}
    params = inspect.signature(api.map_unordered).parameters

    assert set(settings) <= set(params), "a Session setting map_unordered cannot pass"
    assert {name: params[name].default for name in settings} == settings


def test_invoke_dispatches_once_without_waiting_for_the_answer():
    invoker = RecordingInvoker()
    session = _session(invoker)

    asyncio.run(session.invoke_detached("delete-scratch", store="s3://bucket/scratch"))

    assert invoker.invoked == [("delete-scratch", {"store": "s3://bucket/scratch"})]
    # not dispatch: no partition went out
    assert invoker.payloads == []


def test_invoke_prefixes_the_function_with_the_project():
    invoker = RecordingInvoker()

    asyncio.run(_session(invoker, project="proj").invoke_detached("delete-scratch"))

    assert invoker.invoked == [("proj-delete-scratch", {})]


def test_invoke_is_not_counted_as_a_call():
    invoker = RecordingInvoker([dispatcher_summary(1)])
    stats: list[dict[str, Any]] = []
    session = _session(invoker, dispatcher_stats=stats)

    async def invoke_then_call() -> None:
        await session.invoke_detached("delete-scratch")
        await session.for_each("worker", [0], input_key="i")

    asyncio.run(invoke_then_call())

    assert [record["call"] for record in stats] == [0]


def test_invoke_raises_when_it_could_not_be_submitted():
    """Submission is the only failure `invoke_detached` can see, so it has to raise."""

    async def invoke_nowhere() -> None:
        # Nothing listens on the discard port, so the connection is refused.
        async with api.Session(endpoint_url="http://127.0.0.1:9") as session:
            await session.invoke_detached("delete-scratch", store="s3://bucket/scratch")

    with pytest.raises(RuntimeError, match="invoking Lambda failed"):
        asyncio.run(invoke_nowhere())


def test_invoke_without_a_client_says_so_rather_than_failing_later():
    with pytest.raises(RuntimeError, match="async with"):
        asyncio.run(api.Session().invoke_detached("delete-scratch", store="s3://bucket/scratch"))
