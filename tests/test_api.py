"""
The session: what a `Lambdas` hands its dispatcher, and what
`map_unordered` gives back.
"""

import asyncio

import pytest

from map_lambda import api
from map_lambda.testing import InProcessCoordinator, dispatching_to

from .utils import drain, mapped


def _session(worker, **settings):
    """A `Lambdas` whose client is an in-process coordinator."""
    lambdas = api.Lambdas(**{"coordinators": 1, "concurrency": 4, **settings})
    lambdas._client = InProcessCoordinator(worker)
    return lambdas


def test_a_session_without_a_client_says_so_rather_than_failing_later():
    with pytest.raises(RuntimeError, match="async with"):
        api.Lambdas().map_unordered("worker", [1], input_key="i")


def test_map_returns_one_answer_per_input_carrying_the_workers_body():
    partitions: list[dict] = []
    lambdas = _session(lambda payload: {"doubled": payload["i"] * 2}, coordinator_stats=partitions)

    answers = mapped(lambdas, "worker", [1, 2, 3], input_key="i", constant_args={"prefix": "p"})

    assert [a["doubled"] for a in answers] == [2, 4, 6]
    assert len(partitions) == 1


def test_map_passes_the_template_and_the_session_settings_through():
    fake = InProcessCoordinator(lambda payload: None)
    lambdas = api.Lambdas(coordinators=1, concurrency=6, retries=3)
    lambdas._client = fake

    mapped(lambdas, "worker", [[0, 1]], input_key="items", constant_args={"prefix": "p"})

    request = fake.requests[0]
    assert request["target_function"] == "worker"
    assert request["template"] == {"prefix": "p"}
    assert request["input_key"] == "items"
    assert request["inputs"] == [[0, 1]]
    assert request["retries"] == 3
    assert request["concurrency"] == 6


def test_a_list_input_reaches_one_worker_whole_and_answers_in_its_own_place():
    lambdas = _session(lambda payload: {"items": len(payload["items"])})

    answers = mapped(lambdas, "worker", [[0, 1, 2], [3]], input_key="items")

    # a list is one input, however many items a worker reads out of it
    assert [a["items"] for a in answers] == [3, 1]


def test_a_worker_that_answers_nothing_is_never_heard_from():
    """
    There is no place left empty, because there is no place: a run
    yields fewer answers than it had inputs.
    """
    partitions: list[dict] = []
    lambdas = _session(lambda payload: None, coordinator_stats=partitions)

    assert drain(lambdas.map_unordered("worker", [1, 2, 3], input_key="i")) == []
    # the work still happened, and the report says so
    assert partitions[0]["succeeded"] == 3


def test_for_each_dispatches_the_same_and_returns_nothing():
    fake = InProcessCoordinator(lambda payload: None)
    lambdas = api.Lambdas(coordinators=2, concurrency=4)
    lambdas._client = fake

    result = asyncio.run(lambdas.for_each("worker", [1, 2, 3, 4], input_key="i"))

    assert result is None
    # every input still went out, partitioned exactly as `map_unordered` would have
    assert sorted(i for r in fake.requests for i in r["inputs"]) == [1, 2, 3, 4]


def test_for_each_discards_answers_rather_than_preventing_them():
    records: list[dict] = []
    lambdas = _session(lambda payload: {"chatty": payload["i"]}, invocation_stats=records)

    assert asyncio.run(lambdas.for_each("worker", [1, 2], input_key="i")) is None
    assert len(records) == 2


def test_a_test_session_dispatches_through_an_in_process_coordinator():
    from map_lambda.testing import session

    lambdas = session(lambda payload: {"seen": payload["i"]}, retries=1)

    answers = mapped(lambdas, "worker", [1, 2], input_key="i")

    assert [a["seen"] for a in answers] == [1, 2]
    assert lambdas.retries == 1
    # small by default, so a test can reason about which partition got what
    assert (lambdas.coordinators, lambdas.concurrency) == (2, 4)


def test_a_worker_may_have_fields_named_after_maps_own_arguments():
    fake = InProcessCoordinator(lambda payload: None)
    lambdas = api.Lambdas(coordinators=1)
    lambdas._client = fake

    mapped(
        lambdas,
        "worker",
        [1],
        input_key="i",
        constant_args={"input_key": "not mine", "grouped": "nor this", "inputs": 7},
    )

    assert fake.requests[0]["template"] == {
        "input_key": "not mine",
        "grouped": "nor this",
        "inputs": 7,
    }
    assert fake.requests[0]["input_key"] == "i"


def test_mistyping_one_of_maps_own_arguments_raises():
    lambdas = _session(lambda payload: None)

    with pytest.raises(TypeError, match="groupd"):
        lambdas.map_unordered("worker", [[0, 1]], input_key="items", groupd=True)


def test_the_module_level_map_runs_one_call_through_a_session_of_its_own():
    with dispatching_to(lambda payload: {"doubled": payload["i"] * 2}) as coordinator:
        answers = drain(
            api.map_unordered(
                "worker", [1, 2, 3], input_key="i", constant_args={"prefix": "p"}, coordinators=1
            )
        )

    assert sorted(answer["doubled"] for _, answer in answers) == [2, 4, 6]
    assert coordinator.requests[0]["template"] == {"prefix": "p"}
    assert coordinator.requests[0]["target_function"] == "worker"


def test_the_module_level_map_collects_stats_into_the_caller_s_own_lists():
    """The session closes before this returns, so only the lists outlive it."""
    partitions: list[dict] = []

    with dispatching_to(lambda payload: None):
        drain(api.map_unordered("worker", [1, 2], input_key="i", coordinator_stats=partitions))

    assert len(partitions) == 1
    assert partitions[0]["succeeded"] == 2
    assert partitions[0]["call"] == 0


def test_the_module_level_for_each_runs_one_call_through_a_session_of_its_own():
    ran: list[int] = []

    with dispatching_to(lambda payload: ran.append(payload["i"])) as coordinator:
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

    assert sorted(ran) == [1, 2, 3]
    assert len(coordinator.requests) == 2
    assert [r["concurrency"] for r in coordinator.requests] == [25, 25]
    assert coordinator.requests[0]["retries"] == 3
    assert coordinator.requests[0]["template"] == {"prefix": "p"}
    assert coordinator.requests[0]["target_function"] == "worker"
    # The one thing that distinguishes `for_each` on the wire: a worker's
    # body is dropped at the coordinator, not carried back and discarded.
    assert coordinator.requests[0]["return_outputs"] is False


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
    fake = InProcessCoordinator(lambda payload: None)
    lambdas = api.Lambdas()
    lambdas._client = fake

    asyncio.run(lambdas.invoke("delete-scratch", store="s3://bucket/scratch"))

    assert fake.invoked == [("delete-scratch", {"store": "s3://bucket/scratch"})]
    # not dispatch: no partition went out, and nothing was counted
    assert fake.requests == []
    assert lambdas._calls == 0


def test_invoke_raises_when_it_could_not_be_submitted():
    """Submission is the only failure `invoke` can see, so it has to raise."""
    fake = InProcessCoordinator(lambda payload: None, invoke_error=RuntimeError("no such function"))
    lambdas = api.Lambdas()
    lambdas._client = fake

    with pytest.raises(RuntimeError, match="no such function"):
        asyncio.run(lambdas.invoke("delete-scratch", store="s3://bucket/scratch"))


def test_invoke_without_a_client_says_so_rather_than_failing_later():
    with pytest.raises(RuntimeError, match="async with"):
        asyncio.run(api.Lambdas().invoke("delete-scratch", store="s3://bucket/scratch"))


def test_an_answer_is_tied_to_its_input_by_index():
    """By `index`, which is exact where `i` is not: these inputs repeat."""
    lambdas = _session(lambda payload: {"sq": payload["i"] ** 2}, coordinators=2)
    inputs = [4, 1, 3, 1]

    answers = mapped(lambdas, "worker", inputs, input_key="i")

    assert [a["sq"] for a in answers] == [16, 1, 9, 1]


def test_where_an_answer_arrived_says_nothing_about_which_input_it_ran():
    """
    Half the workers answer, so input 6's answer is the fourth to
    arrive: only `index` puts it back at 6, and counting arrivals would
    put it at 3.
    """
    lambdas = _session(
        lambda payload: {"sq": payload["i"] ** 2} if payload["i"] % 2 == 0 else None,
        coordinators=2,
    )
    inputs = list(range(8))

    answers = drain(lambdas.map_unordered("worker", inputs, input_key="i"))

    assert len(answers) == 4
    assert sorted((index, answer["sq"]) for index, answer in answers) == [
        (0, 0),
        (2, 4),
        (4, 16),
        (6, 36),
    ]


def test_map_counts_inputs_not_what_a_worker_reads_out_of_one():
    lambdas = _session(lambda payload: {"n": len(payload["items"])}, coordinators=1)
    inputs = [[0, 1, 2], [3], [4, 5]]

    answers = mapped(lambdas, "worker", inputs, input_key="items")

    assert [a["n"] for a in answers] == [3, 1, 2]
