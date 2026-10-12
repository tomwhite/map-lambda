import asyncio
from pathlib import Path

import obstore as obs


def path_to_store(path):
    if isinstance(path, str):
        if "://" not in path:
            return obs.store.from_url(Path(path).as_uri(), mkdir=True)
        else:
            return obs.store.from_url(path)
    elif isinstance(path, Path):
        return obs.store.from_url(path.as_uri(), mkdir=True)


def check_invocation_counts(
    path, timing_map, n_tasks, retries=None, expected_invocation_counts_overrides=None
):
    asyncio.run(
        check_invocation_counts_async(
            path,
            timing_map,
            n_tasks,
            retries=retries,
            expected_invocation_counts_overrides=expected_invocation_counts_overrides,
        )
    )


async def check_invocation_counts_async(
    path, timing_map, n_tasks, retries=None, expected_invocation_counts_overrides=None
):
    expected_invocation_counts = {}
    for i in range(n_tasks):
        if i not in timing_map:
            expected_invocation_counts[i] = 1
        else:
            timing_codes = timing_map[i]
            expected_invocation_count = len(timing_codes) + 1

            if retries is not None:
                # there shouldn't have been more than retries + 1 invocations
                max_invocations = retries + 1
                expected_invocation_count = min(expected_invocation_count, max_invocations)

            expected_invocation_counts[i] = expected_invocation_count

    if expected_invocation_counts_overrides is not None:
        expected_invocation_counts.update(expected_invocation_counts_overrides)

    # retrieve outputs concurrently, so we can test on large numbers of inputs
    store = path_to_store(path)
    paths = [str(i) for i in range(n_tasks)]
    results = await asyncio.gather(*[obs.get_async(store, path) for path in paths])
    values = await asyncio.gather(*[result.bytes_async() for result in results])
    actual_invocation_counts = {i: int(val) for i, val in enumerate(values)}

    if actual_invocation_counts != expected_invocation_counts:
        for i, expected_count in expected_invocation_counts.items():
            actual_count = actual_invocation_counts[i]
            if actual_count != expected_count:
                print(
                    f"Invocation count for {i}, expected: {expected_count}, actual: {actual_count}"
                )
    assert actual_invocation_counts == expected_invocation_counts


def mapped(lambdas, function_name, inputs, **kwargs):
    """
    `map_unordered` drained into input order, with `None` where a worker
    answered nothing.

    What most of these tests want to assert on, and what a caller who
    needs every answer together writes for itself.
    """

    async def go():
        values = [None] * len(inputs)
        async for index, answer in lambdas.map_unordered(function_name, inputs, **kwargs):
            values[index] = answer
        return values

    return asyncio.run(go())


def drain(stream):
    """Every `(index, answer)` a `map_unordered` yields, in arrival order."""

    async def go():
        return [pair async for pair in stream]

    return asyncio.run(go())
