"""
A highly-scalable parallel Python map function running on Rust AWS lambdas.

Most users will use `map_unordered` (when the function return values are
needed) or `for_each` (when they are not):

    import map_lambda

    async for index, answer in map_lambda.map_unordered(
        "square", [1, 2, 3], input_key="input"
    ):
        # answer is what inputs[index] came back with

Answers arrive as the workers finish them, so `index` is what says which
input each one ran.

For a run of several calls, create a `Lambdas` object to hold the settings
and the stats across them:

    from map_lambda import Lambdas

    async with Lambdas(dispatchers=4, concurrency=500) as lambdas:
        async for index, answer in lambdas.map_unordered(
            "square", list(range(10_000)), input_key="input"
        ):
            ...

The public API also includes utilities for analysing stats for a run:

* `stats` - what a run recorded about itself: `summarize_dispatchers`
  for the dispatchers and `summarize_invocations` for the workers.
* `timeline.generate_timeline` - a chart of a run from invocation stats.
"""

from .api import DEFAULT_CONCURRENCY, LOCAL_ENDPOINT_URL, Lambdas, for_each, map_unordered

__all__ = [
    "DEFAULT_CONCURRENCY",
    "LOCAL_ENDPOINT_URL",
    "Lambdas",
    "for_each",
    "map_unordered",
]
