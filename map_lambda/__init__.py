"""
A highly-scalable parallel Python map function running on Rust AWS lambdas.

Most users will use `map_unordered` (when the function return values are
needed) or `for_each` (when they are not):

    import map_lambda

    async for index, output in map_lambda.map_unordered(
        "square", [1, 2, 3], input_key="input"
    ):
        # output is what inputs[index] came back with

Outputs arrive as the workers finish them, so `index` is what says which
input each one ran.

For several calls, create a `Session` object to hold the settings
and the stats across them:

    from map_lambda import Session

    async with Session(dispatchers=4, concurrency=500) as session:
        async for index, output in session.map_unordered(
            "square", list(range(10_000)), input_key="input"
        ):
            ...

The public API also includes utilities for analysing stats:

* `stats` - what calls recorded about themselves: `summarize_dispatchers`
  for the dispatchers and `summarize_invocations` for the workers.
* `timeline.generate_timeline` - a chart of invocation stats.
"""

from .api import (
    DEFAULT_CONCURRENCY,
    DISPATCHER,
    LOCAL_ENDPOINT_URL,
    Session,
    for_each,
    map_unordered,
)

__all__ = [
    "DEFAULT_CONCURRENCY",
    "DISPATCHER",
    "LOCAL_ENDPOINT_URL",
    "Session",
    "for_each",
    "map_unordered",
]
