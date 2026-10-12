"""
A timeline of a run: one row per invocation, showing when its call
began, when it went on the wire, when the function actually ran, and
when it came back.

Adapted from cubed's `cubed/diagnostics/timeline.py`.
"""

from collections.abc import Sequence
from typing import Any

from .stats import Stats

# (label, key, colour). Colours are fixed so the execution band never
# shares one with a scatter series. `call_start` is shared by a whole
# phase, so it plots as a vertical line.
CLIENT_FIELDS = [
    ("call", "call_start", "tab:blue"),
    ("invoke start", "invoke_start", "tab:orange"),
    ("invoke end", "invoke_end", "tab:purple"),
]

EXECUTION_COLOUR = "tab:green"

# Past this the plot is thinned to every nth invocation, to keep an SVG
# openable: a 23,612-task run is otherwise ~118,000 points.
DEFAULT_MAX_POINTS = 4000


def phases(records: Sequence[Stats]) -> list[list[Stats]]:
    """
    A run's invocations grouped by the call they came from, in
    order.
    """
    groups: dict[float, list[Stats]] = {}
    for record in records:
        groups.setdefault(record["call"], []).append(record)
    return [groups[key] for key in sorted(groups)]


def thin(records: list[Stats], max_points: int) -> tuple[list[Stats], int]:
    """
    Every nth record, and the stride used - 1 when nothing was dropped.

    Striding an already-sorted list rather than sampling at random keeps
    each phase's share of the plot, and keeps the staircase edges that
    are the point of looking.
    """
    if max_points <= 0 or len(records) <= max_points:
        return records, 1
    stride = -(-len(records) // max_points)
    return records[::stride], stride


def execution_window(record: Stats) -> tuple[float, float] | None:
    """
    When the function ran, anchored to the end of the client interval.

    `init_ms` is Lambda's cold-start setup, billed and reported
    separately from `@duration`, so a cold invocation's band covers
    both. Returns None for a record with no REPORT line joined to it.
    """
    execution_ms = record.get("execution_ms")
    if execution_ms is None:
        return None
    total_s = (execution_ms + (record.get("init_ms") or 0.0)) / 1000
    return record["invoke_end"] - total_s, record["invoke_end"]


def generate_timeline(
    records: Sequence[Stats],
    title: str | None = None,
    max_points: int = DEFAULT_MAX_POINTS,
) -> Any:
    """
    Build the figure. Returns a matplotlib Figure for the caller to save.

    matplotlib is imported lazily: it is an optional extra
    (`map-lambda[diagnostics]`).
    """
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if not records:
        raise ValueError("no invocation records to plot")

    # By completion, not dispatch: a phase dispatches within a fraction of
    # a second, so dispatch order is near-arbitrary, while completion order
    # shows the waves as a staircase. `invoke end` is monotone by
    # construction.
    ordered = sorted(records, key=lambda r: (r["call"], r["invoke_end"]))
    start = min(r["call_start"] for r in ordered)
    groups = phases(ordered)
    shown, stride = thin(ordered, max_points)
    y = range(len(shown))

    fig, ax = plt.subplots(figsize=(10, 8))

    windows = [execution_window(r) for r in shown]
    if any(w is not None for w in windows):
        # one line per task rather than a scatter pair, so the queueing
        # gap in front of it is readable as a gap
        for row, window in zip(y, windows, strict=True):
            if window is not None:
                ax.plot(
                    [window[0] - start, window[1] - start],
                    [row, row],
                    color=EXECUTION_COLOUR,
                    linewidth=0.9,
                    alpha=0.6,
                    solid_capstyle="butt",
                    rasterized=True,
                )
        ax.plot([], [], color=EXECUTION_COLOUR, linewidth=2, label="function running")

    for label, key, colour in CLIENT_FIELDS:
        ax.scatter(
            [r[key] - start for r in shown],
            list(y),
            label=label,
            color=colour,
            edgecolor="none",
            s=7,
            alpha=0.8,
            rasterized=True,
        )

    if len(groups) > 1:
        boundary = 0
        for n, group in enumerate(groups):
            rows = len(group) // stride if stride > 1 else len(group)
            ax.axhline(boundary, color="0.6", linewidth=0.8, linestyle="--")
            ax.text(
                0.995,
                boundary + rows / 2,
                f"op {n}\n{len(group):,} tasks",
                transform=ax.get_yaxis_transform(),
                ha="right",
                va="center",
                fontsize=8,
                color="0.35",
            )
            boundary += rows

    ax.set_xlabel("Time since first task created (sec)")
    order = "in completion order"
    ax.set_ylabel(f"Task, {order} (every {stride:,})" if stride > 1 else f"Task, {order}")
    ax.set_ylim(-0.01 * len(shown), 1.01 * len(shown))
    ax.grid(True, alpha=0.3)
    if title:
        ax.set_title(title, pad=28)
    # above the axes: the phase labels own the right margin, and a run
    # whose tasks fill the plot leaves no reliable corner free
    ax.legend(
        loc="lower left",
        bbox_to_anchor=(0, 1.02, 1, 0.1),
        mode="expand",
        ncol=4,
        frameon=False,
        fontsize=9,
    )
    fig.tight_layout()
    return fig
