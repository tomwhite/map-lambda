import pytest

from map_lambda import timeline


def _record(call_start, start, end, call=None, **extra):
    """One invocation record. `call` defaults to its `call_start`."""
    return {
        "i": [0],
        "req_id": f"req-{start}",
        "call": call_start if call is None else call,
        "call_start": call_start,
        "invoke_start": start,
        "invoke_end": end,
        **extra,
    }


def test_phases_group_by_the_batch_a_task_was_created_in():
    records = [
        _record(200.0, 200.1, 201.0),
        _record(100.0, 100.1, 101.0),
        _record(100.0, 100.2, 101.5),
    ]
    grouped = timeline.phases(records)
    assert [len(g) for g in grouped] == [2, 1]
    assert grouped[0][0]["call_start"] == 100.0


def test_a_single_pass_run_is_one_phase():
    records = [_record(100.0, 100.0 + i, 101.0 + i) for i in range(5)]
    assert len(timeline.phases(records)) == 1


def test_thin_keeps_everything_under_the_limit():
    records = [_record(1.0, i, i + 1) for i in range(10)]
    assert timeline.thin(records, 100) == (records, 1)
    assert timeline.thin(records, 0) == (records, 1)


def test_thin_takes_every_nth_past_the_limit():
    records = [_record(1.0, i, i + 1) for i in range(100)]
    shown, stride = timeline.thin(records, 10)
    assert stride == 10
    assert len(shown) == 10
    assert shown[0] is records[0] and shown[1] is records[10]


def test_no_execution_window_without_a_report_line():
    assert timeline.execution_window(_record(0.0, 1.0, 2.0)) is None


def test_the_execution_window_is_anchored_to_the_end_of_the_call():
    """Lambda reports a duration, never a start against our clock."""
    record = _record(0.0, 1.0, 5.0, execution_ms=500.0, init_ms=None)
    assert timeline.execution_window(record) == (4.5, 5.0)


def test_a_cold_start_band_covers_init_as_well():
    # Lambda reports init separately from @duration
    record = _record(0.0, 1.0, 5.0, execution_ms=500.0, init_ms=200.0)
    assert timeline.execution_window(record) == (pytest.approx(4.3), 5.0)


def test_generate_timeline_without_cloudwatch_draws_only_client_series():
    records = [_record(100.0, 100.0 + i * 0.01, 101.0 + i) for i in range(20)]
    figure = timeline.generate_timeline(records, title="t")
    labels = [text.get_text() for text in figure.axes[0].get_legend().get_texts()]
    assert labels == ["call", "invoke start", "invoke end"]


def test_generate_timeline_with_cloudwatch_adds_the_execution_band():
    records = [
        _record(100.0, 100.0 + i * 0.01, 101.0 + i, execution_ms=300.0, init_ms=None)
        for i in range(20)
    ]
    figure = timeline.generate_timeline(records)
    labels = [text.get_text() for text in figure.axes[0].get_legend().get_texts()]
    assert "function running" in labels


def test_a_run_with_no_records_is_an_error_rather_than_an_empty_chart():
    with pytest.raises(ValueError, match="no invocation records"):
        timeline.generate_timeline([])


def test_phase_separators_are_only_drawn_for_a_multi_pass_run():
    single = [_record(100.0, 100.0 + i, 101.0 + i) for i in range(4)]
    multi = single + [_record(200.0, 200.0 + i, 201.0 + i) for i in range(4)]
    assert len(timeline.generate_timeline(single).axes[0].lines) == 0
    # one axhline per phase, plus the legend's proxy line if present
    assert len(timeline.generate_timeline(multi).axes[0].lines) == 2
