import report


def test_aggregate_reports_p50_p95_and_cost_per_thousand_tasks():
    reps = [{"wall_s": v, "nodes_executed": 1000,
             "usage_run": {"run-controller": {"cpu_seconds": 2.0, "mem_max_mib": 20.0}}} for v in (10.0, 20.0, 30.0)]
    agg = report.aggregate(reps)
    assert agg["wall_s"]["p50"] == 20
    assert agg["wall_s"]["n"] == 3
    assert agg["usage"]["run-controller"]["cpu_s_per_1k_tasks_p50"] == 2.0
    assert agg["usage"]["run-controller"]["mem_peak_mib_p50"] == 20.0


def test_render_has_a_section_per_scenario():
    markdown = report.render({"dag-500": [{"wall_s": 1.0, "nodes_executed": 1, "usage_run": {}}]})
    assert "## dag-500" in markdown
    assert "| wall_s |" in markdown


def test_render_flags_reps_with_a_sampling_pause():
    reps = [{"rep": n, "wall_s": 1.0, "nodes_executed": 1, "usage_run": {}, "max_sample_gap_s": gap}
            for n, gap in ((1, 5.0), (2, 400.0), (3, None))]
    markdown = report.render({"dag-500": reps})
    assert "Reps with a sampling pause over 60 s (host asleep or overloaded; exclude them): [2]" in markdown
    assert "sampling pause" not in report.render({"dag-500": reps[:1]})


def test_render_shows_dropped_messages_for_outage_scenarios():
    reps = [{"rep": 1, "wall_s": 1.0, "nodes_executed": 1, "usage_run": {}, "messages_dropped": "3"}]
    assert "| messages_dropped | 3.00 | 3.00 | 1 |" in report.render({"outage-postgres": reps})


def sweep_rep(handoff, **fields):
    base = {"rep": 1, "nodes_executed": 1, "usage_run": {}, "handoff_p50_s": handoff}
    base.update(fields)
    return base


def test_sweep_slope_fits_the_handoff_against_the_level_width():
    scenarios = {"sweep-10": [sweep_rep(2.0)], "sweep-50": [sweep_rep(6.0)], "sweep-100": [sweep_rep(11.0)],
                 "dag-500": [sweep_rep(99.0)]}
    assert round(report.sweep_slope(scenarios), 4) == 0.1
    assert report.sweep_slope({"sweep-10": [sweep_rep(2.0)]}) is None


def test_render_lists_each_target_with_its_measured_value():
    scenarios = {
        "sweep-10": [sweep_rep(2.0, overlapping_attempts=0)],
        "sweep-100": [sweep_rep(11.0, messages_per_attempt=5.0, overlapping_attempts=0)],
        "fail-once": [sweep_rep(None, retry_turnaround_p50_s=26.0, overlapping_attempts=0)],
        "start-image": [sweep_rep(None, overlapping_attempts=20)],
    }
    markdown = report.render(scenarios)
    assert "## Targets" in markdown
    assert "| hand-off p50 at width 100 (sweep-100) | ≤ 5 s | 11.00 |" in markdown
    assert "| hand-off growth per task of level width (sweep-*) | < 0.1 s | 0.10 |" in markdown
    assert "| retry turnaround p50 (fail-once) | ≤ 5 s | 26.00 |" in markdown
    assert "| messages per attempt (sweep-100) | 4 | 5.00 |" in markdown
    assert "| overlapping attempts, largest of any scenario | 0 | 20.00 |" in markdown


def test_render_shows_the_retry_overlap_and_breakdown_measures():
    reps = [{"rep": 1, "nodes_executed": 1, "usage_run": {}, "retry_turnaround_p50_s": 3.0, "round_trip_p50_s": 1.5,
             "messages_per_attempt": 4.0, "overlapping_attempts": 0, "retries": 2}]
    markdown = report.render({"fail-once": reps})
    for line in ("| retry_turnaround_p50_s | 3.00 | 3.00 | 1 |", "| round_trip_p50_s | 1.50 | 1.50 | 1 |",
                 "| retries | 2.00 | 2.00 | 1 |",
                 "| messages_per_attempt | 4.00 | 4.00 | 1 |", "| overlapping_attempts | 0.00 | 0.00 | 1 |"):
        assert line in markdown
