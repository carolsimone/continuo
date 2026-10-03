import report


def test_aggregate_reports_p50_p95_and_cost_per_thousand_tasks():
    reps = [{"wall_s": v, "nodes_executed": 1000,
             "usage_run": {"state": {"cpu_seconds": 2.0, "mem_max_mib": 20.0}}} for v in (10.0, 20.0, 30.0)]
    agg = report.aggregate(reps)
    assert agg["wall_s"]["p50"] == 20
    assert agg["wall_s"]["n"] == 3
    assert agg["usage"]["state"]["cpu_s_per_1k_tasks_p50"] == 2.0
    assert agg["usage"]["state"]["mem_peak_mib_p50"] == 20.0


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
