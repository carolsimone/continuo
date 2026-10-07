import json

import compare


def rep(n, **fields):
    base = {"rep": n, "nodes_executed": 1000, "usage_run": {}, "final_status": "succeeded"}
    base.update(fields)
    return base


def test_compare_shows_each_sides_p50_and_the_change():
    before = {"dag-500": [rep(n, wall_s=v) for n, v in ((1, 10.0), (2, 20.0), (3, 30.0))]}
    after = {"dag-500": [rep(n, wall_s=v) for n, v in ((1, 5.0), (2, 10.0), (3, 15.0))]}
    markdown = compare.compare(before, after, "A", "B")
    assert "## dag-500" in markdown
    assert "| wall_s | 20.00 | 10.00 | -10.00 (-50%) |" in markdown


def test_compare_leaves_out_reps_with_a_sampling_pause():
    before = {"dag-500": [rep(1, wall_s=10.0, max_sample_gap_s=5.0), rep(2, wall_s=20.0),
                          rep(3, wall_s=1000.0, max_sample_gap_s=400.0)]}
    after = {"dag-500": [rep(1, wall_s=15.0)]}
    markdown = compare.compare(before, after, "A", "B")
    assert "| wall_s | 15.00 | 15.00 | +0.00 (+0%) |" in markdown
    assert "Left out, sampling pause over 60 s: A rep [3]" in markdown


def test_compare_notes_when_the_outcomes_differ():
    before = {"dag-2000": [rep(1, wall_s=1865.0, final_status="cancelled")]}
    after = {"dag-2000": [rep(1, wall_s=1500.0)]}
    note = "Outcomes differ (A: cancelled; B: succeeded)"
    assert note in compare.compare(before, after, "A", "B")
    assert "Outcomes differ" not in compare.compare(after, after, "A", "B")


def test_compare_names_a_scenario_found_on_one_side_only():
    before = {"dag-500": [rep(1, wall_s=1.0)], "test-2000": [rep(1, wall_s=1.0)]}
    after = {"dag-500": [rep(1, wall_s=1.0)]}
    markdown = compare.compare(before, after, "A", "B")
    assert "## test-2000\n\nOnly in A." in markdown


def test_compare_shows_no_percentage_against_a_zero_and_a_dash_for_a_missing_value():
    before = {"cancel-500": [rep(1, overlap_s=0.0)]}
    after = {"cancel-500": [rep(1, overlap_s=5.0, messages_dropped=0)]}
    markdown = compare.compare(before, after, "A", "B")
    assert "| overlap_s | 0.00 | 5.00 | +5.00 |" in markdown
    assert "| messages_dropped | — | 0.00 | — |" in markdown


def test_compare_takes_the_relative_change_against_a_negative_value_by_its_size():
    # A cancelled run can report finalize_s below zero: the run is finalized before its last Job finishes.
    before = {"cascade-2000": [rep(1, finalize_s=-12.0)]}
    assert "| finalize_s | -12.00 | -12.00 | +0.00 (+0%) |" in compare.compare(before, before, "A", "B")
    after = {"cascade-2000": [rep(1, finalize_s=-6.0)]}
    assert "| finalize_s | -12.00 | -6.00 | +6.00 (+50%) |" in compare.compare(before, after, "A", "B")


def test_compare_reports_each_services_cost_per_thousand_tasks_and_peak_memory():
    before = {"dag-500": [rep(1, usage_run={"state": {"cpu_seconds": 2.0, "mem_max_mib": 20.0}})]}
    after = {"dag-500": [rep(1, usage_run={"state": {"cpu_seconds": 1.0, "mem_max_mib": 30.0},
                                           "run-controller": {"cpu_seconds": 4.0, "mem_max_mib": 40.0}})]}
    markdown = compare.compare(before, after, "A", "B")
    assert "| state | 2.0 | 1.0 | -1.0 (-50%) | 20 | 30 | +10 (+50%) |" in markdown
    assert "| run-controller | — | 4.0 | — | — | 40 | — |" in markdown


def test_compare_reports_the_idle_footprint():
    idle_a = {"state": {"cpu_avg_m": 4.0, "mem_avg_mib": 8.0}}
    idle_b = {"state": {"cpu_avg_m": 2.0, "mem_avg_mib": 10.0}}
    before = {"smoke": [rep(1, usage_idle=idle_a)]}
    after = {"smoke": [rep(1, usage_idle=idle_b)]}
    markdown = compare.compare(before, after, "A", "B")
    assert "| state | 4 | 2 | -2 (-50%) | 8 | 10 | +2 (+25%) |" in markdown


def write_reps(root, scenario, reps):
    folder = root / scenario
    folder.mkdir(parents=True)
    for r in reps:
        (folder / f"rep{r['rep']}.json").write_text(json.dumps(r), encoding="utf-8")


def test_main_compares_two_result_folders(tmp_path, capsys):
    write_reps(tmp_path / "a", "dag-500", [rep(1, wall_s=20.0)])
    write_reps(tmp_path / "b", "dag-500", [rep(1, wall_s=10.0)])
    code = compare.main([str(tmp_path / "a"), str(tmp_path / "b"), "--labels", "A,B"])
    assert code == 0
    assert "| wall_s | 20.00 | 10.00 | -10.00 (-50%) |" in capsys.readouterr().out


def test_main_fails_when_a_folder_holds_no_results(tmp_path, capsys):
    write_reps(tmp_path / "a", "dag-500", [rep(1, wall_s=20.0)])
    (tmp_path / "b").mkdir()
    assert compare.main([str(tmp_path / "a"), str(tmp_path / "b")]) == 1
    assert capsys.readouterr().out == ""
