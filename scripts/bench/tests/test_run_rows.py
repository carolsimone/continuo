import pytest

import run_rows

RUN = "7f3e1a2b-0000-4000-8000-000000000001"


def test_each_query_reads_only_the_run():
    assert f"run_id = '{RUN}'" in run_rows.query("deployments", RUN)
    assert "FROM deployments" in run_rows.query("deployments", RUN)
    assert "mode = 'production'" in run_rows.query("deployments", RUN)
    assert f"run_id = '{RUN}'" in run_rows.query("tasks", RUN)
    assert "FROM tasks" in run_rows.query("tasks", RUN)


def test_a_run_id_that_is_not_a_uuid_is_refused():
    with pytest.raises(ValueError):
        run_rows.query("tasks", "x'; DROP TABLE tasks; --")


def test_main_prints_the_query_or_refuses(capsys):
    assert run_rows.main(["tasks", RUN]) == 0
    assert "FROM tasks" in capsys.readouterr().out
    assert run_rows.main(["tasks", "not-a-uuid"]) == 2
    assert run_rows.main(["runs", RUN]) == 2
