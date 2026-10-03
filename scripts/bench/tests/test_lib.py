import os
import subprocess
from pathlib import Path

BENCH = Path(__file__).resolve().parents[1]


def in_quiet_window(now, windows=None):
    env = dict(os.environ)
    env.pop("BENCH_QUIET_WINDOWS_UTC", None)
    if windows is not None:
        env["BENCH_QUIET_WINDOWS_UTC"] = windows
    return subprocess.run(["bash", "-c", f'. "{BENCH}/lib.sh"; bench_in_quiet_window {now}'], env=env).returncode == 0


def test_default_window_surrounds_a_23_00_utc_daily_run():
    assert in_quiet_window("2300")
    assert not in_quiet_window("2214")
    assert not in_quiet_window("2345")


def test_windows_are_configurable_and_may_cross_midnight():
    windows = "0415-0545,2330-0030"
    assert in_quiet_window("0415", windows)
    assert in_quiet_window("0010", windows)
    assert in_quiet_window("2345", windows)
    assert not in_quiet_window("0600", windows)
    assert not in_quiet_window("2300", windows)
