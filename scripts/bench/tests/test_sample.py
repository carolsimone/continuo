import subprocess
import time
from pathlib import Path

import fake_env

BENCH = Path(__file__).resolve().parents[1]


def test_the_sampler_keeps_sampling_after_a_failed_reading(tmp_path):
    env = fake_env.make(tmp_path, [])
    env["FAKE_DOCKER_STATS_FAIL"] = "1"
    out = tmp_path / "samples.tsv"
    proc = subprocess.Popen(["bash", str(BENCH / "sample.sh"), "docker", str(out), "1", "state"], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(3)
        assert proc.poll() is None
    finally:
        proc.kill()
        proc.wait()
