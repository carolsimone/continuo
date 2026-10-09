import json
import os
import subprocess
from pathlib import Path

BENCH = Path(__file__).resolve().parents[1]

# Stands in for kubectl on a k8s target: answers the CLI calls preflight.sh makes
# through `kubectl exec deploy/agent-chat -- continuo ...` and, like the real
# kubectl, consumes its stdin when given -i.
FAKE_KUBECTL = """#!/bin/sh
case " $* " in *" -i "*) cat > /dev/null ;; esac
case "$*" in
  *"schedule list"*) echo '{"schedules":[{"schedule_name":"a"},{"schedule_name":"b"},{"schedule_name":"c"}]}' ;;
  *"schedule graph"*) echo '{"nodes":[]}' ;;
  *) echo "unexpected kubectl call: $*" >&2; exit 1 ;;
esac
"""


def test_preflight_fetches_the_graph_of_every_schedule(tmp_path):
    fakebin = tmp_path / "bin"
    fakebin.mkdir()
    kubectl = fakebin / "kubectl"
    kubectl.write_text(FAKE_KUBECTL)
    kubectl.chmod(0o755)
    out = tmp_path / "out"
    out.mkdir()
    (out / "restore.json").write_text(json.dumps({"release_id": "r", "topology": []}))
    env = dict(os.environ, PATH=f"{fakebin}:{os.environ['PATH']}", BENCH_TARGET="k8s", BENCH_KUBECONFIG="/dev/null")
    result = subprocess.run(["bash", str(BENCH / "preflight.sh"), str(out)], env=env,
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert sorted(p.name for p in (out / "graphs").iterdir()) == ["a.json", "b.json", "c.json"]
