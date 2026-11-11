#!/usr/bin/env python3
"""Exercise assert-metrics.py's discovery of Go and Python metrics services."""
import importlib.util
import pathlib
import tempfile
import unittest

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("assert_metrics", HERE / "assert-metrics.py")
assert_metrics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(assert_metrics)


def write(root, relative, text):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


class AssertMetricsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        write(self.root, "svc-go/main.go", "package main\n")
        write(self.root, "svc-go/adapters/m.go",
              'import "github.com/carolsimone/continuo/pkg/metrics"\nvar x = Opts{Name: "continuo_go_total"}\n')
        write(self.root, "svc-py/main.py", "")
        write(self.root, "svc-py/adapters/metrics/m.py",
              'from prometheus_client import Counter\nX = Counter(\n    "continuo_py_total", "help")\n')
        write(self.root, "svc-py/tests/test_m.py",
              'from prometheus_client import Counter\nCounter("continuo_test_only_total", "h")\n')
        write(self.root, "svc-py/.venv/lib/prometheus_client/c.py", 'Counter("continuo_vendored_total", "h")\n')
        write(self.root, "svc-plain/main.py", "import logging\n")

    def tearDown(self):
        self.tmp.cleanup()

    def guide(self, *names):
        write(self.root, "deploy/OBSERVABILITY.md", "".join(f"| `{n}` | counter | | x |\n" for n in names))

    def test_a_python_service_that_imports_prometheus_client_serves_metrics(self):
        self.assertEqual(assert_metrics.serving_services(self.root), {"svc-go", "svc-py"})

    def test_python_series_are_read_from_non_test_sources_only(self):
        self.assertEqual(assert_metrics.registered_metrics(self.root), {"continuo_go_total", "continuo_py_total"})

    def test_a_python_series_missing_from_the_guide_fails(self):
        self.guide("continuo_go_total")
        problems = assert_metrics.check_documented_metrics(self.root)
        self.assertEqual(problems, ["deploy/OBSERVABILITY.md does not list ['continuo_py_total']"])


if __name__ == "__main__":
    unittest.main()
