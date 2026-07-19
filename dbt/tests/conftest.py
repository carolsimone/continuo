"""Make the s3-sidecar scripts importable regardless of container layout.

`test_compile_uploader.py` and `test_parse_cache_fetcher.py` do a bare
`import compile_uploader` / `import parse_cache_fetcher` / `import s3_common`.
Locally, pyproject.toml's `pythonpath = [".", "s3-sidecar"]` resolves against
dbt/ as pytest's rootdir, so `dbt/s3-sidecar` (the repo layout) is already on
sys.path and those imports work unassisted.

Inside the dbt-compile-and-load image (dbt/Dockerfile.upload), the same
scripts are copied flat to the image root — `/compile_uploader.py` etc. —
mirroring the production s3-sidecar image's own layout, which some tests
invoke directly as a deployed script via subprocess. There is no
`/app/s3-sidecar` directory in that image, so pyproject.toml's "s3-sidecar"
pythonpath entry resolves to nothing and the bare imports fail.

Check both candidate locations for the actual module file and prepend
whichever one is present, so collection succeeds under either layout without
weakening what the tests import.
"""
import sys
from pathlib import Path

_TESTS_DIR = Path(__file__).resolve().parent
_MARKER = "compile_uploader.py"

for _candidate in (
    _TESTS_DIR.parent / "s3-sidecar",  # repo layout: dbt/s3-sidecar
    Path("/"),  # dbt-compile-and-load image layout: scripts copied to /
):
    if (_candidate / _MARKER).is_file():
        _candidate_str = str(_candidate)
        if _candidate_str not in sys.path:
            sys.path.insert(0, _candidate_str)
        break
