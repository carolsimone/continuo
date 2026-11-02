"""Guard: credentials are read only through config.secrets.read_secret.

A direct os.environ / os.getenv read of a credential variable would ignore the
<NAME>_FILE form, so a deployment that mounts the credential as a file would
run without it. Every non-test module except config/secrets.py (which
implements the read) must go through read_secret.
"""
import ast
from pathlib import Path

SERVICE_DIR = Path(__file__).parent.parent
SECRETS_MODULE = Path("config") / "secrets.py"
SKIPPED_DIRS = {".venv", "tests", "proto", "__pycache__"}

CREDENTIAL_VARS = (
    "POSTGRES_PASSWORD",
    "REDIS_PASSWORD",
    "NEO4J_PASSWORD",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "LLM_API_KEY",
    "GITHUB_TOKEN",
    "GITHUB_APP_PRIVATE_KEY",
    "AUTH_OIDC_CLIENT_SECRET",
)


def _is_os_environ(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "environ"
        and isinstance(node.value, ast.Name)
        and node.value.id == "os"
    )


def _string_literal(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _credential_reads(source: str) -> list[str]:
    """Return the credential variables a source string reads from the environment."""
    offenders: list[str] = []
    for node in ast.walk(ast.parse(source)):
        name = None
        if isinstance(node, ast.Call) and node.args:
            func = node.func
            is_get = (
                isinstance(func, ast.Attribute)
                and func.attr == "get"
                and _is_os_environ(func.value)
            )
            is_getenv = (
                isinstance(func, ast.Attribute)
                and func.attr == "getenv"
                and isinstance(func.value, ast.Name)
                and func.value.id == "os"
            )
            if is_get or is_getenv:
                name = _string_literal(node.args[0])
        elif isinstance(node, ast.Subscript) and _is_os_environ(node.value):
            name = _string_literal(node.slice)
        if name in CREDENTIAL_VARS:
            offenders.append(name)
    return offenders


def _service_modules():
    for path in sorted(SERVICE_DIR.rglob("*.py")):
        relative = path.relative_to(SERVICE_DIR)
        if SKIPPED_DIRS & set(relative.parts) or relative == SECRETS_MODULE:
            continue
        yield path, relative


def test_credentials_are_read_only_through_read_secret():
    inspected = 0
    offenders = []
    for path, relative in _service_modules():
        inspected += 1
        for name in _credential_reads(path.read_text()):
            offenders.append(f"{relative}: reads {name} from the environment")

    assert inspected, "guard inspected no modules — the discovery is broken"
    assert not offenders, (
        "credentials must be read through config.secrets.read_secret so the "
        "<NAME>_FILE form works:\n" + "\n".join(offenders)
    )


def test_guard_catches_a_violation():
    """The detector flags each read shape it is meant to reject."""
    assert _credential_reads('import os\nos.environ.get("REDIS_PASSWORD", "")') == ["REDIS_PASSWORD"]
    assert _credential_reads('import os\nos.getenv("AWS_ACCESS_KEY_ID")') == ["AWS_ACCESS_KEY_ID"]
    assert _credential_reads('import os\nos.environ["LLM_API_KEY"]') == ["LLM_API_KEY"]
    # A variable outside the credential list, and the read_secret form, are fine.
    assert _credential_reads('import os\nos.environ.get("REDIS_URL", "")') == []
    assert _credential_reads('read_secret("REDIS_PASSWORD")') == []
    # A non-literal key cannot be judged and is not flagged.
    assert _credential_reads("import os\nos.environ.get(name)") == []

