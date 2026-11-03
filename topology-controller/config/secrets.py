"""Credentials read from an environment variable or from a file.

A credential <NAME> is given either as the environment variable <NAME> or as
the path of a file named by <NAME>_FILE (a mounted Secret). Giving both is an
error. Every module reads a credential through read_secret, so the file form
works everywhere; tests/test_credential_reads_guard.py enforces it.
"""
import os

from redis.credentials import CredentialProvider


def read_secret(name: str) -> str:
    """Return the credential name, or "" when neither form is set.

    A file's content has one trailing newline ("\\n" or "\\r\\n") removed.
    Raises RuntimeError when both forms are set, or when the file cannot be
    read, is not UTF-8 text or is empty.
    """
    file_var = f"{name}_FILE"
    value = os.environ.get(name, "")
    path = os.environ.get(file_var, "")
    if value and path:
        raise RuntimeError(f"{name}: set {name} or {file_var}, not both")
    if not path:
        return value
    try:
        with open(path, encoding="utf-8", newline="") as f:
            content = f.read()
    except OSError as exc:
        raise RuntimeError(f"{file_var}: {exc}") from exc
    except UnicodeDecodeError:
        # The decode error would quote the offending byte of the credential.
        raise RuntimeError(f"{file_var}: {path} is not UTF-8 text") from None
    if content.endswith("\n"):
        content = content.removesuffix("\n").removesuffix("\r")
    if content == "":
        raise RuntimeError(f"{file_var}: {path} is empty")
    return content


class EnvOrFileCredentialProvider(CredentialProvider):
    """Redis credentials read from the environment or files on every call.

    redis-py asks its provider for credentials each time it opens a connection,
    so a password file that is rotated is picked up by the next connection
    without a restart. The username is not a credential: it is read from its
    environment variable only, with no <NAME>_FILE form.
    """

    def __init__(self, username_var: str = "REDIS_USERNAME", password_var: str = "REDIS_PASSWORD") -> None:
        self._username_var = username_var
        self._password_var = password_var

    def get_credentials(self) -> tuple[str] | tuple[str, str]:
        username = os.environ.get(self._username_var, "")
        password = read_secret(self._password_var)
        if username:
            return (username, password)
        return (password,)
