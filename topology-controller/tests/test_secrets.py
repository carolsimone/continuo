"""read_secret and the Redis credential provider read a credential from an
environment variable or from the file named by <NAME>_FILE."""
import pytest

from config.secrets import EnvOrFileCredentialProvider, read_secret


def test_read_secret_from_env(monkeypatch):
    monkeypatch.setenv("REDIS_PASSWORD", "pw")
    monkeypatch.delenv("REDIS_PASSWORD_FILE", raising=False)
    assert read_secret("REDIS_PASSWORD") == "pw"


def test_read_secret_unset_is_empty(monkeypatch):
    monkeypatch.delenv("REDIS_PASSWORD", raising=False)
    monkeypatch.delenv("REDIS_PASSWORD_FILE", raising=False)
    assert read_secret("REDIS_PASSWORD") == ""


@pytest.mark.parametrize("content, want", [
    (b"pw\n", "pw"),
    (b"pw\r\n", "pw"),
    (b"pw\n\n", "pw\n"),
    (b"p w'@:\\", "p w'@:\\"),
])
def test_read_secret_from_file_trims_one_newline(monkeypatch, tmp_path, content, want):
    path = tmp_path / "REDIS_PASSWORD"
    path.write_bytes(content)
    monkeypatch.delenv("REDIS_PASSWORD", raising=False)
    monkeypatch.setenv("REDIS_PASSWORD_FILE", str(path))
    assert read_secret("REDIS_PASSWORD") == want


def test_read_secret_both_set_raises(monkeypatch, tmp_path):
    path = tmp_path / "REDIS_PASSWORD"
    path.write_text("pw")
    monkeypatch.setenv("REDIS_PASSWORD", "pw")
    monkeypatch.setenv("REDIS_PASSWORD_FILE", str(path))
    with pytest.raises(RuntimeError) as exc_info:
        read_secret("REDIS_PASSWORD")
    assert str(exc_info.value) == "REDIS_PASSWORD: set REDIS_PASSWORD or REDIS_PASSWORD_FILE, not both"


@pytest.mark.parametrize("content", [b"", b"\n", b"\r\n"])
def test_read_secret_empty_file_raises(monkeypatch, tmp_path, content):
    path = tmp_path / "REDIS_PASSWORD"
    path.write_bytes(content)
    monkeypatch.delenv("REDIS_PASSWORD", raising=False)
    monkeypatch.setenv("REDIS_PASSWORD_FILE", str(path))
    with pytest.raises(RuntimeError) as exc_info:
        read_secret("REDIS_PASSWORD")
    assert str(exc_info.value) == f"REDIS_PASSWORD_FILE: {path} is empty"


def test_read_secret_missing_file_raises(monkeypatch, tmp_path):
    path = tmp_path / "absent"
    monkeypatch.delenv("REDIS_PASSWORD", raising=False)
    monkeypatch.setenv("REDIS_PASSWORD_FILE", str(path))
    with pytest.raises(RuntimeError) as exc_info:
        read_secret("REDIS_PASSWORD")
    assert str(exc_info.value).startswith("REDIS_PASSWORD_FILE: ")
    assert str(path) in str(exc_info.value)


def test_credential_provider_rereads_the_file(monkeypatch, tmp_path):
    path = tmp_path / "REDIS_PASSWORD"
    path.write_text("first\n")
    monkeypatch.delenv("REDIS_PASSWORD", raising=False)
    monkeypatch.delenv("REDIS_USERNAME", raising=False)
    monkeypatch.setenv("REDIS_PASSWORD_FILE", str(path))
    provider = EnvOrFileCredentialProvider()
    assert provider.get_credentials() == ("first",)

    path.write_text("second\n")
    assert provider.get_credentials() == ("second",)


def test_credential_provider_without_username_returns_password_only(monkeypatch):
    monkeypatch.delenv("REDIS_USERNAME", raising=False)
    monkeypatch.delenv("REDIS_PASSWORD_FILE", raising=False)
    monkeypatch.setenv("REDIS_PASSWORD", "pw")
    assert EnvOrFileCredentialProvider().get_credentials() == ("pw",)


def test_credential_provider_with_username_returns_both(monkeypatch):
    monkeypatch.delenv("REDIS_PASSWORD_FILE", raising=False)
    monkeypatch.setenv("REDIS_USERNAME", "svc")
    monkeypatch.setenv("REDIS_PASSWORD", "pw")
    assert EnvOrFileCredentialProvider().get_credentials() == ("svc", "pw")


def test_read_secret_non_utf8_file_names_the_variable(monkeypatch, tmp_path):
    path = tmp_path / "REDIS_PASSWORD"
    path.write_bytes(b"\xff\xfe pw")
    monkeypatch.delenv("REDIS_PASSWORD", raising=False)
    monkeypatch.setenv("REDIS_PASSWORD_FILE", str(path))
    with pytest.raises(RuntimeError) as exc_info:
        read_secret("REDIS_PASSWORD")
    assert str(exc_info.value).startswith("REDIS_PASSWORD_FILE: ")
    assert str(path) in str(exc_info.value)


def test_credential_provider_ignores_a_username_file(monkeypatch, tmp_path):
    path = tmp_path / "REDIS_USERNAME"
    path.write_text("svc\n")
    monkeypatch.delenv("REDIS_USERNAME", raising=False)
    monkeypatch.setenv("REDIS_USERNAME_FILE", str(path))
    monkeypatch.delenv("REDIS_PASSWORD_FILE", raising=False)
    monkeypatch.setenv("REDIS_PASSWORD", "pw")
    assert EnvOrFileCredentialProvider().get_credentials() == ("pw",)
