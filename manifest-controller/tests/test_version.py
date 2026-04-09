import pytest
from service.version import parse_version


def test_parse_version_v1():
    assert parse_version("manifest_v1.json") == "v1"


def test_parse_version_v3():
    assert parse_version("manifest_v3.json") == "v3"


def test_parse_version_large_number():
    assert parse_version("manifest_v42.json") == "v42"


def test_parse_version_invalid_no_version():
    with pytest.raises(ValueError, match="Invalid manifest filename"):
        parse_version("manifest.json")


def test_parse_version_invalid_wrong_prefix():
    with pytest.raises(ValueError, match="Invalid manifest filename"):
        parse_version("schema_v1.json")


def test_parse_version_invalid_uppercase():
    with pytest.raises(ValueError, match="Invalid manifest filename"):
        parse_version("manifest_V1.json")


def test_parse_version_invalid_empty():
    with pytest.raises(ValueError, match="Invalid manifest filename"):
        parse_version("")
