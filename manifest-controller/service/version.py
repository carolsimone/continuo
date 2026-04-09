import re

_VERSION_RE = re.compile(r'^manifest_(v\d+)\.json$')


def parse_version(filename: str) -> str:
    """
    Extracts the version string from a manifest filename.
    Raises ValueError if the filename does not match manifest_v{N}.json.
    """
    m = _VERSION_RE.match(filename)
    if not m:
        raise ValueError(f"Invalid manifest filename: {filename!r}")
    return m.group(1)
