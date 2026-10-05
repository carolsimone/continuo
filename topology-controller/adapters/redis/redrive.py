"""Routing of entries dead-letter-controller redrives onto a stream."""

REDRIVE_GROUP_FIELD = "redrive_group"
REDRIVEN_FROM_FIELD = "redriven_from"
_REDRIVE_FIELDS = {REDRIVE_GROUP_FIELD, REDRIVEN_FROM_FIELD}


def _text(value) -> str:
    return value.decode() if isinstance(value, bytes) else value


def route(fields: dict, group: str) -> tuple[bool, dict]:
    """Return (for_this_group, fields_without_redrive_fields). An entry that
    names another group in redrive_group is not for this group; every other
    entry is, with redrive_group and redriven_from removed."""
    keys = {_text(k): k for k in fields}
    target = keys.get(REDRIVE_GROUP_FIELD)
    if target is not None and _text(fields[target]) != group:
        return False, fields
    if not _REDRIVE_FIELDS & keys.keys():
        return True, fields
    return True, {k: v for k, v in fields.items() if _text(k) not in _REDRIVE_FIELDS}
