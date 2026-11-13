from __future__ import annotations

from domain.contract_vocabulary import NODE_TYPE_RUNTIME, NodeRuntime, NodeType

_BUILT_RUNTIMES = frozenset({NodeRuntime.DBT, NodeRuntime.PYTHON})


def is_dormant(node_type: NodeType, schedule: str) -> bool:
    """Whether a node of this type and schedule is dormant: a node of a type a
    release builds (runtime dbt or python, other than a dbt test) with an empty
    schedule. It applies the rule pkg/domain/model.IsDormant states and
    explains, and tests/fixtures/dormant_cases.json pins the two to each other.
    """
    return (
        schedule == ""
        and node_type != NodeType.DBT_TEST
        and NODE_TYPE_RUNTIME.get(node_type) in _BUILT_RUNTIMES
    )
