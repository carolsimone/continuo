from __future__ import annotations

from domain.contract_vocabulary import NODE_TYPE_RUNTIME, NodeRuntime, NodeType

_BUILT_RUNTIMES = frozenset({NodeRuntime.DBT, NodeRuntime.PYTHON})


def is_dormant(node_type: NodeType, schedule: str) -> bool:
    """Whether a node of this type and schedule is dormant: a release builds it
    and no schedule tag puts it on a cron, so only a manual run executes it.

    A dbt test only validates, a table no node builds is not built, and a type
    the contract does not declare has no runtime, so none of them is dormant.
    pkg/domain/model.IsDormant is the same rule, and tests/fixtures/dormant_cases.json
    pins the two to each other.
    """
    return (
        schedule == ""
        and node_type != NodeType.DBT_TEST
        and NODE_TYPE_RUNTIME.get(node_type) in _BUILT_RUNTIMES
    )
