#!/usr/bin/env python3
"""Assert the chart's rendered NetworkPolicy set permits the cross-service
edges the platform depends on.

The chart applies a default-deny ingress policy and re-opens exactly the edges
the service graph needs. A service that starts calling another without its
allow rule renders the platform "up" but silently unreachable on that path:
default-deny rejects the connection and the failure surfaces far from its
cause. This gate fails closed on a missing edge instead of letting it ship.

Usage: assert-netpol-reachability.py <rendered-manifest.yaml> [release-name]

Extend REQUIRED_EDGES whenever a service begins reaching another over the
network. Each edge is (source service, destination service, port, why): the
destination pod must admit ingress from the source pod on that port.
"""
import sys

import yaml

REQUIRED_EDGES = [
    ("agent-remediation", "release-controller", 8088,
     "shadow-verify lane: submit/poll shadow releases, read the failing release's image tags"),
    ("release-controller", "agent-remediation", 50054,
     "retry-remediation: ListProposals before starting a new remediation round"),
    ("ui", "release-controller", 8088,
     "operator dashboard reads release detail over HTTP"),
    ("ui", "agent-remediation", 50054,
     "operator dashboard reads remediation proposals over gRPC"),
    ("ui", "orchestrator", 50052,
     "operator dashboard reads the topology and run projections over gRPC"),
]


def pod_labels(instance, service):
    return {
        "app.kubernetes.io/instance": instance,
        "app.kubernetes.io/name": service,
    }


def selector_matches(selector, labels):
    """True if a Kubernetes label selector matches the given pod labels.

    An empty selector ({}) matches every pod; a None selector (e.g. a peer
    that selects by namespace or ipBlock rather than pod) matches nothing.
    """
    if selector is None:
        return False
    for key, value in (selector.get("matchLabels") or {}).items():
        if labels.get(key) != value:
            return False
    for expr in selector.get("matchExpressions") or []:
        key = expr.get("key")
        op = expr.get("operator")
        values = expr.get("values") or []
        present = key in labels
        if op == "In" and labels.get(key) not in values:
            return False
        if op == "NotIn" and labels.get(key) in values:
            return False
        if op == "Exists" and not present:
            return False
        if op == "DoesNotExist" and present:
            return False
    return True


def rule_admits_port(rule, port):
    ports = rule.get("ports")
    if not ports:
        return True  # no ports restriction means every port is allowed
    return any(entry.get("port") == port for entry in ports)


def rule_admits_source(rule, source_labels):
    peers = rule.get("from")
    if not peers:
        return True  # no `from` means every source is allowed
    return any(
        selector_matches(peer.get("podSelector"), source_labels) for peer in peers
    )


def edge_permitted(policies, instance, source, dest, port):
    dest_labels = pod_labels(instance, dest)
    source_labels = pod_labels(instance, source)
    for policy in policies:
        spec = policy.get("spec") or {}
        policy_types = spec.get("policyTypes") or []
        if policy_types and "Ingress" not in policy_types:
            continue
        if not selector_matches(spec.get("podSelector") or {}, dest_labels):
            continue
        for rule in spec.get("ingress") or []:
            if rule_admits_port(rule, port) and rule_admits_source(rule, source_labels):
                return True
    return False


def main():
    if len(sys.argv) < 2:
        sys.stderr.write("usage: assert-netpol-reachability.py <manifest.yaml> [release-name]\n")
        sys.exit(2)
    path = sys.argv[1]
    instance = sys.argv[2] if len(sys.argv) > 2 else "continuo"

    with open(path) as handle:
        policies = [
            doc for doc in yaml.safe_load_all(handle)
            if doc and doc.get("kind") == "NetworkPolicy"
        ]
    if not policies:
        sys.stderr.write(
            f"FAIL: {path} rendered no NetworkPolicy documents — "
            "networkPolicy.enabled must be true for this render.\n"
        )
        sys.exit(1)

    failures = [
        f"  {source} -> {dest}:{port}  ({why})"
        for source, dest, port, why in REQUIRED_EDGES
        if not edge_permitted(policies, instance, source, dest, port)
    ]
    if failures:
        sys.stderr.write(
            "FAIL: the rendered NetworkPolicy set does not permit these required edges:\n"
            + "\n".join(failures)
            + "\n\nThe default-deny policy rejects any edge not explicitly allowed. "
            "Add the missing ingress rule in deploy/continuo/templates/networkpolicy.yaml.\n"
        )
        sys.exit(1)

    print(f"NetworkPolicy reachability OK: {len(REQUIRED_EDGES)} required edges permitted.")


if __name__ == "__main__":
    main()
