#!/usr/bin/env python3
"""Assert the chart's rendered NetworkPolicy set permits the connections the
platform depends on and refuses the ones it must not make.

Ingress: the chart default-denies ingress and re-opens exactly the edges the
service graph needs. A service that starts calling another without its allow
rule renders the platform "up" but silently unreachable on that path, so
REQUIRED_EDGES must be admitted; CLOSED_EDGES, edges no service needs, must
not be. While the render holds egress-proxy, every source its ingress rules
admit (sampled as below) must be agent-chat or agent-remediation of this
release, in its namespace, on the proxy port.

Egress: what each pod may reach is derived from facts the render holds
independently of its NetworkPolicy documents, and the policies are then
checked against it in both directions: every expected connection in a probe
set is allowed, and every address, pod and port a rule admits is one the facts
allow. A rule is sampled at the edges of each ipBlock, at the edges of its
exceptions and at the probe addresses inside it, and at the probe pods its
selectors match, which include, for every pod or namespace selector of every
policy in the render, a pod made to satisfy it.
  - agent-chat and agent-remediation, when the render holds the
    `<fullname>-agents-egress` policy, may reach cluster DNS (any pod, or a
    link-local node cache, on port 53); every pod in the release namespace on
    any port; an external Postgres or Redis on its port (the shared
    ConfigMap's POSTGRES_PORT and REDIS_PORT, when the render holds no bundled
    one), in the private ranges or as a pod of any namespace; the pods of
    another namespace on the port of each address there they connect to
    without the proxy; the direct destinations given with --direct; and an
    external Postgres or Redis whose host (the shared ConfigMap's
    POSTGRES_HOST or REDIS_HOST) is an IP address outside the private ranges,
    on its port; nothing else. Without that policy their egress must be open: no egress
    policy may select them. A render that holds egress-proxy always holds
    that policy.
  - egress-proxy, when the render holds its Deployment, may reach cluster DNS,
    public addresses on every port its allowlist (the `<fullname>-egress-proxy`
    ConfigMap) names, and private addresses and pods in any namespace only on
    the ports of entries with allowPrivate; never loopback, link-local
    (except DNS on port 53) or a cloud metadata address.
  - no egress policy selects any other workload: their egress stays open.
The addresses an agent connects to without the proxy are the http(s) URL and
host:port values of its env and of the shared ConfigMap's *_GRPC_ADDR and
S3_ENDPOINT_URL that its NO_PROXY covers by Go's rules, or all of them when it
has no proxy variable. Each must be one the policy admits: an IP address, a
name with no dot or ending in .svc or .svc.cluster.local, or a release
Service's <name>.<namespace>. A host <name>.<namespace>.svc puts that
namespace and port in the facts above. An address whose host is an IP address
(other than loopback, or the unspecified address of a listen address) must be
one its NO_PROXY covers, because egress-proxy never admits an IP address.
A pod is never matched by an ipBlock here, so a rule reaches pods only through
its pod and namespace selectors.

Usage: assert-netpol-reachability.py <rendered-manifest.yaml> [release-name]
           [--direct CIDR=PORT[,PORT...]]...

--direct names a destination the agents reach without the proxy, as the
values that produced the render set it (an egress.directDestinations entry,
or the IP address of a URL the agents call, as a /32 or /128 on its port).

Extend REQUIRED_EDGES whenever a service begins reaching another over the
network. Each edge is (source service, destination service, port, why): the
destination must admit ingress from the source on that port, and a source
whose egress is restricted must be allowed to send it.
"""
import argparse
import importlib.util
import ipaddress
import logging
import os
import sys

import yaml

logger = logging.getLogger("assert-netpol-reachability")


def load_env_checker():
    """assert-agent-egress-env.py, beside this script: Go's NO_PROXY rules
    (NoProxy) and how an agent reads the host and port of a value (address)."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "assert-agent-egress-env.py")
    spec = importlib.util.spec_from_file_location("assert_agent_egress_env", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ENV_CHECKER = load_env_checker()

AGENTS = ("agent-chat", "agent-remediation")
PROXY = "egress-proxy"
PROXY_PORT = 3128
INSTANCE = "app.kubernetes.io/instance"
NAME = "app.kubernetes.io/name"
NAMESPACE_NAME = "kubernetes.io/metadata.name"

REQUIRED_EDGES = [
    ("agent-remediation", "release-controller", 8088,
     "shadow-verify lane: submit/poll shadow releases, read the failing release's image tags"),
    ("release-controller", "agent-remediation", 50054,
     "retry-remediation: ListProposals before starting a new remediation round"),
    ("ui", "release-controller", 8088,
     "operator dashboard reads release detail over HTTP"),
    ("ui", "agent-remediation", 50054,
     "operator dashboard reads remediation proposals over gRPC"),
    ("ui", "catalog", 50052,
     "operator dashboard reads the topology and run projections over gRPC"),
    ("agent-chat", "dead-letter-controller", 50055,
     "continuo dlq list/show/redrive read and redrive dead letters over gRPC"),
]

# Edges that exist only while the render holds egress-proxy.
PROXY_EDGES = [
    ("agent-chat", PROXY, PROXY_PORT, "LLM calls go through the proxy"),
    ("agent-remediation", PROXY, PROXY_PORT, "LLM, GitHub and external S3 calls go through the proxy"),
]

CLOSED_EDGES = [
    ("catalog", "run-controller", 50051,
     "catalog calls no run-controller RPC: run-controller fails a stalled run itself"),
]

# While the render holds egress-proxy, no service but the two agents may use it.
PROXY_CLOSED_EDGES = [
    (source, PROXY, PROXY_PORT, "only agent-chat and agent-remediation use the proxy")
    for source in ("ui", "run-controller", "catalog", "execution-controller", "release-controller",
                   "remediation", "dead-letter-controller", "topology-controller")
] + [
    ("agent-chat", PROXY, 8093, "the agents reach the proxy on its proxy port only"),
]

# Pods of the release the agents reach directly. Only the agent's egress is
# checked here: the destination's ingress rules are other policies' concern.
REQUIRED_AGENT_EGRESS = [
    ("agent-chat", "run-controller", 50051, "the continuo CLI reads run state"),
    ("agent-chat", "catalog", 50052, "the continuo CLI reads the topology"),
    ("agent-chat", "postgresql", 5432, "chat threads"),
    ("agent-chat", "redis", 6379, "the shared rate limiter"),
    ("agent-remediation", "catalog", 50052, "graph context"),
    ("agent-remediation", "postgresql", 5432, "proposals"),
    ("agent-remediation", "redis", 6379, "the remediation stream and the LLM cache"),
    ("agent-remediation", "minio", 9000, "evidence and code bundles"),
]

TCP, UDP = "TCP", "UDP"

# Address classes the expectations are written in.
LINK_LOCAL = [ipaddress.ip_network(n) for n in ("169.254.0.0/16", "fe80::/10")]
# Where a node-local DNS cache, or a cloud resolver, answers on port 53.
DNS_LINK_LOCAL = [ipaddress.ip_network("169.254.0.0/16")]
LOOPBACK = [ipaddress.ip_network(n) for n in ("127.0.0.0/8", "::1/128")]
METADATA = [ipaddress.ip_network(n) for n in (
    "100.100.100.200/32", "168.63.129.16/32", "fd00:ec2::/32", "fd20:ce::254/128")]
PRIVATE = [ipaddress.ip_network(n) for n in (
    "10.0.0.0/8", "100.64.0.0/10", "172.16.0.0/12", "192.168.0.0/16", "fc00::/7")]
# Neither public nor private: egress-proxy refuses them itself, and whether
# its NetworkPolicy admits them is left open.
UNROUTED = [ipaddress.ip_network(n) for n in (
    "0.0.0.0/8", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24",
    "224.0.0.0/4", "240.0.0.0/4",
    "::/96", "::ffff:0:0/96", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64", "2001:db8::/32",
    "fe00::/9", "fec0::/10", "ff00::/8")]

PUBLIC_PROBES = ["140.82.112.5", "8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"]
PRIVATE_PROBES = ["10.1.2.3", "172.16.0.1", "192.168.1.1", "100.64.0.1", "fd00::1"]
NEVER_PROBES = ["127.0.0.1", "::1", "169.254.169.254", "169.254.20.10", "fe80::1",
                "100.100.100.200", "168.63.129.16", "fd00:ec2::254", "fd20:ce::254"]
NODE_LOCAL_DNS = "169.254.20.10"
PROBE_PORTS = [(53, UDP), (53, TCP), (22, TCP), (80, TCP), (443, TCP), (PROXY_PORT, TCP), (5432, TCP),
               (6379, TCP), (8000, TCP), (8080, TCP), (8443, TCP), (9000, TCP), (50051, TCP)]

DNS_NAMESPACE = "kube-system"
DNS_POD_LABELS = {"k8s-app": "kube-dns"}
OTHER_NAMESPACE = "tools"


class Pod:
    def __init__(self, namespace, labels, name, namespace_labels=None):
        self.namespace = namespace
        self.labels = labels
        self.name = name
        self.namespace_labels = {**(namespace_labels or {}), NAMESPACE_NAME: namespace}

    def __str__(self):
        return self.name


class Address:
    def __init__(self, address):
        self.ip = ipaddress.ip_address(address)

    def __str__(self):
        return str(self.ip)


def in_any(ip, networks):
    return any(ip.version == n.version and ip in n for n in networks)


def selector_matches(selector, labels):
    """True if a Kubernetes label selector matches the given labels.

    An empty selector ({}) matches everything; a None selector matches nothing.
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


def satisfying_labels(selector):
    """Labels a selector matches: its matchLabels, the first value of each In
    expression and a value for each Exists key."""
    labels = dict(selector.get("matchLabels") or {})
    for expr in selector.get("matchExpressions") or []:
        if expr.get("operator") == "In" and expr.get("values"):
            labels.setdefault(expr["key"], expr["values"][0])
        elif expr.get("operator") == "Exists":
            labels.setdefault(expr["key"], "x")
    return labels


def policy_types(spec):
    types = spec.get("policyTypes")
    if types:
        return types
    return ["Ingress"] + (["Egress"] if "egress" in spec else [])


def selecting(policies, pod, kind):
    """The specs of the policies of `kind` (Ingress or Egress) selecting a pod."""
    return [p["spec"] for p in policies
            if p["metadata"].get("namespace", "default") == pod.namespace
            and kind in policy_types(p["spec"])
            and selector_matches(p["spec"].get("podSelector") or {}, pod.labels)]


def rule_ports(rule):
    """The (first, last, protocol) port ranges a rule admits, or None for every
    port of every protocol. An entry without a port admits every port of its
    protocol."""
    ports = rule.get("ports")
    if not ports:
        return None
    out = []
    for entry in ports:
        protocol = entry.get("protocol", TCP)
        if "port" not in entry:
            out.append((1, 65535, protocol))
            continue
        first = entry["port"]
        if not isinstance(first, int):
            raise SystemExit(f"FAIL: a NetworkPolicy port {first!r} is not a number; named ports are not modelled")
        out.append((first, entry.get("endPort", first), protocol))
    return out


def port_admitted(rule, port, protocol):
    ranges = rule_ports(rule)
    return ranges is None or any(lo <= port <= hi and proto == protocol for lo, hi, proto in ranges)


def peer_admits(peer, target, policy_namespace):
    if "ipBlock" in peer:
        if not isinstance(target, Address):
            return False
        block = peer["ipBlock"]
        net = ipaddress.ip_network(block["cidr"], strict=False)
        if target.ip.version != net.version or target.ip not in net:
            return False
        return not in_any(target.ip, [ipaddress.ip_network(e, strict=False) for e in block.get("except") or []])
    if not isinstance(target, Pod):
        return False
    namespaces = peer.get("namespaceSelector")
    pods = peer.get("podSelector")
    pods = {} if pods is None else pods
    if namespaces is None:
        in_namespace = target.namespace == policy_namespace
    else:
        in_namespace = selector_matches(namespaces, target.namespace_labels)
    return in_namespace and selector_matches(pods, target.labels)


def egress_allowed(policies, source, target, port, protocol):
    specs = selecting(policies, source, "Egress")
    if not specs:
        return True  # no egress policy selects the pod: egress is open
    for spec in specs:
        for rule in spec.get("egress") or []:
            peers = rule.get("to")
            if port_admitted(rule, port, protocol) and (
                    not peers or any(peer_admits(p, target, source.namespace) for p in peers)):
                return True
    return False


def ingress_allowed(policies, source, dest, port, protocol=TCP):
    specs = selecting(policies, dest, "Ingress")
    if not specs:
        return True
    for spec in specs:
        for rule in spec.get("ingress") or []:
            peers = rule.get("from")
            if port_admitted(rule, port, protocol) and (
                    not peers or any(peer_admits(p, source, dest.namespace) for p in peers)):
                return True
    return False


class Facts:
    """What the render says each pod may reach, read from everything in it but
    its NetworkPolicy documents."""

    def __init__(self, docs, instance, directs):
        self.instance = instance
        self.policies = [d for d in docs if d.get("kind") == "NetworkPolicy"]
        self.namespace = next((p["metadata"].get("namespace") for p in self.policies), None) or "default"
        self.workloads = {}
        for d in docs:
            if d.get("kind") in ("Deployment", "StatefulSet", "Job"):
                labels = ((d["spec"].get("template") or {}).get("metadata") or {}).get("labels") or {}
                name = labels.get(NAME, d["metadata"]["name"])
                self.workloads[name] = labels
        self.services = {d["metadata"]["name"] for d in docs if d.get("kind") == "Service"}
        self.proxy = any(d.get("kind") == "Deployment" and d["metadata"]["name"] == PROXY for d in docs)
        self.allowlist = None
        for d in docs:
            if d.get("kind") == "ConfigMap" and d["metadata"]["name"].endswith("-" + PROXY):
                self.allowlist = yaml.safe_load(d["data"]["allowlist.yaml"])["entries"]
        entries = self.allowlist or []
        self.public_ports = {int(e["port"]) for e in entries}
        self.private_ports = {int(e["port"]) for e in entries if e.get("allowPrivate")}
        # A render that holds egress-proxy runs with egress on, so its agents
        # must be locked whether or not the policy that locks them rendered.
        self.agents_policy = any(p["metadata"]["name"].endswith("-agents-egress") for p in self.policies)
        self.agents_locked = self.agents_policy or self.proxy
        self.shared = next((d.get("data") or {} for d in docs
                            if d.get("kind") == "ConfigMap" and "METRICS_PORT" in (d.get("data") or {})), {})
        self.metrics_port = int(self.shared.get("METRICS_PORT") or 0)
        self.datastore_ports = set()
        self.directs = list(directs)
        for name, key, host_key in (("postgresql", "POSTGRES_PORT", "POSTGRES_HOST"),
                                    ("redis", "REDIS_PORT", "REDIS_HOST")):
            if name not in self.workloads:
                if key not in self.shared:
                    raise SystemExit(f"FAIL: the render holds no {name} workload and its shared ConfigMap "
                                     f"(the one with METRICS_PORT) has no {key}")
                port = int(self.shared[key])
                self.datastore_ports.add(port)
                ip = ENV_CHECKER.ip_literal(str(self.shared.get(host_key) or "").strip("[]"))
                if ip is not None and not ip.is_loopback and not in_any(ip, PRIVATE):
                    self.directs.append((ipaddress.ip_network(ip), frozenset({port})))
        self.agent_env = {}
        for d in docs:
            if d.get("kind") == "Deployment" and d["metadata"]["name"] in AGENTS:
                container = d["spec"]["template"]["spec"]["containers"][0]
                self.agent_env[d["metadata"]["name"]] = {e["name"]: e.get("value") for e in container.get("env", [])}
        self.proxied_ips = []
        self.unproxied = self.unproxied_addresses()
        self.namespace_ports = {}
        for _agent, _key, _value, host, port in self.unproxied:
            kind, where = self.place(host)
            if kind == "pod" and where != self.namespace:
                self.namespace_ports.setdefault(where, set()).add(port)
        self.ports = compute_probe_ports(self)

    def unproxied_addresses(self):
        """The addresses each agent is given and connects to without the proxy,
        as (agent, key, value, host, port): the http(s) URL and host:port values
        of its env, over the shared ConfigMap's *_GRPC_ADDR and S3_ENDPOINT_URL,
        that its NO_PROXY covers, or all of them when it has no proxy variable."""
        shared = {k: v for k, v in self.shared.items() if k.endswith("_GRPC_ADDR") or k == "S3_ENDPOINT_URL"}
        out = []
        for agent, env in sorted(self.agent_env.items()):
            proxied = bool(env.get("HTTP_PROXY") or env.get("HTTPS_PROXY"))
            no_proxy = ENV_CHECKER.NoProxy(env.get("NO_PROXY") or "")
            values = {**shared, **{k: v for k, v in env.items() if v and k != "NO_PROXY"}}
            for key, value in sorted(values.items()):
                target = ENV_CHECKER.address(str(value))
                if not target:
                    continue
                host, port = target[0].lower().rstrip("."), target[1]
                if proxied and not no_proxy.covers(host, port):
                    ip = ENV_CHECKER.ip_literal(host)
                    if ip is not None and not (ip.is_loopback or ip.is_unspecified):
                        self.proxied_ips.append((agent, key, value))
                    continue
                out.append((agent, key, value, host, int(port)))
        return out

    def place(self, host):
        """Where a host an agent connects to without the proxy is: ("loopback",
        None); ("ip", address); ("pod", namespace) for a name cluster DNS gives a
        Service (no dot, a release Service's <name>.<namespace>, or a name
        ending in <namespace>.svc or <namespace>.svc.cluster.local); or ("name",
        None) for any other name, which no NetworkPolicy rule can admit."""
        if host == "localhost":
            return "loopback", None
        try:
            ip = ipaddress.ip_address(host.strip("[]"))
        except ValueError:
            ip = None
        if ip is not None:
            return ("loopback", None) if ip.is_loopback else ("ip", ip)
        if "." not in host:
            return "pod", self.namespace
        labels = (host[:-len(".cluster.local")] if host.endswith(".cluster.local") else host).split(".")
        if len(labels) >= 3 and labels[-1] == "svc":
            return "pod", labels[-2]
        if len(labels) == 2 and labels[0] in self.services and labels[1] == self.namespace:
            return "pod", self.namespace
        return "name", None

    def pod(self, service):
        labels = self.workloads.get(service) or {INSTANCE: self.instance, NAME: service}
        return Pod(self.namespace, labels, service)

    def expected(self, source, target, port, protocol):
        """True when `source` may reach `target`, False when it must not, None
        when either is acceptable."""
        dns = port == 53
        if source in AGENTS:
            if not self.agents_locked:
                return True
            if isinstance(target, Pod):
                if target.namespace == self.namespace or dns:
                    return True
                return protocol == TCP and (port in self.datastore_ports
                                            or port in self.namespace_ports.get(target.namespace, ()))
            ip = target.ip
            if dns and in_any(ip, DNS_LINK_LOCAL):
                return True
            if protocol != TCP:
                return False
            if port in self.datastore_ports and in_any(ip, PRIVATE) and not in_any(ip, METADATA):
                return True
            return any(ip.version == net.version and ip in net and port in ports for net, ports in self.directs)
        if source == PROXY:
            if isinstance(target, Pod):
                return dns or (protocol == TCP and port in self.private_ports)
            ip = target.ip
            if dns and in_any(ip, DNS_LINK_LOCAL):
                return True
            if protocol != TCP:
                return False
            if in_any(ip, LOOPBACK + LINK_LOCAL + METADATA):
                return False
            if in_any(ip, PRIVATE):
                return port in self.private_ports
            if in_any(ip, UNROUTED):
                return None
            return port in self.public_ports
        return True

    def expected_proxy_ingress(self, source, port, protocol):
        """Whether egress-proxy may admit `source` on `port`: agent-chat or
        agent-remediation of this release, in its namespace, on the proxy port.
        The metrics port is the metrics scrape rule's concern."""
        if port == self.metrics_port:
            return None
        return (isinstance(source, Pod) and source.namespace == self.namespace
                and source.labels.get(INSTANCE) == self.instance and source.labels.get(NAME) in AGENTS
                and port == PROXY_PORT and protocol == TCP)


def compute_probe_ports(facts):
    extra = set(facts.public_ports) | facts.datastore_ports
    for _, ports in facts.directs:
        extra |= set(ports)
    for ports in facts.namespace_ports.values():
        extra |= ports
    ports = set(PROBE_PORTS)
    for p in extra:
        for q in (p - 1, p, p + 1):
            if 1 <= q <= 65535:
                ports.add((q, TCP))
    return sorted(ports)


def probe_addresses(facts):
    out = PUBLIC_PROBES + PRIVATE_PROBES + NEVER_PROBES
    for net, _ in facts.directs:
        out += edge_addresses(net)
    return list(dict.fromkeys(str(a) for a in out))


def edge_addresses(net):
    """The first and last address of a network and the ones just outside it."""
    out = [net.network_address, net.broadcast_address]
    top = (1 << net.max_prefixlen) - 1
    if int(net.network_address) > 0:
        out.append(net.network_address - 1)
    if int(net.broadcast_address) < top:
        out.append(net.broadcast_address + 1)
    return out


def probe_pods(facts):
    """Every workload of the render, the release pods the agents use, a dbt
    Job pod, cluster DNS, a pod in another namespace and one in each namespace
    the agents connect to without the proxy, and, for every pod or namespace
    selector of every policy, a pod that satisfies it."""
    pods = [facts.pod(name) for name in sorted(set(facts.workloads) | {"postgresql", "redis", "minio"})]
    pods.append(Pod(facts.namespace, {"app": "dbt-job"}, "a dbt Job pod"))
    pods.append(Pod(DNS_NAMESPACE, DNS_POD_LABELS, "cluster DNS"))
    pods.append(Pod(OTHER_NAMESPACE, {"app": "other"}, f"a pod in namespace {OTHER_NAMESPACE}"))
    for ns in sorted(facts.namespace_ports):
        pods.append(Pod(ns, {"app": "other"}, f"a pod in namespace {ns}"))
    seen = set()
    for policy in facts.policies:
        spec = policy["spec"]
        for rule in (spec.get("egress") or []) + (spec.get("ingress") or []):
            for peer in (rule.get("to") or []) + (rule.get("from") or []):
                if "ipBlock" in peer:
                    continue
                pod = satisfying_pod(peer, policy["metadata"].get("namespace", facts.namespace))
                key = (pod.namespace, tuple(sorted(pod.labels.items())), tuple(sorted(pod.namespace_labels.items())))
                if key not in seen:
                    seen.add(key)
                    pods.append(pod)
    return pods


def satisfying_pod(peer, policy_namespace):
    """A pod a pod or namespace selector peer matches: in the namespace its
    namespaceSelector names (kubernetes.io/metadata.name, from matchLabels or
    an In expression), in another namespace when it names none, or in the
    policy's namespace when it has no namespaceSelector."""
    namespaces = peer.get("namespaceSelector")
    labels = satisfying_labels(peer.get("podSelector") or {})
    if namespaces is None:
        namespace, namespace_labels = policy_namespace, {}
    else:
        namespace_labels = satisfying_labels(namespaces)
        namespace = namespace_labels.get(NAMESPACE_NAME) or OTHER_NAMESPACE
    shown = ",".join(f"{k}={v}" for k, v in sorted(labels.items())) or "any labels"
    return Pod(namespace, labels, f"a pod ({shown}) in namespace {namespace}", namespace_labels)


def describe(target, port, protocol):
    return f"{target}:{port}" + ("" if protocol == TCP else f"/{protocol}")


def rule_samples(rule, facts, pods, peers_key):
    """The (target, port, protocol) points a rule admits that the checks look
    at: the edges of each ipBlock and of its exceptions and the probe addresses
    inside it, and the probe pods its selectors match, on each port the rule
    names (both ends of a range), or on every probe port when it names none.
    `peers_key` is "to" for an egress rule and "from" for an ingress one."""
    ranges = rule_ports(rule)
    if ranges is None:
        ports = facts.ports
    else:
        ports = []
        for lo, hi, proto in ranges:
            if (lo, hi) == (1, 65535):
                ports += [(p, q) for p, q in facts.ports if q == proto]
            else:
                ports += [(lo, proto), (hi, proto)]
    peers = rule.get(peers_key) or [{"ipBlock": {"cidr": "0.0.0.0/0"}}, {"ipBlock": {"cidr": "::/0"}},
                                    {"namespaceSelector": {}}]
    targets = []
    for peer in peers:
        if "ipBlock" in peer:
            block = peer["ipBlock"]
            net = ipaddress.ip_network(block["cidr"], strict=False)
            excepts = [ipaddress.ip_network(e, strict=False) for e in block.get("except") or []]
            candidates = edge_addresses(net)
            for ex in excepts:
                candidates += edge_addresses(ex)
            candidates += [ipaddress.ip_address(a) for a in probe_addresses(facts)]
            for ip in dict.fromkeys(candidates):
                if ip.version == net.version and ip in net and not in_any(ip, excepts):
                    targets.append(Address(str(ip)))
        else:
            targets += [p for p in pods if peer_admits(peer, p, facts.namespace)]
    return [(t, port, proto) for t in targets for port, proto in ports]


def main(argv):
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("manifest")
    parser.add_argument("release", nargs="?", default="continuo")
    parser.add_argument("--direct", action="append", default=[], metavar="CIDR=PORT[,PORT...]")
    args = parser.parse_args(argv[1:])

    directs = []
    for value in args.direct:
        cidr, _, ports = value.rpartition("=")
        directs.append((ipaddress.ip_network(cidr), frozenset(int(p) for p in ports.split(","))))

    with open(args.manifest) as handle:
        docs = [d for d in yaml.safe_load_all(handle) if d]
    if not any(d.get("kind") == "NetworkPolicy" for d in docs):
        logger.error("FAIL: %s rendered no NetworkPolicy documents — networkPolicy.enabled must be true "
                     "for this render.", args.manifest)
        return 1
    facts = Facts(docs, args.release, directs)
    policies = facts.policies

    failures = []
    checks = {"edges": 0, "agent": 0, "external": 0}

    def check(kind, source, target, port, protocol, got, want, why):
        checks[kind] += 1
        if want is not None and got != want:
            failures.append(f"egress {source} -> {describe(target, port, protocol)} is "
                            f"{'allowed' if got else 'refused'}, want {'allowed' if want else 'refused'}  ({why})")

    if facts.proxy and facts.allowlist is None:
        failures.append(f"the render holds the {PROXY} Deployment but no *-{PROXY} ConfigMap with its allowlist")
    if facts.proxy and not facts.agents_policy:
        failures.append(f"the render holds {PROXY} but no *-agents-egress policy: the agents could go around "
                        "the proxy")
    if not facts.proxy:
        for p in policies:
            labels = (p["spec"].get("podSelector") or {}).get("matchLabels") or {}
            if labels.get(NAME) == PROXY:
                failures.append(f"policy {p['metadata']['name']} selects {PROXY}, which this render does not hold")

    pods = probe_pods(facts)
    addresses = [Address(a) for a in probe_addresses(facts)]

    # Ingress edges, and the source side of each when its egress is restricted.
    edges = REQUIRED_EDGES + (PROXY_EDGES if facts.proxy else [])
    closed = CLOSED_EDGES + (PROXY_CLOSED_EDGES if facts.proxy else [])
    for source, dest, port, why in edges:
        checks["edges"] += 1
        if not ingress_allowed(policies, facts.pod(source), facts.pod(dest), port):
            failures.append(f"ingress {source} -> {dest}:{port}  ({why})")
        if not egress_allowed(policies, facts.pod(source), facts.pod(dest), port, TCP):
            failures.append(f"egress {source} -> {dest}:{port}  ({why})")
    for source, dest, port, why in closed:
        checks["edges"] += 1
        if ingress_allowed(policies, facts.pod(source), facts.pod(dest), port):
            failures.append(f"ingress {source} -> {dest}:{port} is admitted, want refused  ({why})")
    if facts.proxy:
        proxy_pod = facts.pod(PROXY)
        outsiders = [Pod(OTHER_NAMESPACE, {INSTANCE: facts.instance, NAME: agent},
                         f"a pod labelled as {agent} in namespace {OTHER_NAMESPACE}") for agent in AGENTS]
        for outsider in outsiders:
            checks["edges"] += 1
            if ingress_allowed(policies, outsider, proxy_pod, PROXY_PORT):
                failures.append(f"ingress {outsider} -> {PROXY}:{PROXY_PORT} is admitted, want refused  "
                                "(only agent-chat and agent-remediation of this release use the proxy)")
        # Reverse: every source and port an ingress rule of the proxy admits
        # must be one of the two agents on the proxy port.
        for spec in selecting(policies, proxy_pod, "Ingress"):
            for rule in spec.get("ingress") or []:
                for source, port, protocol in rule_samples(rule, facts, pods + outsiders, "from"):
                    checks["edges"] += 1
                    if facts.expected_proxy_ingress(source, port, protocol) is False:
                        failures.append(f"ingress {source} -> {describe(PROXY, port, protocol)} is admitted, "
                                        "want refused  (only agent-chat and agent-remediation of this release "
                                        f"use the proxy, on port {PROXY_PORT})")

    # Every workload but the agents and the proxy keeps open egress, and the
    # agents keep it while the render holds no agents-egress policy.
    restricted = set(AGENTS if facts.agents_locked else ()) | ({PROXY} if facts.proxy else set())
    for name in sorted(facts.workloads):
        if name not in restricted and selecting(policies, facts.pod(name), "Egress"):
            failures.append(f"an egress policy selects {name}, whose egress must stay open")

    sources = [s for s in AGENTS if facts.agents_locked] + ([PROXY] if facts.proxy else [])
    for source in sources:
        src = facts.pod(source)
        if source in AGENTS:
            for agent, dest, port, why in REQUIRED_AGENT_EGRESS:
                if agent == source:
                    check("agent", source, dest, port, TCP,
                          egress_allowed(policies, src, facts.pod(dest), port, TCP), True, why)
        dns = Pod(DNS_NAMESPACE, DNS_POD_LABELS, "cluster DNS")
        for protocol in (UDP, TCP):
            check("external", source, dns, 53, protocol,
                  egress_allowed(policies, src, dns, 53, protocol), True, "name resolution")
        node_dns = Address(NODE_LOCAL_DNS)
        check("external", source, node_dns, 53, UDP,
              egress_allowed(policies, src, node_dns, 53, UDP), True, "name resolution through a node-local cache")
        # Forward: the probe set, against the facts.
        for target in pods + addresses:
            for port, protocol in facts.ports:
                want = facts.expected(source, target, port, protocol)
                got = egress_allowed(policies, src, target, port, protocol)
                check("external", source, target, port, protocol, got, want, why_for(facts, source, target, port))
        # Reverse: every point a rule admits must be one the facts allow.
        for spec in selecting(policies, src, "Egress"):
            for rule in spec.get("egress") or []:
                for target, port, protocol in rule_samples(rule, facts, pods, "to"):
                    want = facts.expected(source, target, port, protocol)
                    check("external", source, target, port, protocol, True, want,
                          why_for(facts, source, target, port))
    for agent, key, value in facts.proxied_ips:
        failures.append(f"{agent}: {key}={value} names an IP address but NO_PROXY does not cover it, so the call "
                        "goes to egress-proxy, which never admits an IP address")
    if facts.agents_locked:
        # Every address an agent connects to without the proxy must be one its
        # egress policy admits, or that call is dropped without a trace.
        for agent, key, value, host, port in facts.unproxied:
            kind, where = facts.place(host)
            checks["agent"] += 1
            if kind == "loopback":
                continue
            if kind == "name":
                failures.append(f"{agent}: {key}={value} is reached without the proxy, but {host} is neither an "
                                "in-cluster name nor an IP address, so no NetworkPolicy rule admits it")
                continue
            target = Address(str(where)) if kind == "ip" else Pod(where, {"app": host.split(".")[0]}, host)
            if not egress_allowed(policies, facts.pod(agent), target, port, TCP):
                failures.append(f"{agent}: {key}={value} is reached without the proxy, but the agents' egress "
                                f"policy drops {describe(target, port, TCP)}")
    else:
        for agent in AGENTS:
            if selecting(policies, facts.pod(agent), "Egress"):
                failures.append(f"an egress policy selects {agent} although the render holds no "
                                "*-agents-egress policy")

    if failures:
        unique = list(dict.fromkeys(failures))
        shown = unique[:60]
        more = f"\n  ... and {len(unique) - len(shown)} more" if len(unique) > len(shown) else ""
        logger.error("FAIL: the rendered NetworkPolicy set gets these wrong:\n%s%s\n\n"
                     "Fix the rules in deploy/continuo/templates/networkpolicy.yaml.",
                     "\n".join(f"  {f}" for f in shown), more)
        return 1

    mode = ", ".join([
        f"{PROXY} present (public ports {sorted(facts.public_ports)}, private ports {sorted(facts.private_ports)})"
        if facts.proxy else f"no {PROXY}",
        "agents locked" if facts.agents_locked else "agents' egress open",
        f"external datastore ports {sorted(facts.datastore_ports)}" if facts.datastore_ports else "bundled datastores",
        f"{len(facts.directs)} direct destinations",
        ("other namespaces " + ", ".join(f"{ns}:{sorted(p)}" for ns, p in sorted(facts.namespace_ports.items())))
        if facts.namespace_ports else "no other namespace",
    ])
    logger.info("NetworkPolicy reachability OK: %d edges, %d agent egress paths, %d external checks (%s).",
                checks["edges"], checks["agent"], checks["external"], mode)
    return 0


def why_for(facts, source, target, port):
    if source in AGENTS:
        if isinstance(target, Pod):
            return ("the agents reach every pod of the release namespace" if target.namespace == facts.namespace
                    else "the agents reach another namespace only for DNS, an external datastore's port and "
                         "the addresses there they reach without the proxy")
        return ("outside the cluster the agents reach only link-local DNS, an external Postgres or Redis "
                "on its port and their direct destinations")
    if isinstance(target, Pod):
        return "the proxy reaches pods only for DNS and on the ports of allowPrivate entries"
    if in_any(target.ip, LOOPBACK + LINK_LOCAL + METADATA):
        return "the proxy never reaches loopback, link-local or a cloud metadata address, except DNS on port 53"
    if in_any(target.ip, PRIVATE):
        return "the proxy reaches private addresses only on the ports of allowPrivate entries"
    return "the proxy reaches public addresses only on its allowlisted ports"


if __name__ == "__main__":
    sys.exit(main(sys.argv))
