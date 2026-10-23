#!/usr/bin/env python3
"""agent-chat and agent-remediation send every HTTP and gRPC call through
egress-proxy except to the hosts NO_PROXY lists, and the proxy admits only the
hosts on its allowlist. In a render that holds the egress-proxy ConfigMap:
  - both agents name the proxy in HTTP_PROXY and HTTPS_PROXY;
  - NO_PROXY covers, by Go's matching rule, the host of every in-cluster
    address either agent is given (its own env, and the shared ConfigMap's
    *_GRPC_ADDR and S3_ENDPOINT_URL), unless the allowlist admits that host
    with private addresses, in which case the proxy carries the call;
  - NO_PROXY covers no host the allowlist admits, so an allowlisted call never
    goes around the proxy.
An address counts as in-cluster when the first label of its host names a
Service in the render.

In a render without the egress-proxy ConfigMap (egress.enabled=false, or an
allowlist with nothing in it) neither agent carries a proxy variable, in upper
or lower case.

Usage: assert-agent-egress-env.py <rendered-manifest.yaml>
"""
import ipaddress
import logging
import re
import sys
from urllib.parse import urlsplit

import yaml

logger = logging.getLogger("assert-agent-egress-env")

AGENTS = ("agent-chat", "agent-remediation")
PROXY = "http://egress-proxy:3128"
PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy")
HOST_PORT = re.compile(r"^([A-Za-z0-9.-]+):\d+$")


class NoProxy:
    """NO_PROXY parsed as Go reads it (golang.org/x/net/http/httpproxy): an
    entry with a "/" that parses as a CIDR matches the IP addresses in it; an
    IP address, with an optional port, matches that address; any other entry is
    a domain, optionally with a port, that matches the name itself and every
    name below it, except that a leading "." (or "*.") matches only the names
    below it. Matching ignores case; loopback addresses and "localhost" never
    use a proxy."""

    def __init__(self, value: str):
        self.everything = False
        self.networks = []
        self.ips = []
        self.domains = []
        for raw in value.split(","):
            entry = raw.strip().lower()
            if not entry:
                continue
            if entry == "*":
                self.everything = True
                continue
            if "/" in entry:
                try:
                    self.networks.append(ipaddress.ip_network(entry, strict=False))
                    continue
                except ValueError:
                    pass
            host, port = entry, ""
            m = re.match(r"^\[(.*)\]:(\d+)$", entry) or re.match(r"^([^:]*):(\d+)$", entry)
            if m:
                host, port = m.group(1), m.group(2)
            try:
                self.ips.append((ipaddress.ip_address(host.strip("[]")), port))
                continue
            except ValueError:
                pass
            if not host:
                continue
            if host.startswith("*."):
                host = host[1:]
            match_host = not host.startswith(".")
            self.domains.append(("." + host.lstrip("."), port, match_host))

    def covers(self, host: str, port: str = "") -> bool:
        host = host.lower()
        if self.everything or host == "localhost":
            return True
        try:
            ip = ipaddress.ip_address(host.strip("[]"))
        except ValueError:
            ip = None
        if ip is not None:
            if ip.is_loopback:
                return True
            if any(ip in n for n in self.networks):
                return True
            if any(ip == i and (not p or p == port) for i, p in self.ips):
                return True
        for suffix, p, match_host in self.domains:
            if host.endswith(suffix) or (match_host and host == suffix[1:]):
                if not p or p == port:
                    return True
        return False

    def meets_wildcard(self, domain: str) -> bool:
        """Whether some name strictly below `domain` is covered, so that an
        allowlist entry "*.<domain>" shares a host with NO_PROXY."""
        if self.everything:
            return True
        for suffix, _p, _match_host in self.domains:
            entry = suffix[1:]
            if entry == domain or entry.endswith("." + domain) or domain.endswith("." + entry):
                return True
        return False


def address_host(value: str):
    if value.startswith(("http://", "https://")):
        return urlsplit(value).hostname
    m = HOST_PORT.match(value)
    return m.group(1).lower() if m else None


def check_proxy_absent(name: str, env: dict, failures: list) -> None:
    for var in PROXY_VARS:
        if var in env:
            failures.append(f"{name}: sets {var}={env[var]!r}, but the render holds no egress-proxy to send it to")


def check_proxy_present(name: str, env: dict, shared: dict, services: set, allowlist: list, failures: list) -> None:
    for var in ("HTTP_PROXY", "HTTPS_PROXY"):
        if env.get(var) != PROXY:
            failures.append(f"{name}: {var} is {env.get(var)!r}, want {PROXY!r}")
    for var in ("http_proxy", "https_proxy", "no_proxy"):
        if var in env:
            failures.append(f"{name}: sets {var}, which would sit beside the chart's upper-case variable")
    if "NO_PROXY" not in env:
        failures.append(f"{name}: no NO_PROXY, so every in-cluster call would go to egress-proxy and be refused")
        return
    no_proxy = NoProxy(env.get("NO_PROXY") or "")
    private_hosts = {e["host"] for e in allowlist if e["allowPrivate"] and not e["host"].startswith("*.")}
    addresses = {k: v for k, v in env.items() if v and k not in ("NO_PROXY",)}
    addresses |= {k: v for k, v in shared.items() if k.endswith("_GRPC_ADDR") or k == "S3_ENDPOINT_URL"}
    for key, value in sorted(addresses.items()):
        host = address_host(str(value))
        if not host or host.split(".")[0] not in services:
            continue
        if not no_proxy.covers(host) and host not in private_hosts:
            failures.append(f"{name}: {key}={value} is in-cluster but NO_PROXY does not cover {host}; "
                            "the call would go to egress-proxy and be refused")
    for entry in allowlist:
        host = entry["host"]
        if host.startswith("*."):
            shared_name = no_proxy.meets_wildcard(host[2:])
        else:
            shared_name = no_proxy.covers(host)
        if shared_name:
            failures.append(f"{name}: NO_PROXY covers {host}, an allowlisted host, so its calls would "
                            "skip the proxy")


def main(argv: list) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    if len(argv) != 2:
        logger.error("usage: assert-agent-egress-env.py <rendered-manifest.yaml>")
        return 2
    with open(argv[1]) as f:
        docs = [d for d in yaml.safe_load_all(f) if d]
    services = {d["metadata"]["name"] for d in docs if d["kind"] == "Service"}
    shared = next((d.get("data", {}) for d in docs
                   if d["kind"] == "ConfigMap" and "METRICS_PORT" in d.get("data", {})), {})
    allowlist = None
    for d in docs:
        if d["kind"] == "ConfigMap" and d["metadata"]["name"].endswith("-egress-proxy"):
            allowlist = yaml.safe_load(d["data"]["allowlist.yaml"])["entries"]
    failures: list = []
    seen = set()
    for d in docs:
        if d["kind"] != "Deployment" or d["metadata"]["name"] not in AGENTS:
            continue
        name = d["metadata"]["name"]
        seen.add(name)
        container = d["spec"]["template"]["spec"]["containers"][0]
        entries = container.get("env", [])
        names = [e["name"] for e in entries]
        for var in sorted({n for n in names if names.count(n) > 1}):
            failures.append(f"{name}: sets {var} more than once")
        env = {e["name"]: e.get("value") for e in entries}
        if allowlist is None:
            check_proxy_absent(name, env, failures)
        else:
            check_proxy_present(name, env, shared, services, allowlist, failures)
    for missing in sorted(set(AGENTS) - seen):
        failures.append(f"{missing}: no Deployment rendered")
    for f in failures:
        logger.error("FAIL: %s", f)
    if not failures:
        if allowlist is None:
            logger.info("agent egress env: no egress-proxy in the render, and neither agent sets a proxy variable")
        else:
            logger.info("agent egress env: both agents use %s; NO_PROXY covers every in-cluster address "
                        "and no allowlisted host", PROXY)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
