#!/usr/bin/env python3
"""The egress-proxy allowlist the chart renders follows the settings that name
what the agents reach: the LLM (llm.baseUrl, else the provider's public API),
the GitHub API (agent-remediation's GITHUB_BASE_URL) and the S3 endpoint
(s3.endpointUrl, when MinIO is off), plus egress.allowedDomains and
egress.privateDomains. A URL whose host is an IP address, or any name the proxy
would refuse, or whose port is outside 1-65535, is left out of the list: the
proxy admits host names only. When nothing is left, no egress-proxy object
renders, because the proxy refuses to start on an empty allowlist.

The script renders the chart with each settings combination in CASES and
compares the allowlist in the egress-proxy ConfigMap with the expected one. It
renders every entry in REFUSED and requires the render to fail, naming the key
and the entry. egress.enabled=false must render no egress-proxy object, and a
release whose stored values hold no egress block must render with the proxy on.

Usage: assert-egress-allowlist.py <chart-dir> <kube-version>
"""
import json
import logging
import subprocess
import sys

import yaml

logger = logging.getLogger("assert-egress-allowlist")

RELEASE = "continuo"
CONFIGMAP = f"{RELEASE}-egress-proxy"

GITHUB = {("api.github.com", 443, False), ("codeload.github.com", 443, False)}
ANTHROPIC = {("api.anthropic.com", 443, False)}
S3_EXTERNAL = [
    "--set", "minio.enabled=false",
    "--set", "s3.endpointUrl=https://s3.eu-central-1.amazonaws.com",
    "--set", "s3.bucket=continuo", "--set", "s3.region=eu-central-1",
    "--set", "s3.accessKeyId=probe", "--set", "s3.secretKey=probe",
]
S3_EXTERNAL_IP = [
    "--set", "minio.enabled=false",
    "--set", "s3.endpointUrl=http://10.0.0.9:9000",
    "--set", "s3.bucket=continuo", "--set", "s3.region=eu-central-1",
    "--set", "s3.accessKeyId=probe", "--set", "s3.secretKey=probe",
]


def llm_url(url: str) -> list:
    return ["--set", "llm.provider=openai-compatible", "--set", f"llm.baseUrl={url}"]


def s3_url(url: str) -> list:
    return [
        "--set", "minio.enabled=false", "--set", f"s3.endpointUrl={url}",
        "--set", "s3.bucket=continuo", "--set", "s3.region=eu-central-1",
        "--set", "s3.accessKeyId=probe", "--set", "s3.secretKey=probe",
    ]


LONG_HOST = ".".join(["a" * 60] * 4) + ".com"  # 247 characters: a name, but codeload.<it> is too long


def agent_remediation_github(url: str) -> list:
    """The services override that points agent-remediation at a GitHub API URL."""
    services = [{"name": "agent-remediation", "enabled": True, "env": {"GITHUB_BASE_URL": url}}]
    return ["--set-json", "services=" + json.dumps(services)]


def values(key: str, entries: list) -> list:
    return ["--set-json", f"egress.{key}=" + json.dumps(entries)]


# (name, helm args, expected allowlist as {(host, port, allowPrivate)})
CASES = [
    ("defaults", [], ANTHROPIC | GITHUB),
    ("openai", ["--set", "llm.provider=openai"], {("api.openai.com", 443, False)} | GITHUB),
    ("self-hosted model, explicit port",
     ["--set", "llm.provider=openai-compatible", "--set", "llm.baseUrl=http://gpu-box.internal:8000"],
     {("gpu-box.internal", 8000, True)} | GITHUB),
    ("self-hosted model, http without a port",
     ["--set", "llm.provider=openai-compatible", "--set", "llm.baseUrl=http://gpu-box.internal"],
     {("gpu-box.internal", 80, True)} | GITHUB),
    ("gateway in front of the provider, https without a port",
     ["--set", "llm.baseUrl=https://llm-gateway.corp.example"],
     {("llm-gateway.corp.example", 443, True)} | GITHUB),
    ("gateway with an upper-case host and a path-free root",
     ["--set", "llm.baseUrl=https://LLM-Gateway.Corp.Example:8443/"],
     {("llm-gateway.corp.example", 8443, True)} | GITHUB),
    ("model at an IPv4 address",
     ["--set", "llm.provider=openai-compatible", "--set", "llm.baseUrl=http://10.0.0.7:8000"],
     GITHUB),
    ("model at an IPv6 address",
     ["--set", "llm.provider=openai-compatible", "--set", "llm.baseUrl=http://[fd00::7]:8000"],
     GITHUB),
    ("model at an IPv6 address without a port",
     ["--set", "llm.provider=openai-compatible", "--set", "llm.baseUrl=http://[fd00::7]"],
     GITHUB),
    ("model at a legacy numeric IPv4 address",
     ["--set", "llm.provider=openai-compatible", "--set", "llm.baseUrl=http://2130706433:8000"],
     GITHUB),
    ("external S3", S3_EXTERNAL, ANTHROPIC | GITHUB | {("s3.eu-central-1.amazonaws.com", 443, True)}),
    ("external S3 at an IP address", S3_EXTERNAL_IP, ANTHROPIC | GITHUB),
    ("extra domains",
     ["--set-json", 'egress.allowedDomains=["api.example.com","*.example.org:8443"]',
      "--set-json", 'egress.privateDomains=["vault.internal:8200","API.example.com"]'],
     ANTHROPIC | GITHUB | {("api.example.com", 443, True), ("*.example.org", 8443, False),
                           ("vault.internal", 8200, True)}),
    ("upper-case and trailing-dot entries are lower-cased after validation",
     values("allowedDomains", ["API.Example.COM", "Host.Example.Com.:8080", "*.Example.NET"]),
     ANTHROPIC | GITHUB | {("api.example.com", 443, False), ("host.example.com", 8080, False),
                           ("*.example.net", 443, False)}),
    ("a name that merely contains digits and hyphens",
     values("allowedDomains", ["1.example.com", "a-b.example.com:65535", "xn--bcher-kva.example", "x" * 63 + ".example"]),
     ANTHROPIC | GITHUB | {("1.example.com", 443, False), ("a-b.example.com", 65535, False),
                           ("xn--bcher-kva.example", 443, False), ("x" * 63 + ".example", 443, False)}),
    ("a listed entry that duplicates a generated one allows private addresses if either does",
     values("privateDomains", ["api.anthropic.com"]),
     {("api.anthropic.com", 443, True)} | GITHUB),
    ("GitHub Enterprise", agent_remediation_github("https://ghe.corp.example/api/v3"),
     ANTHROPIC | {("ghe.corp.example", 443, True), ("codeload.ghe.corp.example", 443, True)}),
    ("GitHub Enterprise with a port", agent_remediation_github("http://ghe.corp.example:8080/api/v3"),
     ANTHROPIC | {("ghe.corp.example", 8080, True), ("codeload.ghe.corp.example", 8080, True)}),
    ("GitHub Enterprise at an IPv4 address", agent_remediation_github("https://10.1.2.3/api/v3"), ANTHROPIC),
    ("GitHub Enterprise at an IPv6 address", agent_remediation_github("https://[fd00::7]:8443/api/v3"), ANTHROPIC),
    ("agent-remediation without GITHUB_BASE_URL",
     ["--set-json", 'services=[{"name":"agent-remediation","enabled":true}]'], ANTHROPIC | GITHUB),
    ("no agent-remediation entry",
     ["--set-json", 'services=[{"name":"state","enabled":true}]'], ANTHROPIC | GITHUB),
    # Ports of URLs are read in base 10, as written.
    ("URL port with a leading zero", llm_url("http://gpu:08080"), {("gpu", 8080, True)} | GITHUB),
    ("URL port 0 is dropped", llm_url("http://gpu:0"), GITHUB),
    ("URL port above 65535 is dropped", llm_url("http://gpu:65536"), GITHUB),
    ("URL port far above 65535 is dropped", llm_url("http://gpu:99999999999999999999"), GITHUB),
    ("GitHub Enterprise port with a leading zero",
     agent_remediation_github("http://ghe.corp.example:08080/api/v3"),
     ANTHROPIC | {("ghe.corp.example", 8080, True), ("codeload.ghe.corp.example", 8080, True)}),
    # URL hosts the proxy would refuse are dropped, like IP addresses.
    ("scheme-less GITHUB_BASE_URL", agent_remediation_github("ghe.corp.example/api/v3"), ANTHROPIC),
    ("GITHUB_BASE_URL without a host", agent_remediation_github("https://"), ANTHROPIC),
    ("underscore host in llm.baseUrl", llm_url("http://vllm_x:8000"), GITHUB),
    ("underscore host in GITHUB_BASE_URL", agent_remediation_github("https://gh_e.example"), ANTHROPIC),
    ("underscore host in s3.endpointUrl", s3_url("http://my_minio:9000"), ANTHROPIC | GITHUB),
    ("wildcard host in llm.baseUrl", llm_url("https://*.example.com"), GITHUB),
    ("single-label URL host with a trailing dot", llm_url("http://gpu-box.:8000"), {("gpu-box", 8000, True)} | GITHUB),
    ("GitHub Enterprise host whose codeload name is too long",
     agent_remediation_github(f"https://{LONG_HOST}/api/v3"), ANTHROPIC | {(LONG_HOST, 443, True)}),
    # Nothing left to allow: no proxy object renders.
    ("every URL is an IP address and no domain is listed",
     [*llm_url("http://10.0.0.7:8000"), *agent_remediation_github("http://10.0.0.8:8080")], None),
    ("every URL is unusable and no domain is listed",
     [*llm_url("http://vllm_x:8000"), *agent_remediation_github("https://")], None),
    ("every URL is an IP address, one domain is listed",
     [*llm_url("http://10.0.0.7:8000"), *agent_remediation_github("http://10.0.0.8:8080"),
      *values("allowedDomains", ["api.example.com"])], {("api.example.com", 443, False)}),
    ("release without an egress block", ["--set", "egress=null"], ANTHROPIC | GITHUB),
    ("release without llm.baseUrl", ["--set", "llm.baseUrl=null"], ANTHROPIC | GITHUB),
]

# Entries each of which must fail the render, written to egress.allowedDomains
# and checked as written. The error must name the key and the entry.
BAD_ENTRIES = [
    ("a scheme", "https://api.example.com"),
    ("a scheme and a port", "https://api.example.com:443"),
    ("a path", "api.example.com/v1"),
    ("a trailing slash", "api.example.com/"),
    ("a query", "api.example.com?x=1"),
    ("a fragment", "api.example.com#x"),
    ("userinfo", "user@api.example.com"),
    ("userinfo and a password", "user:pw@api.example.com"),
    ("an empty string", ""),
    ("a whitespace-only string", "   "),
    ("a tab", "\t"),
    ("a space inside", "api example.com"),
    ("a trailing space", "api.example.com "),
    ("a leading space", " api.example.com"),
    ("a newline", "api.example.com\n"),
    ("an IPv4 address", "10.0.0.5"),
    ("an IPv4 address with a port", "10.0.0.5:8080"),
    ("a bracketed IPv6 address", "[::1]"),
    ("a bracketed IPv6 address with a port", "[fd00::7]:8000"),
    ("a bare IPv6 address", "::1"),
    ("a bare IPv6 address, long", "fd00::7"),
    ("a bare wildcard", "*"),
    ("a bare wildcard with a port", "*:443"),
    ("a top-level wildcard", "*.com"),
    ("a wildcard with one label after it and a port", "*.com:443"),
    ("an inner wildcard", "api.*.example.com"),
    ("a trailing wildcard", "api.example.*"),
    ("a wildcard inside a label", "a*.example.com"),
    ("a double wildcard", "*.*.example.com"),
    ("a label starting with a hyphen", "-a.example.com"),
    ("a label ending with a hyphen", "a-.example.com"),
    ("an underscore", "a_b.example.com"),
    ("an empty label", "a..example.com"),
    ("a leading dot", ".example.com"),
    ("a label of 64 characters", "x" * 64 + ".example.com"),
    ("a name over 253 characters", ".".join(["a" * 60] * 5) + ".com"),
    ("a non-ASCII label", "bücher.example"),
    ("an all-digit last label, 127.1", "127.1"),
    ("an all-digit last label, a decimal address", "2130706433"),
    ("an all-digit last label under a name", "host.123"),
    ("a hex last label", "0x7f.0.0.1"),
    ("a bare hex last label", "0x7f000001"),
    ("a hex last label in upper case", "0X7F.1"),
    ("an empty hex last label", "host.0x"),
    ("an empty port", "api.example.com:"),
    ("a non-numeric port", "api.example.com:https"),
    ("port 0", "api.example.com:0"),
    ("a port above 65535", "api.example.com:65536"),
    ("a port far above 65535", "api.example.com:99999999999999999999"),
    ("a port with a leading zero", "api.example.com:0443"),
    ("a port with two leading zeros", "api.example.com:00443"),
    ("a port with a leading zero that is octal-looking", "api.example.com:010"),
    ("a port with a leading zero that is no octal number", "api.example.com:09"),
    ("a negative port", "api.example.com:-1"),
    ("a signed port", "api.example.com:+443"),
    ("a port with a space", "api.example.com: 443"),
    ("a hex port", "api.example.com:0x1bb"),
    ("two colons", "api.example.com:443:443"),
    ("no host", ":443"),
    ("only a colon", ":"),
    ("a lone dot", "."),
]

# Entries that are not strings. The schema leaves an entry's type to the
# templates, which refuse it naming the key and the entry.
BAD_TYPES = [
    ("a number", 443),
    ("a boolean", True),
    ("a map", {"host": "api.example.com"}),
    ("a list", ["api.example.com"]),
    ("null", None),
]



def as_json(entry) -> str:
    """The entry as the chart's toJson prints it in an error message."""
    return json.dumps(entry, ensure_ascii=False, separators=(",", ":"))


REFUSED = []
for _label, _entry in BAD_ENTRIES:
    REFUSED.append((f"allowedDomains: {_label}", values("allowedDomains", [_entry]), "allowedDomains", as_json(_entry)))
# egress.privateDomains goes through the same validator; a sample of the bad
# entries is enough to show it is wired to it.
PRIVATE_SAMPLE = {
    "a port with a leading zero", "a port with a leading zero that is no octal number",
    "a scheme", "a path", "an empty string", "a whitespace-only string", "an IPv4 address",
    "a bare IPv6 address", "a bare wildcard", "an inner wildcard", "an all-digit last label, 127.1",
    "an empty port", "port 0", "two colons",
}
for _label, _entry in BAD_ENTRIES:
    if _label in PRIVATE_SAMPLE:
        REFUSED.append((f"privateDomains: {_label}", values("privateDomains", [_entry]), "privateDomains", as_json(_entry)))
for _label, _entry in BAD_TYPES:
    REFUSED.append((f"allowedDomains: {_label}", values("allowedDomains", [_entry]), "allowedDomains", as_json(_entry)))
    REFUSED.append((f"privateDomains: {_label}", values("privateDomains", [_entry]), "privateDomains", as_json(_entry)))
REFUSED.append(("one bad entry among good ones",
                values("allowedDomains", ["api.example.com", "https://x.example.com", "b.example.com"]),
                "allowedDomains", as_json("https://x.example.com")))
REFUSED.append(("metrics.port equal to the proxy port", ["--set", "metrics.port=3128"], "metrics.port", None))
REFUSED.append(("metrics.port equal to the health port", ["--set", "metrics.port=8093"], "metrics.port", None))
REFUSED.append(("a services entry named egress-proxy",
                ["--set-json", 'services=[{"name":"egress-proxy","enabled":true}]'], "egress-proxy", None))

# Refusals whose message must also say why.
WHY = [
    ("api.example.com:0443", "leading zero"),
    ("api.example.com:010", "leading zero"),
    ("api.example.com:09", "leading zero"),
    ("api.example.com:0", "outside 1-65535"),
]


def helm(chart: str, kube_version: str, args: list) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["helm", "template", RELEASE, chart, "--kube-version", kube_version, *args],
        capture_output=True, text=True, check=False,
    )


def docs(text: str) -> list:
    return [d for d in yaml.safe_load_all(text) if d]


def allowlist(documents: list):
    """The allowlist the egress-proxy ConfigMap carries, or None without one."""
    for d in documents:
        if d.get("kind") == "ConfigMap" and d["metadata"]["name"] == CONFIGMAP:
            entries = yaml.safe_load(d["data"]["allowlist.yaml"])["entries"]
            return {(e["host"], e["port"], e["allowPrivate"]) for e in entries}, entries
    return None


def proxy_objects(documents: list) -> list:
    return [
        f'{d["kind"]}/{d["metadata"]["name"]}'
        for d in documents
        if d["metadata"].get("labels", {}).get("app.kubernetes.io/name") == "egress-proxy"
        or d["metadata"]["name"] in ("egress-proxy", CONFIGMAP)
    ]


def check_cases(chart: str, kube_version: str, failures: list) -> None:
    for name, args, want in CASES:
        result = helm(chart, kube_version, args)
        if result.returncode != 0:
            failures.append(f"{name}: render failed: {result.stderr.strip()}")
            continue
        if want is None:
            if objects := proxy_objects(docs(result.stdout)):
                failures.append(f"{name}: an empty allowlist must render no egress-proxy object, got {objects}")
            continue
        found = allowlist(docs(result.stdout))
        if found is None:
            failures.append(f"{name}: no {CONFIGMAP} ConfigMap rendered")
            continue
        got, entries = found
        if got != want:
            failures.append(f"{name}: allowlist is {sorted(got)}, want {sorted(want)}")
        if len(entries) != len(got):
            failures.append(f"{name}: allowlist repeats a host and port: {entries}")
        if entries != sorted(entries, key=lambda e: (e["host"], e["port"])):
            failures.append(f"{name}: allowlist is not sorted by host and port: {entries}")
        if any(any(c.isupper() for c in e["host"]) or e["host"].endswith(".") for e in entries):
            failures.append(f"{name}: allowlist holds a host that is not lower-case or ends in a dot: {entries}")


def check_refused(chart: str, kube_version: str, failures: list) -> None:
    for name, args, key, entry in REFUSED:
        result = helm(chart, kube_version, args)
        if result.returncode == 0:
            failures.append(f"{name}: must fail the render")
        elif not result.stderr:
            failures.append(f"{name}: failed without an error message")
        elif key not in result.stderr:
            failures.append(f"{name}: the error does not name {key}: {result.stderr.strip()}")
        elif entry is not None and entry not in result.stderr:
            failures.append(f"{name}: the error does not name the entry {entry}: {result.stderr.strip()}")


def check_why(chart: str, kube_version: str, failures: list) -> None:
    for entry, why in WHY:
        result = helm(chart, kube_version, values("allowedDomains", [entry]))
        if why not in result.stderr:
            failures.append(f"{entry!r}: the error must say {why!r}: {result.stderr.strip()}")


def check_disabled(chart: str, kube_version: str, failures: list) -> None:
    off = helm(chart, kube_version, ["--set", "egress.enabled=false"])
    if off.returncode != 0:
        failures.append(f"egress.enabled=false: render failed: {off.stderr.strip()}")
    elif objects := proxy_objects(docs(off.stdout)):
        failures.append(f"egress.enabled=false still renders {objects}")
    # With the proxy off its ports are free: a metrics.port equal to them, and a
    # service named egress-proxy, render.
    for name, args in (
        ("metrics.port=3128", ["--set", "metrics.port=3128"]),
        ("metrics.port=8093", ["--set", "metrics.port=8093"]),
        ("a services entry named egress-proxy",
         ["--set-json", 'services=[{"name":"egress-proxy","enabled":true}]']),
    ):
        result = helm(chart, kube_version, ["--set", "egress.enabled=false", *args])
        if result.returncode != 0:
            failures.append(f"egress.enabled=false with {name}: render failed: {result.stderr.strip()}")
    # An empty allowlist renders no proxy, so its ports are free as well.
    empty = [*llm_url("http://10.0.0.7:8000"), *agent_remediation_github("http://10.0.0.8:8080")]
    for name, args in (
        ("metrics.port=3128", ["--set", "metrics.port=3128"]),
        ("metrics.port=8093", ["--set", "metrics.port=8093"]),
    ):
        result = helm(chart, kube_version, [*empty, *args])
        if result.returncode != 0:
            failures.append(f"an empty allowlist with {name}: render failed: {result.stderr.strip()}")
        elif objects := proxy_objects(docs(result.stdout)):
            failures.append(f"an empty allowlist with {name} still renders {objects}")
    named = ["--set-json", "services=" + json.dumps([
        {"name": "egress-proxy", "enabled": True},
        {"name": "agent-remediation", "enabled": True, "env": {"GITHUB_BASE_URL": "http://10.0.0.8:8080"}},
    ])]
    result = helm(chart, kube_version, [*llm_url("http://10.0.0.7:8000"), *named])
    if result.returncode != 0:
        failures.append(f"an empty allowlist with a services entry named egress-proxy: render failed: {result.stderr.strip()}")
    # One listed domain brings the proxy back, with that single entry.
    result = helm(chart, kube_version, [*empty, *values("allowedDomains", ["api.example.com"])])
    found = allowlist(docs(result.stdout)) if result.returncode == 0 else None
    if found is None or found[1] != [{"host": "api.example.com", "port": 443, "allowPrivate": False}]:
        failures.append(f"an empty generated allowlist plus one listed domain must render that single entry: {found}")
    on = helm(chart, kube_version, [])
    kinds = sorted(o.split("/")[0] for o in proxy_objects(docs(on.stdout))) if on.returncode == 0 else []
    if kinds != ["ConfigMap", "Deployment", "Service"]:
        failures.append(f"the default render holds egress-proxy objects {kinds}, want ConfigMap, Deployment and Service")


def main(argv: list) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
    if len(argv) != 3:
        logger.error("usage: assert-egress-allowlist.py <chart-dir> <kube-version>")
        return 2
    chart, kube_version = argv[1], argv[2]
    failures: list = []
    check_cases(chart, kube_version, failures)
    check_refused(chart, kube_version, failures)
    check_why(chart, kube_version, failures)
    check_disabled(chart, kube_version, failures)
    for f in failures:
        logger.error("FAIL: %s", f)
    if not failures:
        logger.info("egress allowlist: %d cases, %d refusals OK", len(CASES), len(REFUSED))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
