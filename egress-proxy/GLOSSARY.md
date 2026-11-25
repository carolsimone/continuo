# egress-proxy

egress-proxy is the one way out to the internet for the components that call outside services (agent-chat and agent-remediation): it lets a connection through only to a host and port the install allows.

## Language

**Allowlist**:
The hosts and ports an install lets agent-chat and agent-remediation reach, such as the model provider and GitHub. Every other destination is refused.
_Avoid_: whitelist, firewall rules

**Private address**:
An address inside a private network. A connection to one is refused unless its allowlist entry permits it; a loopback or cloud-metadata address is always refused.
_Avoid_: internal IP, local address
