# continuo glossary map

continuo has one glossary for the words every service shares and one per service for the words only that service uses. A service's glossary exists once its first term is settled; until then its row points at a file that is not there yet.

| Context | Glossary | Covers |
|---|---|---|
| Shared | [`GLOSSARY.md`](GLOSSARY.md) | Words more than one service uses: release, live release, run, rerun, rebase |
| run-controller | [`run-controller/GLOSSARY.md`](run-controller/GLOSSARY.md) | Run lifecycle, tasks, attempts, schedules, write leases |
| catalog | [`catalog/GLOSSARY.md`](catalog/GLOSSARY.md) | The topology graph, code versions, contracts, exports |
| execution-controller | [`execution-controller/GLOSSARY.md`](execution-controller/GLOSSARY.md) | Kubernetes Jobs for attempts and release legs |
| release-controller | [`release-controller/GLOSSARY.md`](release-controller/GLOSSARY.md) | Candidate releases, validation, promotion |
| topology-controller | [`topology-controller/GLOSSARY.md`](topology-controller/GLOSSARY.md) | Parsing releases into a topology and contracts |
| remediation | [`remediation/GLOSSARY.md`](remediation/GLOSSARY.md) | Classifying a rejected release's failures |
| agent-remediation | [`agent-remediation/GLOSSARY.md`](agent-remediation/GLOSSARY.md) | Fix proposals: tries, rounds, clusters, steps, hand-overs |
| agent-chat | [`agent-chat/GLOSSARY.md`](agent-chat/GLOSSARY.md) | The chat and its tool calls |
| dead-letter-controller | [`dead-letter-controller/GLOSSARY.md`](dead-letter-controller/GLOSSARY.md) | Dead letters and redrives |
| egress-proxy | [`egress-proxy/GLOSSARY.md`](egress-proxy/GLOSSARY.md) | Outbound traffic allowlist |
| ui | [`ui/GLOSSARY.md`](ui/GLOSSARY.md) | The operator dashboard |
| cli | [`cli/GLOSSARY.md`](cli/GLOSSARY.md) | The `continuo` command line |

A term used by two or more services goes in the shared glossary, never in two service glossaries.
