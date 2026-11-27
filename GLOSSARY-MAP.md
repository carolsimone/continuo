# continuo glossary map

continuo has one glossary for the words its components share and one per component for the words only that component uses. A component's glossary exists once its first term is settled; until then its row points at a file that is not there yet.

| Context | Glossary | Covers |
|---|---|---|
| Shared | [`GLOSSARY.md`](GLOSSARY.md) | Words more than one component uses: node, project, data contract, schedule, release, release check, run, task, attempt, heal trigger, round, fix |
| run-controller | [`run-controller/GLOSSARY.md`](run-controller/GLOSSARY.md) | Run lifecycle, tasks, attempts, schedules, write leases |
| catalog | [`catalog/GLOSSARY.md`](catalog/GLOSSARY.md) | The topology graph, code versions, contracts, exports |
| execution-controller | [`execution-controller/GLOSSARY.md`](execution-controller/GLOSSARY.md) | Kubernetes Jobs for attempts and release legs |
| release-controller | [`release-controller/GLOSSARY.md`](release-controller/GLOSSARY.md) | Candidate releases, the release check, promotion |
| topology-controller | [`topology-controller/GLOSSARY.md`](topology-controller/GLOSSARY.md) | Parsing releases into a topology and contracts |
| remediation | [`remediation/GLOSSARY.md`](remediation/GLOSSARY.md) | Classifying a rejected release's failures |
| agent-remediation | [`agent-remediation/GLOSSARY.md`](agent-remediation/GLOSSARY.md) | Remediations: tries, clusters, steps, drafts, revisions, handover reasons |
| agent-chat | [`agent-chat/GLOSSARY.md`](agent-chat/GLOSSARY.md) | The chat and its tool calls |
| dead-letter-controller | [`dead-letter-controller/GLOSSARY.md`](dead-letter-controller/GLOSSARY.md) | Dead letters and redrives |
| egress-proxy | [`egress-proxy/GLOSSARY.md`](egress-proxy/GLOSSARY.md) | Outbound traffic allowlist |

A term used by two or more components goes in the shared glossary, never in two component glossaries.

The ui and the `continuo` command line have no glossary of their own: they show the domain in the words of the glossaries above.
