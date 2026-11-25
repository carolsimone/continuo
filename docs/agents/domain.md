# Domain docs

How the engineering skills read continuo's domain documentation. Layout: multi-context, one context per service plus one for the words every service shares.

## Before exploring, read these

- **`GLOSSARY-MAP.md`** at the repository root. It lists every context and where its `GLOSSARY.md` lives. Read the shared glossary (`GLOSSARY.md` at the root) and the glossary of each service you are about to work in.
- **ADRs** in the private repository `carolsimone/arch`, under `continuo/adr/`: read the ones that touch the area you are about to work in. Each ADR's `Area:` line names the service or subsystem it covers. That area's design (`continuo/design/`) and plan (`continuo/plans/`) explain its intent.

Work from a local clone of `carolsimone/arch` (the owner's is `~/github/arch`). Without one, read a file with `gh api repos/carolsimone/arch/contents/continuo/<path> --jq .content | base64 -d`.

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/matt-domain-modeling` skill creates a service's `GLOSSARY.md`, or an ADR, when a term or a decision is actually settled.

An ADR, design or plan describes the system as of its date. Precedence, highest first: the code; tests, guards and CI; `AGENTS.md` and `docs/arch/`; then `carolsimone/arch`. Check any file, function or line detail against the code before you rely on it.

## File structure

```
carolsimone/continuo                    carolsimone/arch
├── GLOSSARY-MAP.md                     └── continuo/
├── GLOSSARY.md         ← shared words      ├── design/
├── run-controller/                         ├── plans/
│   └── GLOSSARY.md                         └── adr/
├── agent-remediation/                          ├── TEMPLATE-adr.md
│   └── GLOSSARY.md                             └── 2026-10-14-rebase-restarts-from-the-failure.md
└── …one per service
```

A term goes in the shared `GLOSSARY.md` when more than one service uses it (release, run, node), and in a service's `GLOSSARY.md` when only that service uses it (a remediation's round, run-controller's write lease). ADRs are never kept per service: they all live in `carolsimone/arch` `continuo/adr/`.

## Writing an ADR

Use `continuo/adr/TEMPLATE-adr.md` in `carolsimone/arch`, not the skill's own format: a title, then `Date`, `Status`, `Product: continuo` and `Area` (the service or subsystem), then Context, Decision, Consequences and Notes. Name the file `YYYY-MM-DD-<the-decision-as-a-sentence>.md`, for example `2026-10-29-consumer-groups-follow-the-service-name.md`. ADRs have no numbers.

In the `carolsimone/arch` clone, `git add` the exact file path, never a directory (the clone holds the owner's untracked drafts), and commit as `continuo: decision — <one-line summary>`. Don't push without asking.

An ADR is never rewritten. To reverse one, write a new ADR and set the old one's `Status` to `Superseded by <file>`; nothing else in it changes.

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in the glossaries. Don't drift to synonyms a glossary explicitly avoids.

If the concept you need isn't in a glossary yet, that's a signal: either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/matt-domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts `2026-10-21-no-compatibility-work-before-users`, but worth reopening because…_
