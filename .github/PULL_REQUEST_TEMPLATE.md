## What this changes

<!-- One or two sentences. Link the issue this addresses. -->

Closes #

## Why

<!-- What problem does this solve? -->

## How it was verified

<!-- The commands you ran and what they printed. "Tests pass" on its own is not enough. -->

## Checklist

- [ ] Every commit is signed off (`git commit -s`) — the `dco` check enforces this
- [ ] Tests added or updated for the behavior changed
- [ ] `scripts/lint-go.sh --ci` passes (Go changes)
- [ ] Full `npm test` and `npm run build` pass (ui changes)
- [ ] `docs/arch/*` updated if service behavior, interfaces, storage ownership, Redis
      flows, gRPC surfaces, Kubernetes behavior, or S3 usage changed
- [ ] Chart changes also update `deploy/continuo/values.schema.json` and
      `deploy/continuo/CHANGELOG.md`
- [ ] New Redis streams or consumer groups are declared in `pkg/streams/contract.yaml`
      with the regenerated files committed
