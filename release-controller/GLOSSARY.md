# release-controller

release-controller takes each candidate release through its release check, one at a time, and either promotes it or rejects it. The shared words (release, candidate release, live release, release check, promotion, rejection, project) are in the root `GLOSSARY.md`.

## Language

### Taking a release in

**Release queue**:
The line candidate releases wait in. Only one candidate release is in its release check at a time; the next starts when it ends.
_Avoid_: backlog, release lock

**Changed project**:
The one project a release changes. Every other project of the release is taken as production has it.
_Avoid_: changed service, delta

**Bootstrap release**:
A project's first release, promoted without being validated because production holds nothing to check it against.
_Avoid_: initial release, seed release

**Rebaseline release**:
A bootstrap release that also records every live seed as already loaded, so its promotion starts no promote-seed run.
_Avoid_: reset release, re-seed

### The release check

**Leg**:
One part of the release check, done in order: compile, parse, seed build, validation.
_Avoid_: stage, step, phase

**Candidate schema**:
The warehouse schema where the release check builds a candidate release's tables, apart from production's.
_Avoid_: staging schema, scratch schema, blue schema

**Production copy**:
A node the release check copies from its production table instead of building, because it only feeds the nodes that changed.
_Avoid_: clone, prod clone, snapshot

**Seed reload notice**:
A warning on a candidate release that one of its seeds will drop its table when loaded, naming the views rebuilt with it and predicting whether the load reloads, stops or is refused.
_Avoid_: seed warning, reload alert

### Production

**Production pointer**:
The record, per project, of the code and image production runs now. A new release is assembled from every project's pointer plus the changed project.
_Avoid_: current prod, service prod, prod ref
