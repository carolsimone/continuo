package events

// ModePromoteSeed is the legacy job mode for prod seed builds triggered on
// promotion. Nothing produces it any more: promoted seeds now run as an ordinary
// run, so their Jobs carry no mode label and route through the production
// lifecycle that owns retries, task executions, and status.
//
// It is retained only to drain work queued by a previous version during a
// rolling upgrade — in-flight query.model:v1 messages and deployments
// rows whose job_params still carry it. Those tasks have synthetic IDs with no
// matching row in state, so announcing their lifecycle would wedge state's
// consumer on a run it cannot load. Remove this once no such work can remain.
const ModePromoteSeed = "promote_seed"

// ModeCompile is the job mode for a release's compile leg (dbt compile +
// manifest upload, plus the parse-export/rehearsal initContainers). the
// dispatcher stamps it as a Job label; the job-status handler reads it to
// route the terminal status to the compile-specific handling path.
const ModeCompile = "compile"

// ModeValidation is the job mode for a release's per-node validation
// (`dbt --empty`) Jobs. the dispatcher stamps it as a Job label; the
// job-status handler reads it to route the terminal status to the validation
// path and to suppress production task-status announcements for these
// synthetic tasks.
const ModeValidation = "validation"

// ModeSeedBuild is the job mode for a release's seed-build Jobs (materializing
// new/changed seeds into the candidate schema). the dispatcher stamps it as a
// Job label; the job-status handler reads it to route the terminal status to
// the seed-build path and to suppress production task-status announcements
// for these synthetic tasks.
const ModeSeedBuild = "seed_build"
