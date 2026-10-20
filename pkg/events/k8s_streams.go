package events

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
