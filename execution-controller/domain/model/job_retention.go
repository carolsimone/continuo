package model

import "time"

// FinishedJobRetention is how long Kubernetes keeps a finished Job, and its
// pods, before deleting it: the TTL every Job execution-controller creates
// carries. A deployment done without its outcome for longer than this has no
// Job left to learn its result from.
const FinishedJobRetention = 24 * time.Hour
