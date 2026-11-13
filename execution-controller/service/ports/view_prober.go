package ports

import "context"

// ProbeRequest asks for a probe of views in the warehouse.
type ProbeRequest struct {
	// JobName is the Job of the attempt the probe belongs to; the probe's own
	// name derives from it, so a repeated request names the same probe.
	JobName string
	// NodeID is the node the attempt runs.
	NodeID string
	// Views holds the "<schema>.<name>" of each view to probe.
	Views []string
}

// ViewProber queries views in the warehouse to see whether each still answers.
type ViewProber interface {
	// ProbeViews runs the probe and returns the log it printed, which holds
	// its result block. It returns an error when the probe cannot run to the
	// end or ctx ends first.
	ProbeViews(ctx context.Context, req ProbeRequest) (log string, err error)
}
