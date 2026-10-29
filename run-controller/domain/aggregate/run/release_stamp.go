package run

// ReleaseStamp names the release a run runs on and the promotion seq that
// release was live under when the run was created — or, for a run that
// replays another run's release, the source run's. The zero value means the
// run names no release.
type ReleaseStamp struct {
	ReleaseID    string
	PromotionSeq int64
}

// IsZero reports whether s names no release.
func (s ReleaseStamp) IsZero() bool { return s.ReleaseID == "" && s.PromotionSeq == 0 }
