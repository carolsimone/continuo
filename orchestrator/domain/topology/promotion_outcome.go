package topology

// PromotionOutcome is what a release promotion did to the live topology.
type PromotionOutcome string

const (
	// PromotionApplied: the promotion was newer than the live one and its
	// topology is now live.
	PromotionApplied PromotionOutcome = "applied"
	// PromotionRedelivered: the same promotion is already live; nothing changed.
	PromotionRedelivered PromotionOutcome = "redelivered"
	// PromotionStale: a newer promotion is live; the topology was left alone.
	PromotionStale PromotionOutcome = "stale"
)

// DecidePromotion compares an incoming promotion with the live one. Promotion
// seqs are allocated by release-controller, one per announcement and strictly
// increasing, so a greater seq is always the newer topology whatever order the
// events arrive in. A seq equal to the live one is the same announcement
// delivered again when it names the live release, and an anomaly otherwise; an
// anomaly is never applied.
func DecidePromotion(liveSeq int64, liveReleaseID string, seq int64, releaseID string) PromotionOutcome {
	switch {
	case seq > liveSeq:
		return PromotionApplied
	case seq == liveSeq && releaseID == liveReleaseID:
		return PromotionRedelivered
	default:
		return PromotionStale
	}
}
