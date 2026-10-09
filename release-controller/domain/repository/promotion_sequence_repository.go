package repository

import "context"

// PromotionSequenceRepository hands out promotion seqs: one strictly
// increasing number per announcement of a topology to the rest of continuo.
// Consumers order announcements by it, never by arrival.
type PromotionSequenceRepository interface {
	// Next takes the next seq. Inside a transaction it holds the sequence
	// row's lock until that transaction ends, so concurrent announcements
	// take their seqs one after another; a rolled-back transaction gives its
	// seq back.
	Next(ctx context.Context) (int64, error)
}
