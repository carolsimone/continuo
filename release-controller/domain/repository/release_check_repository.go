package repository

import (
	"context"
	"errors"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
)

// ErrCheckKindConflict marks a check id that already names a persisted check of the
// other kind. The receive handlers raise it from the check they load for the id;
// ReleaseCheckRepository.Save also returns it, as a guard against an upsert that would
// change a persisted check's kind.
var ErrCheckKindConflict = errors.New("check id already names a check of another kind")

// ListCursor is the keyset position for paginating checks newest-first.
type ListCursor struct {
	CreatedAt time.Time
	CheckID   string
}

// ListFilter parameterises a paginated check listing. Every field is optional;
// a nil pointer means "no filter on that dimension".
type ListFilter struct {
	Kind              *releasecheck.Kind
	Status            *string
	VerifiesReleaseID *string
	Limit             int
	Cursor            *ListCursor
}

// ReleaseCheckRepository is the collection-like abstraction over the releasecheck.Check
// aggregate, both kinds in one collection. Concrete implementations live in
// adapters/postgres.
type ReleaseCheckRepository interface {
	Get(ctx context.Context, id string) (*releasecheck.Check, error)
	// Load returns the check like Get, but takes a row-level FOR UPDATE lock so
	// concurrent per-node projection upserts and the terminal handler
	// serialize on the same row. Must be called inside a transaction.
	Load(ctx context.Context, id string) (*releasecheck.Check, error)
	Save(ctx context.Context, r *releasecheck.Check) error
	// Create inserts r only when no check has its id, and reports whether it
	// did. It never changes an existing row, so two submissions racing on one
	// new id cannot both write: the loser gets false and reads the winner's
	// check instead.
	Create(ctx context.Context, r *releasecheck.Check) (bool, error)
	// NextQueued is the oldest received check of either kind; nil if none.
	NextQueued(ctx context.Context) (*releasecheck.Check, error)

	// NextQueuedOfKind is the oldest received check of the given kind; nil if none.
	NextQueuedOfKind(ctx context.Context, kind releasecheck.Kind) (*releasecheck.Check, error)
	// Active is the single check of either kind currently compiling, parsing,
	// seed_building, or validating; nil if none.
	Active(ctx context.Context) (*releasecheck.Check, error)
	// ActiveCandidateProjectsWithKind lists the changed projects of the
	// candidate checks of the given manifest kind that have not ended, sorted
	// ascending and without repeats. A fix's release check and a check that ended
	// are left out. No such check is an empty list, not an error.
	ActiveCandidateProjectsWithKind(ctx context.Context, kind release.ManifestKind) ([]string, error)
	// List is the paginated history, newest first, narrowed by the filter.
	List(ctx context.Context, f ListFilter) (items []*releasecheck.Check, next *ListCursor, err error)
	// DeleteFinishedBefore removes terminal checks of either kind created
	// before cutoff, except those whose id is in keepIDs. Returns the count.
	DeleteFinishedBefore(ctx context.Context, cutoff time.Time, keepIDs []string) (int, error)
}
