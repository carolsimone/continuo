package repository

import (
	"context"
	"errors"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
)

// ErrRunKindConflict is returned by RunRepository.Save when a run's id already
// names a persisted run of the other kind. It is the atomic backstop for the
// pre-write kind check: two submissions racing on one id both read no row, and
// this is what stops the loser from silently overwriting the winner's kind.
var ErrRunKindConflict = errors.New("run id already names a run of another kind")

// ListCursor is the keyset position for paginating runs newest-first.
type ListCursor struct {
	CreatedAt time.Time
	RunID     string
}

// ListFilter parameterises a paginated run listing. Every field is optional;
// a nil pointer means "no filter on that dimension".
type ListFilter struct {
	Kind              *pipeline.Kind
	Status            *string
	VerifiesReleaseID *string
	Limit             int
	Cursor            *ListCursor
}

// RunRepository is the collection-like abstraction over the pipeline.Run
// aggregate, both kinds in one collection. Concrete implementations live in
// adapters/postgres.
type RunRepository interface {
	Get(ctx context.Context, id string) (*pipeline.Run, error)
	// Load returns the run like Get, but takes a row-level FOR UPDATE lock so
	// concurrent per-node projection upserts and the terminal handler
	// serialize on the same row. Must be called inside a transaction.
	Load(ctx context.Context, id string) (*pipeline.Run, error)
	Save(ctx context.Context, r *pipeline.Run) error
	// NextQueued is the oldest received run of either kind; nil if none.
	NextQueued(ctx context.Context) (*pipeline.Run, error)
	// Active is the single run of either kind currently compiling, parsing,
	// seed_building, or validating; nil if none.
	Active(ctx context.Context) (*pipeline.Run, error)
	// List is the paginated history, newest first, narrowed by the filter.
	List(ctx context.Context, f ListFilter) (items []*pipeline.Run, next *ListCursor, err error)
	// DeleteFinishedBefore removes terminal runs of either kind created
	// before cutoff, except those whose id is in keepIDs. Returns the count.
	DeleteFinishedBefore(ctx context.Context, cutoff time.Time, keepIDs []string) (int, error)
}
