package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/carolsimone/continuo/executor-controller/service/ports"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// candidateSchemaNameRe is the strict allowlist for candidate schema names: the
// literal "_candidate_" prefix followed by one or more [a-zA-Z0-9_] characters.
// DROP SCHEMA cannot use bind parameters (identifiers are not values), so the
// name is validated against this pattern before being passed to pq.QuoteIdentifier.
var candidateSchemaNameRe = regexp.MustCompile(`^_candidate_[a-zA-Z0-9_]+$`)

type candidateSchemaCleaner struct {
	db     *sqlx.DB
	logger *slog.Logger
}

var _ ports.CandidateSchemaCleaner = (*candidateSchemaCleaner)(nil)

// NewCandidateSchemaCleaner builds a cleaner over a connection to the dbt
// warehouse (the database where validation jobs materialize candidate tables).
func NewCandidateSchemaCleaner(db *sqlx.DB, logger *slog.Logger) ports.CandidateSchemaCleaner {
	return &candidateSchemaCleaner{db: db, logger: logger}
}

// DropCandidateSchema runs DROP SCHEMA IF EXISTS "<schema>" CASCADE. The name
// is validated against the candidate-schema allowlist first; any other shape is
// a programming error and is refused rather than executed. IF EXISTS makes the
// call idempotent: dropping a missing schema is not an error.
func (c *candidateSchemaCleaner) DropCandidateSchema(ctx context.Context, schema string) error {
	if !candidateSchemaNameRe.MatchString(schema) {
		return fmt.Errorf("refusing to drop non-candidate schema %q", schema)
	}
	stmt := fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, pq.QuoteIdentifier(schema))
	if _, err := c.db.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("drop candidate schema %s: %w", schema, err)
	}
	c.logger.InfoContext(ctx, "candidate schema dropped", "schema", schema)
	return nil
}
