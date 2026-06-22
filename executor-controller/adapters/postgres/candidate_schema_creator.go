package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/executor-controller/service/ports"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// pgUniqueViolation is the SQLSTATE Postgres raises when two concurrent
// `CREATE SCHEMA IF NOT EXISTS` statements race on the pg_namespace unique
// index. IF EXISTS is not atomic, so this can surface even though the desired
// end state (schema present) is reached.
const pgUniqueViolation = "23505"

type candidateSchemaCreator struct {
	db     *sqlx.DB
	logger *slog.Logger
}

var _ ports.CandidateSchemaCreator = (*candidateSchemaCreator)(nil)

// NewCandidateSchemaCreator builds a creator over a connection to the dbt
// warehouse (the database where validation jobs materialize candidate tables).
func NewCandidateSchemaCreator(db *sqlx.DB, logger *slog.Logger) ports.CandidateSchemaCreator {
	return &candidateSchemaCreator{db: db, logger: logger}
}

// EnsureCandidateSchema creates the candidate schema race-safely.
//
// The name is validated against the candidate-schema allowlist first; any other
// shape is a programming error and is refused rather than executed. Creation runs
// inside a transaction that first takes a transaction-scoped advisory lock keyed
// on the schema name, so concurrent callers for the same release serialize and
// only one issues the CREATE — the lock releases automatically at commit. A
// unique-violation is still tolerated (and treated as success) as a second line
// of defense against any creator that does not hold the lock.
func (c *candidateSchemaCreator) EnsureCandidateSchema(ctx context.Context, schema string) error {
	if !candidateSchemaNameRe.MatchString(schema) {
		return fmt.Errorf("refusing to create non-candidate schema %q", schema)
	}

	tx, err := c.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx for candidate schema %s: %w", schema, err)
	}
	defer tx.Rollback() //nolint:errcheck

	// hashtext maps the schema name to the int4 key pg_advisory_xact_lock needs.
	// Holders of the same key block until the lock is released at commit, so only
	// one transaction at a time runs the CREATE for a given candidate schema.
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, schema); err != nil {
		return fmt.Errorf("advisory lock for candidate schema %s: %w", schema, err)
	}

	stmt := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, pq.QuoteIdentifier(schema))
	if _, err := tx.ExecContext(ctx, stmt); err != nil {
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && string(pqErr.Code) == pgUniqueViolation {
			// Another creator that did not take the lock won the race; the schema
			// now exists, which is the desired outcome.
			c.logger.InfoContext(ctx, "candidate schema already exists (concurrent create); continuing",
				"schema", schema)
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit candidate schema %s after concurrent create: %w", schema, err)
			}
			return nil
		}
		return fmt.Errorf("create candidate schema %s: %w", schema, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit candidate schema %s: %w", schema, err)
	}
	c.logger.InfoContext(ctx, "candidate schema ensured", "schema", schema)
	return nil
}
