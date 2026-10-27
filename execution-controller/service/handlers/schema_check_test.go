package handlers

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakeSchemaColumns struct {
	missing []string
	err     error
	asked   []string
}

func (f *fakeSchemaColumns) Missing(_ context.Context, want []string) ([]string, error) {
	f.asked = want
	return f.missing, f.err
}

func TestCheckSchema_PassesWhenEveryRequiredColumnExists(t *testing.T) {
	cols := &fakeSchemaColumns{}
	if err := CheckSchema(context.Background(), cols); err != nil {
		t.Fatalf("err = %v", err)
	}
	if strings.Join(cols.asked, ",") != strings.Join(RequiredColumns, ",") {
		t.Fatalf("asked for %v, want %v", cols.asked, RequiredColumns)
	}
}

func TestCheckSchema_RefusesWhileAColumnIsMissing(t *testing.T) {
	err := CheckSchema(context.Background(), &fakeSchemaColumns{missing: []string{"deployments.facts"}})
	if !errors.Is(err, ErrSchemaNotReady) {
		t.Fatalf("err = %v, want ErrSchemaNotReady", err)
	}
	if !strings.Contains(err.Error(), "deployments.facts") {
		t.Fatalf("err = %q, want the missing column named", err)
	}
}

func TestCheckSchema_ReadFailureIsNotAMissingSchema(t *testing.T) {
	err := CheckSchema(context.Background(), &fakeSchemaColumns{err: errors.New("connection refused")})
	if err == nil || errors.Is(err, ErrSchemaNotReady) {
		t.Fatalf("err = %v, want a read error", err)
	}
}

// schemaWithout is a SchemaColumns over a database holding every required
// column but one.
type schemaWithout struct{ absent string }

func (s schemaWithout) Missing(_ context.Context, want []string) ([]string, error) {
	var missing []string
	for _, col := range want {
		if col == s.absent {
			missing = append(missing, col)
		}
	}
	return missing, nil
}

func TestCheckSchema_RefusesADatabaseLackingAnyRequiredColumn(t *testing.T) {
	for _, col := range RequiredColumns {
		t.Run(col, func(t *testing.T) {
			err := CheckSchema(context.Background(), schemaWithout{absent: col})
			if !errors.Is(err, ErrSchemaNotReady) {
				t.Fatalf("err = %v, want ErrSchemaNotReady", err)
			}
			if !strings.Contains(err.Error(), col) {
				t.Fatalf("err = %q, want %s named", err, col)
			}
		})
	}
}

func TestRequiredColumns_CoverTheRunColumns(t *testing.T) {
	have := strings.Join(RequiredColumns, ",")
	for _, col := range []string{"deployments.run_id", "cancelled_runs.run_id"} {
		if !strings.Contains(have, col) {
			t.Fatalf("RequiredColumns = %v, want %s", RequiredColumns, col)
		}
	}
}
