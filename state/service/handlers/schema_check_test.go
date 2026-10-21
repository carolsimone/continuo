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
	err := CheckSchema(context.Background(), &fakeSchemaColumns{missing: []string{"task_attempts.attempt"}})
	if !errors.Is(err, ErrSchemaNotReady) {
		t.Fatalf("err = %v, want ErrSchemaNotReady", err)
	}
	if !strings.Contains(err.Error(), "task_attempts.attempt") {
		t.Fatalf("err = %q, want the missing column named", err)
	}
}

func TestCheckSchema_ReadFailureIsNotAMissingSchema(t *testing.T) {
	err := CheckSchema(context.Background(), &fakeSchemaColumns{err: errors.New("connection refused")})
	if err == nil || errors.Is(err, ErrSchemaNotReady) {
		t.Fatalf("err = %v, want a read error", err)
	}
}
