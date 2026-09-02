// Package handlers holds the remediation application layer. Handlers are thin:
// they orchestrate ports and the domain classifier inside a unit of work and
// hold no infrastructure dependencies directly.
package handlers

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/pkg/validationresult"
	"github.com/carolsimone/continuo/remediation/domain/failure"
	"github.com/carolsimone/continuo/remediation/service/ports"
	"github.com/carolsimone/continuo/remediation/service/uow"
)

// Deps holds the collaborators ClassifyRejection needs, all behind ports.
type Deps struct {
	NewUoW    func() uow.UnitOfWork
	LogReader ports.LogReader
	Clock     ports.Clock
	Logger    *slog.Logger
}

// classify produces the classification for one piece of evidence, fetching only
// what the source actually needs. A duplicate-relation rejection is classified
// from the evidence alone: it happens at parse time, before any Job runs, so
// there is no dbt log to read. ev is a pointer because the compile path fills
// in FilePath from the log text.
func classify(ctx context.Context, deps Deps, ev *failure.FailureEvidence) (failure.Classification, error) {
	if ev.Source == failure.SourceDuplicateTable {
		return failure.ClassifyDuplicateTable(*ev), nil
	}

	logText, err := deps.LogReader.Fetch(ctx, ev.DBTLogURI)
	if err != nil {
		if err != ports.ErrLogNotFound {
			return failure.Classification{}, fmt.Errorf("fetch dbt log %q: %w", ev.DBTLogURI, err)
		}
		logText = "" // not found → classify unknown:log_unavailable (or structured, below)
	}

	// Prefer the structured validation result when present; a fetch/parse failure
	// degrades to the text log rather than failing the message.
	var structured *failure.StructuredResult
	if ev.RunResultsURI != "" {
		body, ferr := deps.LogReader.Fetch(ctx, ev.RunResultsURI)
		if ferr != nil && ferr != ports.ErrLogNotFound {
			return failure.Classification{}, fmt.Errorf("fetch run results %q: %w", ev.RunResultsURI, ferr)
		}
		if ferr == nil {
			if r, perr := validationresult.Parse([]byte(body)); perr != nil {
				deps.Logger.Warn("run_results parse failed — falling back to text log",
					"uri", ev.RunResultsURI, "error", perr)
			} else {
				structured = &failure.StructuredResult{
					Status:   r.Status,
					Message:  r.Message,
					Failures: r.Failures,
					UniqueID: r.UniqueID,
				}
			}
		}
	}

	// For compile-stage failures, extract the offending source file path from
	// the log text so the remediation agent can read the file directly. Compile
	// failures have a synthetic service-name NodeID (not a real dbt node), so
	// the log is the only source of the file path. Seed_build failures carry
	// FilePath and Service from the candidate topology via the rejection
	// payload, so no extraction is needed.
	if ev.Source == failure.SourceCompile && ev.FilePath == "" {
		ev.FilePath = failure.ExtractDbtFilePath(logText)
	}

	return failure.ClassifyWithStructured(structured, logText), nil
}
