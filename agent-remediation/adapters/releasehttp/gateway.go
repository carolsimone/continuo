// Package releasehttp implements ports.VerificationPipeline and
// ports.ReleaseReader over release-controller's public HTTP API.
package releasehttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// maxReleaseBodyBytes bounds how much of a release-controller response this
// gateway reads, matching the defensive cap other HTTP adapters in this
// service apply to third-party responses.
const maxReleaseBodyBytes = 1 << 20 // 1 MiB

// Gateway implements ports.VerificationPipeline and ports.ReleaseReader
// against a release-controller HTTP endpoint.
type Gateway struct {
	baseURL  string
	hc       *http.Client
	evidence ports.EvidenceReader
}

var (
	_ ports.VerificationPipeline = (*Gateway)(nil)
	_ ports.ReleaseReader        = (*Gateway)(nil)
)

// NewGateway builds a Gateway. baseURL is release-controller's HTTP root
// (e.g. http://release-controller:8088); evidence resolves a validation
// node's run_results_uri to the sentinel JSON a failed run's NodeErrors is
// built from.
func NewGateway(baseURL string, evidence ports.EvidenceReader, hc *http.Client) *Gateway {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &Gateway{baseURL: strings.TrimRight(baseURL, "/"), hc: hc, evidence: evidence}
}

// verificationBody is the POST /verification-runs wire shape.
type verificationBody struct {
	RunID             string `json:"run_id"`
	Service           string `json:"service"`
	ImageTag          string `json:"image_tag"`
	Kind              string `json:"kind"`
	VerifiesReleaseID string `json:"verifies_release_id"`
	Attempt           int    `json:"attempt"`
	SourceOverlayURI  string `json:"source_overlay_uri,omitempty"`
}

// Submit posts a verification run. 202 — including release-controller's
// idempotent already-known case — is success; any other status is an error
// carrying the response body.
func (g *Gateway) Submit(ctx context.Context, r ports.VerificationRequest) error {
	if r.Kind == "" {
		return fmt.Errorf("submit verification run %s: kind is required", r.RunID)
	}
	raw, err := json.Marshal(verificationBody{
		RunID: r.RunID, Service: r.Service, ImageTag: r.ImageTag, Kind: r.Kind,
		VerifiesReleaseID: r.VerifiesReleaseID, Attempt: r.Attempt, SourceOverlayURI: r.SourceOverlayURI,
	})
	if err != nil {
		return fmt.Errorf("marshal verification run %s: %w", r.RunID, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.baseURL+"/verification-runs", bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("build verification run request %s: %w", r.RunID, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.hc.Do(req)
	if err != nil {
		return fmt.Errorf("submit verification run %s: %w", r.RunID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("submit verification run %s: status %d: %s", r.RunID, resp.StatusCode, errBody)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxReleaseBodyBytes))
	return nil
}

// runNodeResult mirrors a per_node_results entry, narrowed to what Status reads.
type runNodeResult struct {
	Stage         string `json:"stage"`
	NodeID        string `json:"node_id"`
	Status        string `json:"status"`
	RunResultsURI string `json:"run_results_uri"`
}

// runResponse mirrors GET /verification-runs/{id}, narrowed to what Status reads.
type runResponse struct {
	Status         string          `json:"status"`
	ActivatedAt    string          `json:"activated_at"`
	FailReason     string          `json:"fail_reason"`
	FailDetail     string          `json:"fail_detail"`
	PerNodeResults []runNodeResult `json:"per_node_results"`
}

// phaseOf maps a run status onto the phase the proposal records.
func phaseOf(status string) proposal.Phase {
	switch status {
	case "received":
		return proposal.PhaseQueued
	case "passed":
		return proposal.PhasePassed
	case "failed":
		return proposal.PhaseFailed
	default: // compiling, parsing, seed_building, validating
		return proposal.PhaseRunning
	}
}

// Status reads a verification run and reports its phase, when it started,
// and — for a failed run — each failing validation node's error text.
func (g *Gateway) Status(ctx context.Context, runID string) (ports.VerificationStatus, error) {
	var run runResponse
	if err := g.getJSON(ctx, "/verification-runs/"+runID, &run); err != nil {
		return ports.VerificationStatus{}, err
	}
	st := ports.VerificationStatus{Phase: phaseOf(run.Status)}
	if run.ActivatedAt != "" {
		at, err := time.Parse(time.RFC3339, run.ActivatedAt)
		if err != nil {
			return ports.VerificationStatus{}, fmt.Errorf("verification run %s: activated_at %q: %w", runID, run.ActivatedAt, err)
		}
		st.ActivatedAt = at
	}
	if st.Phase == proposal.PhaseFailed {
		st.NodeErrors = g.nodeErrors(ctx, run)
	}
	return st, nil
}

// failFallback is the "reason — detail" text a failing node reports when its
// own structured result cannot be read.
func failFallback(run runResponse) string {
	switch {
	case run.FailDetail == "":
		return run.FailReason
	case run.FailReason == "":
		return run.FailDetail
	}
	return run.FailReason + " — " + run.FailDetail
}

// nodeErrors builds the failing-node -> error-text map for a failed run:
// every per_node_results entry from the validation stage that did not pass.
// Each entry's error text is the sentinel JSON's message, read from its
// run_results_uri through the EvidenceReader; when that read yields no
// message (empty/missing URI, a fetch error, or an unparseable/empty-message
// body), the entry falls back to the run-level fail reason and detail.
func (g *Gateway) nodeErrors(ctx context.Context, run runResponse) map[string]string {
	fallback := failFallback(run)
	out := make(map[string]string)
	for _, n := range run.PerNodeResults {
		if n.Stage != "validation" || n.Status == "ok" {
			continue
		}
		msg := g.fetchMessage(ctx, n.RunResultsURI)
		if msg == "" {
			msg = fallback
		}
		out[n.NodeID] = msg
	}
	return out
}

// sentinelResult is the cross-language structured validation-result contract
// (continuo_validation_contract/result.py on the python side, pkg/validationresult
// in Go): the JSON k8s-controller extracts from a validation pod's sentinel-framed
// stdout block and uploads verbatim to run_results_uri.
type sentinelResult struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// parseSentinelResult locates the structured validation-result record inside an
// uploaded run-results object.
//
// What is stored at run_results_uri is the raw text captured between the
// validation pod's sentinel markers, so it can carry a stderr/log preamble
// before the JSON and further output after it. Unmarshalling the whole body
// strictly fails on any of that and costs the caller the node's real error
// text, so the object is located instead: scan from each '{', decode a single
// JSON value (ignoring trailing content), and accept the first that yields a
// status-bearing record. A preamble containing braces of its own therefore
// decodes to a record with no status and is skipped rather than accepted.
//
// ok is false for a body holding no such object, which is a genuine miss the
// caller answers by falling back to the run-level fail text.
func parseSentinelResult(raw []byte) (sentinelResult, bool) {
	for start := bytes.IndexByte(raw, '{'); start >= 0; {
		var sr sentinelResult
		if err := json.NewDecoder(bytes.NewReader(raw[start:])).Decode(&sr); err == nil && sr.Status != "" {
			return sr, true
		}
		next := bytes.IndexByte(raw[start+1:], '{')
		if next < 0 {
			break
		}
		start += next + 1
	}
	return sentinelResult{}, false
}

// fetchMessage resolves a validation node's run_results_uri to its sentinel
// JSON's message. Any failure to fetch or parse degrades to "" rather than an
// error, since a missing/unreadable structured result is not itself the
// caller's fault — nodeErrors falls back to the run-level fail detail.
func (g *Gateway) fetchMessage(ctx context.Context, uri string) string {
	if uri == "" {
		return ""
	}
	raw, err := g.evidence.Fetch(ctx, uri)
	if err != nil {
		return ""
	}
	sr, ok := parseSentinelResult([]byte(raw))
	if !ok {
		return ""
	}
	return sr.Message
}

// releaseResponse mirrors GET /releases/{id}, narrowed to what ImageTag reads.
type releaseResponse struct {
	ImageTags map[string]string `json:"image_tags"`
}

// ImageTag returns image_tags[service] of the candidate release releaseID.
func (g *Gateway) ImageTag(ctx context.Context, releaseID, service string) (string, error) {
	var rel releaseResponse
	if err := g.getJSON(ctx, "/releases/"+releaseID, &rel); err != nil {
		return "", err
	}
	tag, ok := rel.ImageTags[service]
	if !ok || tag == "" {
		return "", fmt.Errorf("release %s has no image tag for service %q", releaseID, service)
	}
	return tag, nil
}

// getJSON fetches and decodes one release-controller resource.
func (g *Gateway) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("build request %s: %w", path, err)
	}
	resp, err := g.hc.Do(req)
	if err != nil {
		return fmt.Errorf("get %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("get %s: status %d: %s", path, resp.StatusCode, errBody)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxReleaseBodyBytes)).Decode(out); err != nil {
		return fmt.Errorf("get %s: decode: %w", path, err)
	}
	return nil
}
