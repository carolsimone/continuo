// Package releasehttp implements ports.ReleaseGateway over release-controller's
// public HTTP API: submitting shadow verification releases and polling their
// verdicts.
package releasehttp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// maxReleaseBodyBytes bounds how much of a release-controller response this
// gateway reads, matching the defensive cap other HTTP adapters in this
// service apply to third-party responses.
const maxReleaseBodyBytes = 1 << 20 // 1 MiB

// Gateway implements ports.ReleaseGateway against a release-controller HTTP
// endpoint.
type Gateway struct {
	baseURL  string
	hc       *http.Client
	evidence ports.EvidenceReader
}

var _ ports.ReleaseGateway = (*Gateway)(nil)

// NewGateway builds a Gateway. baseURL is release-controller's HTTP root
// (e.g. http://release-controller:8088); evidence resolves a validation
// node's run_results_uri to the sentinel JSON a rejected verdict's
// NodeErrors is built from.
func NewGateway(baseURL string, evidence ports.EvidenceReader, hc *http.Client) *Gateway {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &Gateway{baseURL: strings.TrimRight(baseURL, "/"), hc: hc, evidence: evidence}
}

// shadowSubmissionBody is the wire shape POST /releases expects for a shadow
// verification release. Bootstrap, Kind, and Shadow are never caller-supplied
// (see ports.ShadowSubmission) — a shadow submission always bootstraps
// nothing, always parses a python contract, and always sets shadow:true so
// release-controller stops it at "validated" instead of promoting.
type shadowSubmissionBody struct {
	ReleaseID string `json:"release_id"`
	Service   string `json:"service"`
	ImageTag  string `json:"image_tag"`
	Bootstrap bool   `json:"bootstrap"`
	Repo      string `json:"repo"`
	CommitSHA string `json:"commit_sha"`
	Kind      string `json:"kind"`
	Shadow    bool   `json:"shadow"`
}

// Submit posts s as a shadow verification release. A 202 Accepted response is
// success whether release-controller created a fresh release row or matched
// an existing one for the same release id (its POST /releases handler is
// idempotent on that id and returns 202 either way).
func (g *Gateway) Submit(ctx context.Context, s ports.ShadowSubmission) error {
	body := shadowSubmissionBody{
		ReleaseID: s.ReleaseID,
		Service:   s.Service,
		ImageTag:  s.ImageTag,
		Bootstrap: false,
		Repo:      s.Repo,
		CommitSHA: s.CommitSHA,
		Kind:      "python",
		Shadow:    true,
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal shadow submission %s: %w", s.ReleaseID, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.baseURL+"/releases", bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("build shadow submission request %s: %w", s.ReleaseID, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.hc.Do(req)
	if err != nil {
		return fmt.Errorf("submit shadow release %s: %w", s.ReleaseID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusAccepted {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("submit shadow release %s: status %d: %s", s.ReleaseID, resp.StatusCode, errBody)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxReleaseBodyBytes))
	return nil
}

// releaseNodeResult mirrors the per_node_results entries GET /releases/{id}
// returns (release-controller's release.NodeValidationResult), narrowed to
// the fields Verdict needs.
type releaseNodeResult struct {
	Stage         string `json:"stage"`
	NodeID        string `json:"node_id"`
	Status        string `json:"status"`
	RunResultsURI string `json:"run_results_uri"`
}

// releaseResponse mirrors the JSON object release-controller's GET
// /releases/{id} returns (adapters/http/handler_get_release.go's
// getReleaseResponse), narrowed to the fields this gateway reads.
type releaseResponse struct {
	Status         string              `json:"status"`
	RejectReason   string              `json:"reject_reason"`
	RejectDetail   string              `json:"reject_detail"`
	PerNodeResults []releaseNodeResult `json:"per_node_results"`
	ImageTags      map[string]string   `json:"image_tags"`
}

// Release statuses this gateway distinguishes. Every other value from GET
// /releases/{id} (received, compiling, parsing, seed_building, validating,
// promoted, superseded) is non-terminal from a shadow-verdict's point of
// view — promoted/superseded specifically can never occur for a shadow
// release, which release-controller stops at validated instead.
const (
	releaseStatusValidated = "validated"
	releaseStatusRejected  = "rejected"
)

// sentinelResult is the cross-language structured validation-result contract
// (continuo_validation_contract/result.py on the python side, pkg/validationresult
// in Go): the JSON k8s-controller extracts from a validation pod's sentinel-framed
// stdout block and uploads verbatim to run_results_uri.
type sentinelResult struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

// getRelease fetches and decodes GET /releases/{releaseID}.
func (g *Gateway) getRelease(ctx context.Context, releaseID string) (releaseResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.baseURL+"/releases/"+releaseID, nil)
	if err != nil {
		return releaseResponse{}, fmt.Errorf("build get release request %s: %w", releaseID, err)
	}
	resp, err := g.hc.Do(req)
	if err != nil {
		return releaseResponse{}, fmt.Errorf("get release %s: %w", releaseID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return releaseResponse{}, fmt.Errorf("get release %s: status %d: %s", releaseID, resp.StatusCode, errBody)
	}
	var out releaseResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxReleaseBodyBytes)).Decode(&out); err != nil {
		return releaseResponse{}, fmt.Errorf("get release %s: decode: %w", releaseID, err)
	}
	return out, nil
}

// Verdict reads the shadow release identified by releaseID and reports its
// current status.
func (g *Gateway) Verdict(ctx context.Context, releaseID string) (ports.ShadowVerdict, error) {
	rel, err := g.getRelease(ctx, releaseID)
	if err != nil {
		return ports.ShadowVerdict{}, err
	}
	switch rel.Status {
	case releaseStatusValidated:
		return ports.ShadowVerdict{Terminal: true, Validated: true}, nil
	case releaseStatusRejected:
		return ports.ShadowVerdict{Terminal: true, Validated: false, NodeErrors: g.nodeErrors(ctx, rel)}, nil
	default:
		return ports.ShadowVerdict{}, nil
	}
}

// rejectFallback combines the release's reject_reason and reject_detail into
// one human-readable string, the same "reason — detail" shape ui-service's
// ReleaseDetailPage renders, so a node whose own structured result can't be
// resolved still carries a meaningful error text.
func rejectFallback(rel releaseResponse) string {
	if rel.RejectDetail == "" {
		return rel.RejectReason
	}
	if rel.RejectReason == "" {
		return rel.RejectDetail
	}
	return rel.RejectReason + " — " + rel.RejectDetail
}

// nodeErrors builds the failing-node -> error-text map for a rejected
// release: every per_node_results entry from the validation stage that did
// not pass. Each entry's error text is the sentinel JSON's message, read from
// its run_results_uri through the EvidenceReader; when that read yields no
// message (empty/missing URI, a fetch error, or an unparseable/empty-message
// body), the entry falls back to the release-level reject reason and detail.
func (g *Gateway) nodeErrors(ctx context.Context, rel releaseResponse) map[string]string {
	fallback := rejectFallback(rel)
	out := make(map[string]string)
	for _, n := range rel.PerNodeResults {
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

// fetchMessage resolves a validation node's run_results_uri to its sentinel
// JSON's message. Any failure to fetch or parse degrades to "" rather than an
// error, since a missing/unreadable structured result is not itself the
// caller's fault — nodeErrors falls back to the release-level reject detail.
func (g *Gateway) fetchMessage(ctx context.Context, uri string) string {
	if uri == "" {
		return ""
	}
	raw, err := g.evidence.Fetch(ctx, uri)
	if err != nil {
		return ""
	}
	var sr sentinelResult
	if err := json.Unmarshal([]byte(raw), &sr); err != nil {
		return ""
	}
	return sr.Message
}

// ImageTag returns image_tags[service] from the release identified by
// releaseID — the ORIGINAL failing release, not a shadow (see the
// ports.ReleaseGateway doc comment). An absent or empty tag is an error: a
// shadow submission can never proceed without a real image to validate.
func (g *Gateway) ImageTag(ctx context.Context, releaseID, service string) (string, error) {
	rel, err := g.getRelease(ctx, releaseID)
	if err != nil {
		return "", err
	}
	tag, ok := rel.ImageTags[service]
	if !ok || tag == "" {
		return "", fmt.Errorf("release %s has no image tag for service %q", releaseID, service)
	}
	return tag, nil
}
