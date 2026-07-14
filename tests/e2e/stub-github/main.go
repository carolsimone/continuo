// Package main is a minimal GitHub API stub used in e2e tests.
// It listens on :9200 and handles both the remediation-agent read path and the
// ui-service PR-creation write path, so both can be exercised without a real
// GitHub token or network access.
//
// Endpoints served:
//
//	POST /app/installations/{id}/access_tokens
//	    Returns a fake installation token (no JWT verification — any Authorization accepted).
//
//	GET  /repos/{owner}/{repo}/git/ref/heads/{branch}
//	    Returns a deterministic base-commit SHA for branch resolution.
//
//	POST /repos/{owner}/{repo}/git/refs
//	    Creates a branch (stub). Returns 201 with the same deterministic SHA.
//
//	GET  /repos/{owner}/{repo}/contents/{path}  (Accept contains "raw")
//	    Returns the canned ftable_e dbt source — existing remediation-agent read path.
//
//	GET  /repos/{owner}/{repo}/contents/{path}  (JSON Accept, octokit getContent)
//	    Returns 404 so the PR creator issues a create-without-sha.
//
//	PUT  /repos/{owner}/{repo}/contents/{path}
//	    Creates or updates a file (stub). Returns 201 with stub commit SHA.
//
//	GET  /repos/{owner}/{repo}/commits/{sha}
//	    Returns a canned commit whose files[] carries a patch for the upstream
//	    model, exercising the remediation-agent upstream-diff read path.
//
//	POST /repos/{owner}/{repo}/pulls
//	    Opens a PR (stub). Returns 201 with deterministic number and html_url.
//
//	GET  /repos/{owner}/{repo}/pulls
//	    Returns an empty list (no existing PRs).
//
//	GET  /repos/{owner}/{repo}/pulls/{n}
//	    Returns the current PR state (404 when never opened).
//
//	PUT  /repos/{owner}/{repo}/pulls/{n}/merge
//	    Marks PR as merged and closed. Returns 200 with merged confirmation.
//
//	PATCH /repos/{owner}/{repo}/pulls/{n}
//	    With body {"state":"closed"} marks PR as closed without merge. Returns 200 with PR JSON.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// ftableESource is the canned dbt model source for ftable_e as it exists in
// version control. It uses {{ ref(...) }} macros (real source) rather than the
// compiled candidate SQL the remediation-agent receives from S3. The bad join
// to public.wrong_name is present so the Step-2 LLM call can diagnose and
// remove it; the Step-2 stub-llm response returns the corrected source.
const ftableESource = `{{ config(materialized='table') }}
select *
from {{ ref('table_b') }}
join {{ ref('table_c') }} using (id)
join public.silly_error using (id)`

const (
	stubBaseSHA   = "basesha0000000000000000000000000000000000"
	stubCommitSHA = "commitsha000000000000000000000000000000000"
	stubToken     = "ghs_stubtoken"
	stubBranch    = "stub"
	stubPRNumber  = 1
)

// e2eUpstreamFilePath is the repository-relative path of the upstream model whose
// diff the remediation flow fetches: ServiceRepoPaths["service-2"] ("services/
// service-2") joined with the ancestor's original_file_path ("models/ftable_upstream_diff.sql").
const e2eUpstreamFilePath = "services/service-2/models/ftable_upstream_diff.sql"

// ftableCPatch is the canned unified diff returned for any commit lookup. It
// stands in for a recent upstream change so the remediation-agent's upstream-diff
// read path is exercised end to end.
const ftableCPatch = "@@ -1,3 +1,3 @@\n select id, name\n-  , legacy_col\n+  , renamed_col\n from source"

// stubClosedAt is the fixed terminal timestamp reported for closed stub PRs.
const stubClosedAt = "2026-01-01T00:00:00Z"

// prStates tracks each opened stub PR's lifecycle so the reconciler read path
// observes merges/closes performed by tests.
var (
	prMu     sync.Mutex
	prStates = map[int]*stubPRState{}
)

type stubPRState struct {
	Closed bool
	Merged bool
}

// resetPRs clears all PR state (test helper).
func resetPRs() {
	prMu.Lock()
	defer prMu.Unlock()
	prStates = map[int]*stubPRState{}
}

func main() {
	http.HandleFunc("/app/", handleApp)
	http.HandleFunc("/repos/", handleRepos)
	log.Println("stub-github: listening on :9200")
	if err := http.ListenAndServe(":9200", nil); err != nil {
		log.Fatalf("stub-github: %v", err)
	}
}

// handleApp handles GitHub App authentication endpoints.
// POST /app/installations/{id}/access_tokens returns a fake installation token.
func handleApp(w http.ResponseWriter, r *http.Request) {
	// Match: /app/installations/{id}/access_tokens
	if r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/access_tokens") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"token":      stubToken,
			"expires_at": "2099-01-01T00:00:00Z",
		})
		return
	}
	http.Error(w, "not found", http.StatusNotFound)
}

// handleRepos is the top-level dispatcher for all /repos/{owner}/{repo}/...
// endpoints. It routes by HTTP method and path shape.
func handleRepos(w http.ResponseWriter, r *http.Request) {
	// Strip leading /repos/ and split into segments.
	// Path shape: /repos/{owner}/{repo}/{resource...}
	trimmed := strings.TrimPrefix(r.URL.Path, "/repos/")
	parts := strings.SplitN(trimmed, "/", 3) // [owner, repo, rest]
	if len(parts) < 3 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	rest := parts[2] // everything after /repos/{owner}/{repo}/

	switch {
	case strings.HasPrefix(rest, "git/ref/"):
		handleGitRef(w, r)
	case rest == "git/refs" || strings.HasPrefix(rest, "git/refs"):
		handleGitRefs(w, r)
	case strings.HasPrefix(rest, "contents/"):
		handleContents(w, r)
	case strings.HasPrefix(rest, "commits/"):
		handleCommits(w, r)
	case rest == "pulls" || strings.HasPrefix(rest, "pulls"):
		handlePulls(w, r)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// handleGitRef responds to GET /repos/{owner}/{repo}/git/ref/heads/{branch}
// with a deterministic base-commit SHA for branch resolution.
func handleGitRef(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Extract branch name from path: .../git/ref/heads/{branch}
	parts := strings.SplitN(r.URL.Path, "/git/ref/", 2)
	ref := "refs/heads/main"
	if len(parts) == 2 {
		ref = "refs/" + parts[1]
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"ref": ref,
		"object": map[string]string{
			"sha":  stubBaseSHA,
			"type": "commit",
		},
	})
}

// handleGitRefs responds to POST /repos/{owner}/{repo}/git/refs (create branch).
// Returns 201 with the same deterministic SHA.
func handleGitRefs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"ref": "refs/heads/" + stubBranch,
		"object": map[string]string{
			"sha":  stubBaseSHA,
			"type": "commit",
		},
	})
}

// handleContents responds to GET and PUT /repos/{owner}/{repo}/contents/{path}.
//
// GET with Accept containing "raw" (the remediation-agent read path) returns
// the canned ftable_e source as raw text. GET with a JSON Accept (octokit
// getContent) returns 404 so the PR creator issues a create-without-sha.
//
// PUT (create/update file) returns 201 with a stub commit SHA.
func handleContents(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if strings.Contains(r.Header.Get("Accept"), "raw") {
			// Remediation-agent raw read path — existing behavior preserved.
			w.Header().Set("Content-Type", "application/vnd.github.raw")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(ftableESource))
			return
		}
		// octokit getContent (JSON Accept): signal "file does not exist" so the
		// PR creator performs a create without a blob SHA.
		http.Error(w, "not found", http.StatusNotFound)

	case http.MethodPut:
		// Extract the path component after "contents/".
		pathParts := strings.SplitN(r.URL.Path, "/contents/", 2)
		filePath := ""
		if len(pathParts) == 2 {
			filePath = pathParts[1]
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"content": map[string]string{
				"name": "f",
				"path": filePath,
			},
			"commit": map[string]string{
				"sha": stubCommitSHA,
			},
		})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleCommits responds to GET /repos/{owner}/{repo}/commits/{sha} with a canned
// commit whose files[] carries a patch for the upstream model the remediation
// flow reads. The filename matches the upstream ancestor's repository-relative
// path used in the e2e topology.
func handleCommits(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"sha": stubCommitSHA,
		"files": []map[string]string{
			{"filename": e2eUpstreamFilePath, "patch": ftableCPatch},
		},
	})
}

// handlePulls routes /repos/{o}/{r}/pulls[...]:
//
//	GET  pulls            -> empty list (no existing PRs for branch lookups)
//	POST pulls            -> open stub PR #1 (registers lifecycle state)
//	GET  pulls/{n}        -> current PR state (404 when never opened)
//	PUT  pulls/{n}/merge  -> mark merged
//	PATCH pulls/{n}       -> {"state":"closed"} marks closed without merge
func handlePulls(w http.ResponseWriter, r *http.Request) {
	// Isolate the segment after ".../pulls".
	_, after, _ := strings.Cut(r.URL.Path, "/pulls")
	after = strings.TrimPrefix(after, "/")

	switch {
	case after == "" && r.Method == http.MethodGet:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("[]"))

	case after == "" && r.Method == http.MethodPost:
		prMu.Lock()
		prStates[stubPRNumber] = &stubPRState{}
		prMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"number":   stubPRNumber,
			"html_url": "http://stub-github/pull/1",
			"state":    "open",
		})

	case strings.HasSuffix(after, "/merge") && r.Method == http.MethodPut:
		n, err := strconv.Atoi(strings.TrimSuffix(after, "/merge"))
		if err != nil {
			http.Error(w, "bad pull number", http.StatusBadRequest)
			return
		}
		prMu.Lock()
		st, ok := prStates[n]
		if ok {
			st.Closed, st.Merged = true, true
		}
		prMu.Unlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"sha": stubCommitSHA, "merged": true,
		})

	case after != "" && r.Method == http.MethodPatch:
		n, err := strconv.Atoi(after)
		if err != nil {
			http.Error(w, "bad pull number", http.StatusBadRequest)
			return
		}
		var body struct {
			State string `json:"state"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		prMu.Lock()
		st, ok := prStates[n]
		if ok && body.State == "closed" {
			st.Closed = true
		}
		prMu.Unlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		writePullJSON(w, n)

	case after != "" && r.Method == http.MethodGet:
		n, err := strconv.Atoi(after)
		if err != nil {
			http.Error(w, "bad pull number", http.StatusBadRequest)
			return
		}
		writePullJSON(w, n)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// writePullJSON renders the current lifecycle state of PR n in the shape the
// GitHub pulls API returns; unknown n is a 404.
func writePullJSON(w http.ResponseWriter, n int) {
	prMu.Lock()
	st, ok := prStates[n]
	var snapshot stubPRState
	if ok {
		snapshot = *st
	}
	prMu.Unlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	state := "open"
	var mergedAt, closedAt interface{}
	if snapshot.Closed {
		state = "closed"
		closedAt = stubClosedAt
		if snapshot.Merged {
			mergedAt = stubClosedAt
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"number":    n,
		"state":     state,
		"merged":    snapshot.Merged,
		"merged_at": mergedAt,
		"closed_at": closedAt,
		"html_url":  "http://stub-github/pull/1",
	})
}
