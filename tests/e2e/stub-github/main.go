// Package main is a minimal GitHub API stub used in e2e tests.
// It listens on :9200 and handles both the agent-remediation read path and the
// ui PR-creation write path, so both can be exercised without a real
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
//	PATCH /repos/{owner}/{repo}/git/refs/heads/{branch}
//	    Moves the branch to the posted sha. Returns 200 with the updated ref object.
//
//	POST /repos/{owner}/{repo}/git/blobs
//	    Creates a blob from the posted content. Returns 201 with a sha derived
//	    from that content, so two different files get two different shas.
//
//	GET  /repos/{owner}/{repo}/git/commits/{sha}
//	    Returns the commit's sha and its tree's sha, so the PR creator can use
//	    the tree as the base_tree for the tree it builds on top.
//
//	POST /repos/{owner}/{repo}/git/commits
//	    Creates a commit from the posted message/tree/parents. Returns 201
//	    with a sha derived from the request; the request is recorded and
//	    retrievable via recordedCommitFor.
//
//	POST /repos/{owner}/{repo}/git/trees
//	    Creates a tree from the posted base_tree/entries. Returns 201 with a
//	    sha derived from the request; the request is recorded and retrievable
//	    via recordedTreeFor.
//
//	GET  /repos/{owner}/{repo}/contents/{path}  (Accept contains "raw")
//	    When ref names a commit written via the git/commits endpoint below,
//	    resolves commit -> tree -> blob for path and returns that blob's
//	    content (404 when the commit has no such path). Otherwise (ref unknown
//	    or omitted) returns the canned ftable_e dbt source — the
//	    agent-remediation's single-file read path.
//
//	GET  /repos/{owner}/{repo}/contents/{path}  (JSON Accept)
//	    Returns 404 — the agent-remediation's directory-listing read path,
//	    which treats a 404 as "nothing more to list".
//
//	GET  /repos/{owner}/{repo}/tarball/{ref}
//	    Returns a gzipped tar of the working tree at REPO_FIXTURE_DIR, nested
//	    under a single "{repo}-{ref}/" directory the way GitHub's archive
//	    endpoint does — the agent-remediation's whole-repository read path.
//	    404 when no fixture directory is configured or it cannot be read.
//
//	POST /repos/{owner}/{repo}/pulls
//	    Opens a PR (stub). Returns 201 with an auto-incrementing number and html_url.
//
//	GET  /repos/{owner}/{repo}/pulls?head=&state=
//	    Returns PRs matching the head branch (bare "branch" or "owner:branch")
//	    and state ("open" default, "closed", or "all") — the agent-remediation
//	    opening sweep's branch lookup and the ui PR creator's
//	    422-already-exists retry both call this.
//
//	GET  /repos/{owner}/{repo}/pulls/{n}
//	    Returns the current PR state (404 when never opened).
//
//	PUT  /repos/{owner}/{repo}/pulls/{n}/merge
//	    Marks PR as merged and closed. Resolves the PR's head branch to its
//	    latest written commit (see branchRefs) and records/returns that as the
//	    merge commit sha; when the branch was never pushed to, returns the
//	    fixed stubCommitSHA constant instead (back-compat).
//
//	PATCH /repos/{owner}/{repo}/pulls/{n}
//	    With body {"state":"closed"} marks PR as closed without merge. Returns 200 with PR JSON.
package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// repoFixtureDir is the directory this stub serves as the working tree of
// every repository it is asked for a tarball of. Docker Compose bind-mounts
// the e2e fixture repository there. Empty (the default) means no repository
// exists as far as this stub is concerned, and the tarball route answers 404.
var repoFixtureDir = os.Getenv("REPO_FIXTURE_DIR")

// ftableESource is the canned dbt model source for ftable_e as it exists in
// version control. It uses {{ ref(...) }} macros (real source) rather than the
// compiled candidate SQL the agent-remediation receives from S3. The bad join
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

// stubClosedAt is the fixed terminal timestamp reported for closed stub PRs.
const stubClosedAt = "2026-01-01T00:00:00Z"

// stubBaseTreeSHA is the deterministic tree sha reported as the base commit's
// tree by handleGitCommits' GET path, so the PR creator has a base_tree to
// layer new blobs onto.
const stubBaseTreeSHA = "basetreesha0000000000000000000000000000"

// deterministicSHA derives a stable, git-sha-shaped hex string from parts, so
// a given request body always produces the same sha and a different body
// produces a different one, without relying on wall-clock time or a counter.
func deterministicSHA(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:40]
}

// prStates tracks each opened stub PR's lifecycle so the reconciler read path
// observes merges/closes performed by tests, and so a branch lookup (GET
// pulls?head=...) can find a PR a POST already created for that branch.
var (
	prMu         sync.Mutex
	prStates     = map[int]*stubPRState{}
	nextPRNumber = stubPRNumber
)

// stubPRState is one opened stub PR's full lifecycle state. Head is the
// branch name as posted (no "owner:" prefix — see listPulls for how a
// GET ...?head=owner:branch query is matched against it). Title is stored as
// posted and echoed back on every read, so a test can assert what the pull
// request the system opened is actually called.
type stubPRState struct {
	Number         int
	Title          string
	Head           string
	Base           string
	CreatedAt      string
	Closed         bool
	Merged         bool
	MergeCommitSHA string
}

// resetPRs clears all PR state and rewinds the PR-number counter back to
// stubPRNumber (test helper).
func resetPRs() {
	prMu.Lock()
	defer prMu.Unlock()
	prStates = map[int]*stubPRState{}
	nextPRNumber = stubPRNumber
}

// treeEntry is one posted tree entry, in the shape octokit's createTree sends.
type treeEntry struct {
	Path string `json:"path"`
	Mode string `json:"mode"`
	Type string `json:"type"`
	SHA  string `json:"sha"`
}

// stubTreeWrite is one POST git/trees call's recorded request, keyed by the
// tree sha returned for it, so a test can look up which paths and base tree
// a given commit's tree was built from.
type stubTreeWrite struct {
	BaseTree string
	Entries  []treeEntry
}

// stubCommitWrite is one POST git/commits call's recorded request, keyed by
// the commit sha returned for it.
type stubCommitWrite struct {
	Message string
	Tree    string
	Parents []string
}

// gitMu guards recordedTrees, recordedCommits, recordedBlobs and branchRefs,
// the write-side state for POST git/blobs, POST git/trees, POST git/commits,
// and PATCH git/refs/heads/{branch}.
var (
	gitMu           sync.Mutex
	recordedTrees   = map[string]stubTreeWrite{}
	recordedCommits = map[string]stubCommitWrite{}
	// recordedBlobs holds each written blob's decoded content, keyed by the
	// sha handleGitBlobs returned for it, so handleContents can serve a
	// file's actual text when asked for it at a written commit.
	recordedBlobs = map[string]string{}
	// branchRefs holds each branch's latest written commit sha, keyed by
	// branch name without the "refs/heads/" prefix (as posted to PATCH
	// git/refs/heads/{branch} — the same form a PR's Head is stored in). It
	// is how the merge handler finds a PR's head branch tip.
	branchRefs = map[string]string{}
)

// resetGitWrites clears all recorded tree, commit and blob writes, and all
// branch ref pointers (test helper).
func resetGitWrites() {
	gitMu.Lock()
	defer gitMu.Unlock()
	recordedTrees = map[string]stubTreeWrite{}
	recordedCommits = map[string]stubCommitWrite{}
	recordedBlobs = map[string]string{}
	branchRefs = map[string]string{}
}

// recordedTreeFor returns the request recorded for a POST git/trees call
// that returned sha, and whether one was recorded (test helper).
func recordedTreeFor(sha string) (stubTreeWrite, bool) {
	gitMu.Lock()
	defer gitMu.Unlock()
	w, ok := recordedTrees[sha]
	return w, ok
}

// recordedCommitFor returns the request recorded for a POST git/commits call
// that returned sha, and whether one was recorded (test helper).
func recordedCommitFor(sha string) (stubCommitWrite, bool) {
	gitMu.Lock()
	defer gitMu.Unlock()
	w, ok := recordedCommits[sha]
	return w, ok
}

// recordedBlobFor returns the decoded content recorded for a POST git/blobs
// call that returned sha, and whether one was recorded (test helper).
func recordedBlobFor(sha string) (string, bool) {
	gitMu.Lock()
	defer gitMu.Unlock()
	c, ok := recordedBlobs[sha]
	return c, ok
}

// branchTip returns the latest commit sha written to branch (as recorded by
// a PATCH git/refs/heads/{branch} call), and whether one has ever been
// recorded (test helper).
func branchTip(branch string) (string, bool) {
	gitMu.Lock()
	defer gitMu.Unlock()
	sha, ok := branchRefs[branch]
	return sha, ok
}

// commitTreeBlob resolves path within the tree of the commit recorded under
// sha, returning its blob content. knownCommit reports whether sha names a
// commit this stub has recorded at all (independent of whether path was
// found within it), so a caller can distinguish "ref never written here"
// (fall back to other behavior) from "ref written, but path absent" (404).
func commitTreeBlob(sha, path string) (content string, knownCommit, found bool) {
	gitMu.Lock()
	defer gitMu.Unlock()
	commit, ok := recordedCommits[sha]
	if !ok {
		return "", false, false
	}
	tree, ok := recordedTrees[commit.Tree]
	if !ok {
		return "", true, false
	}
	for _, e := range tree.Entries {
		if e.Path == path {
			c, ok := recordedBlobs[e.SHA]
			return c, true, ok
		}
	}
	return "", true, false
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
	case strings.HasPrefix(rest, "git/blobs"):
		handleGitBlobs(w, r)
	case strings.HasPrefix(rest, "git/commits"):
		handleGitCommits(w, r)
	case strings.HasPrefix(rest, "git/trees"):
		handleGitTrees(w, r)
	case strings.HasPrefix(rest, "contents/"):
		handleContents(w, r)
	case strings.HasPrefix(rest, "tarball/"):
		handleTarball(w, r)
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

// handleGitRefs routes /repos/{owner}/{repo}/git/refs[/heads/{branch}]:
//
//	POST  git/refs               -> creates a branch (stub). Returns 201 with
//	                                 the same deterministic SHA.
//	PATCH git/refs/heads/{branch} -> moves the branch to the posted sha.
//	                                 Returns 200 with the updated ref object.
func handleGitRefs(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"ref": "refs/heads/" + stubBranch,
			"object": map[string]string{
				"sha":  stubBaseSHA,
				"type": "commit",
			},
		})

	case http.MethodPatch:
		// Extract branch name from path: .../git/refs/heads/{branch}
		parts := strings.SplitN(r.URL.Path, "/git/refs/heads/", 2)
		branch := stubBranch
		if len(parts) == 2 && parts[1] != "" {
			branch = parts[1]
		}
		var body struct {
			SHA   string `json:"sha"`
			Force bool   `json:"force"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)

		// Record the branch's new tip so a later PR merge on this branch can
		// resolve its head to the commit actually pushed to it.
		if body.SHA != "" {
			gitMu.Lock()
			branchRefs[branch] = body.SHA
			gitMu.Unlock()
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"ref": "refs/heads/" + branch,
			"object": map[string]string{
				"sha":  body.SHA,
				"type": "commit",
			},
		})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGitBlobs responds to POST /repos/{owner}/{repo}/git/blobs, creating a
// blob for one file's content. The returned sha is derived from the posted
// content, so the same content always yields the same sha and different
// content yields different shas — enough for a test to tell two blobs apart
// without any server-side counter or clock. The decoded content is recorded
// under that sha (retrievable via recordedBlobFor) so handleContents can
// later serve it back for a path resolved through a written commit's tree.
func handleGitBlobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	sha := deterministicSHA("blob", body.Content)

	content := body.Content
	if body.Encoding == "base64" {
		if decoded, err := base64.StdEncoding.DecodeString(body.Content); err == nil {
			content = string(decoded)
		}
	}
	gitMu.Lock()
	recordedBlobs[sha] = content
	gitMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{"sha": sha})
}

// handleGitCommits routes /repos/{owner}/{repo}/git/commits[/{sha}]:
//
//	GET  git/commits/{sha}  -> the commit's sha and its tree's sha (see
//	                            handleGetCommit)
//	POST git/commits         -> creates a new commit (see handleCreateCommit)
func handleGitCommits(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		handleGetCommit(w, r)
	case http.MethodPost:
		handleCreateCommit(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGetCommit responds to GET /repos/{owner}/{repo}/git/commits/{sha}
// with the requested commit's sha and a deterministic tree sha, so the PR
// creator can use it as the base_tree for the tree it builds on top.
func handleGetCommit(w http.ResponseWriter, r *http.Request) {
	parts := strings.SplitN(r.URL.Path, "/git/commits/", 2)
	sha := stubBaseSHA
	if len(parts) == 2 && parts[1] != "" {
		sha = parts[1]
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"sha": sha,
		"tree": map[string]string{
			"sha": stubBaseTreeSHA,
		},
	})
}

// handleGitTrees responds to POST /repos/{owner}/{repo}/git/trees, building a
// tree from the posted base_tree plus entries. The returned sha is derived
// from the request so identical requests are idempotent, and the request is
// recorded (retrievable via recordedTreeFor) so a test can assert which
// paths and base tree a given tree was built from.
func handleGitTrees(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		BaseTree string      `json:"base_tree"`
		Tree     []treeEntry `json:"tree"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	parts := []string{"tree", body.BaseTree}
	for _, e := range body.Tree {
		parts = append(parts, e.Path, e.Mode, e.Type, e.SHA)
	}
	sha := deterministicSHA(parts...)

	gitMu.Lock()
	recordedTrees[sha] = stubTreeWrite{BaseTree: body.BaseTree, Entries: body.Tree}
	gitMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{"sha": sha})
}

// handleCreateCommit responds to POST /repos/{owner}/{repo}/git/commits,
// creating a commit from the posted message, tree and parents. The returned
// sha is derived from the request so identical requests are idempotent, and
// the request is recorded (retrievable via recordedCommitFor) so a test can
// assert what a given commit carries.
func handleCreateCommit(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Message string   `json:"message"`
		Tree    string   `json:"tree"`
		Parents []string `json:"parents"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	parts := append([]string{"commit", body.Message, body.Tree}, body.Parents...)
	sha := deterministicSHA(parts...)

	gitMu.Lock()
	recordedCommits[sha] = stubCommitWrite{Message: body.Message, Tree: body.Tree, Parents: body.Parents}
	gitMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{"sha": sha})
}

// handleContents responds to GET /repos/{owner}/{repo}/contents/{path}?ref=.
//
// GET with Accept containing "raw" (the agent-remediation single-file read
// path, and the ui pull-request-creator's amend-detection compare) is
// resolved two ways:
//
//   - ref names a commit this stub recorded through the git/blobs, git/trees
//     and git/commits write endpoints (i.e. something was actually pushed to
//     that sha): the path is resolved commit -> tree -> blob and that blob's
//     content is returned. A path absent from that commit's tree is a 404,
//     so ErrSourceNotFound still fires for a genuinely-absent file.
//   - any other ref (including none): the canned ftable_e source is
//     returned, as before — this stub does not carry a real repository
//     history, only whatever was explicitly written to it in a test.
//
// GET with a JSON Accept (the agent-remediation directory-listing read path)
// returns 404, which its caller treats as "nothing more to list" rather than
// an error.
func handleContents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !strings.Contains(r.Header.Get("Accept"), "raw") {
		// JSON-Accept directory listing: no such path in the stub.
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	ref := r.URL.Query().Get("ref")
	_, path, _ := strings.Cut(r.URL.Path, "/contents/")
	if content, knownCommit, found := commitTreeBlob(ref, path); knownCommit {
		if !found {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.github.raw")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(content))
		return
	}

	// Unknown (or absent) ref — the canned single-file fixture, as before.
	w.Header().Set("Content-Type", "application/vnd.github.raw")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(ftableESource))
}

// handleTarball responds to GET /repos/{owner}/{repo}/tarball/{ref} with a
// gzipped tar of the working tree at repoFixtureDir, for every owner, repo,
// and ref alike — the stub holds one repository, not a history.
//
// Every entry is nested under a single top-level directory named
// "{repo}-{ref}", which is the shape GitHub's archive endpoint returns and
// which the reader on the other side relies on: it strips exactly one leading
// path segment from each entry, so a flat archive would extract one directory
// level too high.
//
// A stub with no fixture directory configured answers 404, which the reader
// maps to "repository not available" — a clean, permanent skip rather than a
// hang or a half-written archive.
func handleTarball(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Path shape: /repos/{owner}/{repo}/tarball/{ref}. The ref keeps any
	// slashes it carries (refs/heads/main), so it is taken as the remainder.
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/repos/"), "/", 4)
	if len(parts) < 4 || parts[3] == "" {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	repo, ref := parts[1], strings.ReplaceAll(parts[3], "/", "-")

	if repoFixtureDir == "" {
		http.Error(w, "no repository fixture configured", http.StatusNotFound)
		return
	}
	if _, err := os.Stat(repoFixtureDir); err != nil {
		log.Printf("stub-github: repository fixture %q unreadable: %v", repoFixtureDir, err)
		http.Error(w, "no repository fixture configured", http.StatusNotFound)
		return
	}

	// The archive is assembled in memory before a single byte is written, so a
	// walk that fails part-way answers 500 rather than a truncated gzip stream
	// the reader would report as a corrupt archive.
	body, err := tarballOf(repoFixtureDir, fmt.Sprintf("%s-%s/", repo, ref))
	if err != nil {
		log.Printf("stub-github: build tarball for %s@%s: %v", repo, ref, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// tarballOf walks root and returns a gzipped tar of every directory and
// regular file under it, each entry named prefix + its root-relative path.
// Irregular entries (symlinks, sockets, devices) are skipped: the reader on
// the other side refuses to recreate them anyway.
func tarballOf(root, prefix string) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil // the root itself; prefix already stands in for it
		}
		name := prefix + filepath.ToSlash(rel)
		if d.IsDir() {
			return tw.WriteHeader(&tar.Header{Name: name + "/", Typeflag: tar.TypeDir, Mode: 0o755})
		}
		if !d.Type().IsRegular() {
			return nil
		}
		content, err := os.ReadFile(path) //nolint:gosec // path comes from WalkDir over repoFixtureDir, the operator-configured fixture root, not from the request.
		if err != nil {
			return err
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(content)),
		}); err != nil {
			return err
		}
		_, err = tw.Write(content)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// handlePulls routes /repos/{o}/{r}/pulls[...]:
//
//	GET  pulls?head=&state=  -> PRs matching head branch and state (see listPulls)
//	POST pulls                -> opens a new stub PR (registers lifecycle state)
//	GET  pulls/{n}             -> current PR state (404 when never opened)
//	PUT  pulls/{n}/merge       -> mark merged, resolving merge_commit_sha to
//	                              the head branch's latest written commit
//	PATCH pulls/{n}            -> {"state":"closed"} marks closed without merge
func handlePulls(w http.ResponseWriter, r *http.Request) {
	// Isolate the segment after ".../pulls".
	_, after, _ := strings.Cut(r.URL.Path, "/pulls")
	after = strings.TrimPrefix(after, "/")

	switch {
	case after == "" && r.Method == http.MethodGet:
		listPulls(w, r)

	case after == "" && r.Method == http.MethodPost:
		var body struct {
			Title string `json:"title"`
			Head  string `json:"head"`
			Base  string `json:"base"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)

		prMu.Lock()
		n := nextPRNumber
		nextPRNumber++
		st := &stubPRState{
			Number:    n,
			Title:     body.Title,
			Head:      body.Head,
			Base:      body.Base,
			CreatedAt: time.Now().UTC().Format(time.RFC3339),
		}
		prStates[n] = st
		prMu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(pullJSON(st))

	case strings.HasSuffix(after, "/merge") && r.Method == http.MethodPut:
		n, err := strconv.Atoi(strings.TrimSuffix(after, "/merge"))
		if err != nil {
			http.Error(w, "bad pull number", http.StatusBadRequest)
			return
		}
		prMu.Lock()
		st, ok := prStates[n]
		var head string
		if ok {
			head = st.Head
		}
		prMu.Unlock()
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		// Resolve the merge commit to the head branch's latest written
		// commit; a branch nothing was ever pushed to falls back to the
		// fixed stub sha (back-compat for tests that merge without pushing).
		sha := stubCommitSHA
		if tip, ok := branchTip(head); ok {
			sha = tip
		}

		prMu.Lock()
		if st, ok := prStates[n]; ok {
			st.Closed, st.Merged = true, true
			st.MergeCommitSHA = sha
		}
		prMu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"sha": sha, "merged": true,
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
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(pullJSON(&snapshot))
}

// pullJSON renders st in the shape the GitHub pulls API returns, shared by
// the single-PR read path (writePullJSON), the create response, and the
// head/state list path (listPulls).
func pullJSON(st *stubPRState) map[string]interface{} {
	state := "open"
	var mergedAt, closedAt interface{}
	if st.Closed {
		state = "closed"
		closedAt = stubClosedAt
		if st.Merged {
			mergedAt = stubClosedAt
		}
	}
	createdAt := st.CreatedAt
	if createdAt == "" {
		createdAt = stubClosedAt
	}
	return map[string]interface{}{
		"number":           st.Number,
		"title":            st.Title,
		"state":            state,
		"merged":           st.Merged,
		"merged_at":        mergedAt,
		"closed_at":        closedAt,
		"created_at":       createdAt,
		"html_url":         fmt.Sprintf("http://stub-github/pull/%d", st.Number),
		"merge_commit_sha": st.MergeCommitSHA,
	}
}

// listPulls responds to GET /repos/{o}/{r}/pulls?head=&state=&per_page=,
// filtering the stored PRs by head branch and by state, matching the real
// GitHub pulls-list endpoint the agent-remediation's FindByBranch and the
// ui PR creator's 422-retry both call:
//
//   - head accepts either "branch" or "owner:branch" (the reconciler's
//     FindByBranch always sends "owner:branch"; a bare branch also matches,
//     since a stub PR's own Head is stored without the owner prefix). An
//     empty head matches every PR, same as omitting the parameter on GitHub.
//   - state is "open" (the default, matching GitHub's own default when the
//     parameter is omitted), "closed", or "all".
//
// Results are ordered newest-first (highest PR number first), same as
// GitHub, though in practice at most one PR ever exists per branch here.
func listPulls(w http.ResponseWriter, r *http.Request) {
	head := r.URL.Query().Get("head")
	if _, branch, ok := strings.Cut(head, ":"); ok {
		head = branch
	}
	state := r.URL.Query().Get("state")
	if state == "" {
		state = "open"
	}

	prMu.Lock()
	var matches []stubPRState
	for _, st := range prStates {
		if head != "" && st.Head != head {
			continue
		}
		switch state {
		case "all":
		case "closed":
			if !st.Closed {
				continue
			}
		default: // "open"
			if st.Closed {
				continue
			}
		}
		matches = append(matches, *st)
	}
	prMu.Unlock()

	sort.Slice(matches, func(i, j int) bool { return matches[i].Number > matches[j].Number })

	out := make([]map[string]interface{}, 0, len(matches))
	for i := range matches {
		out = append(out, pullJSON(&matches[i]))
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(out)
}
