// Package main is a minimal GitHub Contents API stub used in e2e tests.
// It listens on :9200 and handles GET /repos/{owner}/{repo}/contents/{path}
// so the remediation-agent's GitHub source-reader adapter can be exercised
// without a real GitHub token or network access.
//
// Any contents path returns the canned dbt source for ftable_e: the model
// that references public.wrong_name (the deliberately-broken join). The e2e
// test only needs one model, so a single canned response is sufficient.
//
// Content-Type mirrors the GitHub raw-content header the production adapter
// requests via Accept: application/vnd.github.raw+json.
package main

import (
	"log"
	"net/http"
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

func main() {
	http.HandleFunc("/repos/", handleContents)
	log.Println("stub-github: listening on :9200")
	if err := http.ListenAndServe(":9200", nil); err != nil {
		log.Fatalf("stub-github: %v", err)
	}
}

// handleContents responds to GET /repos/{owner}/{repo}/contents/{path...} with
// the canned ftable_e source. Any path is accepted; the e2e only exercises one
// failing node, so a single canned body is correct.
func handleContents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/vnd.github.raw")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(ftableESource))
}
