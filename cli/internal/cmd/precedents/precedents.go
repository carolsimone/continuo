// Package precedents implements `continuo precedents`, a top-level, cross-node
// lookup of how a classified validation failure was rejected and fixed before.
package precedents

import (
	"context"
	"fmt"
	"io"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	orchestratorv1 "github.com/carolsimone/continuo/cli/proto/orchestrator/v1"
	"github.com/spf13/cobra"
)

// orchestratorClientFactory dials and returns a client.OrchestratorClient. In
// production this is defaultOrchestratorFactory; tests pass a closure
// returning a fake.
type orchestratorClientFactory func(ctx context.Context, endpoint string) (client.OrchestratorClient, error)

type proposalPayload struct {
	ProposalID string `json:"proposal_id"`
	PrURL      string `json:"pr_url"`
	PrNumber   int32  `json:"pr_number"`
	PrState    string `json:"pr_state"`
}

type resolvingVersionPayload struct {
	VersionSeq  int64  `json:"version_seq"`
	ContentHash string `json:"content_hash"`
	ReleaseID   string `json:"release_id"`
	Repo        string `json:"repo"`
	CommitSHA   string `json:"commit_sha"`
	PromotedAt  string `json:"promoted_at"`
	RawCode     string `json:"raw_code,omitempty"`
}

type precedentPayload struct {
	ReleaseID               string                   `json:"release_id"`
	NodeID                  string                   `json:"node_id"`
	Stage                   string                   `json:"stage"`
	Category                string                   `json:"category"`
	Reason                  string                   `json:"reason"`
	ErrorExcerpt            string                   `json:"error_excerpt"`
	RejectedAt              string                   `json:"rejected_at"`
	FailingCode             string                   `json:"failing_code,omitempty"`
	Resolved                bool                     `json:"resolved"`
	ResolvingVersion        *resolvingVersionPayload `json:"resolving_version,omitempty"`
	ResolutionDiff          string                   `json:"resolution_diff,omitempty"`
	ResolutionDiffTruncated bool                     `json:"resolution_diff_truncated"`
	Proposals               []proposalPayload        `json:"proposals"`
}

// NewCommand builds `continuo precedents` — top-level: precedent is
// cross-node, global scope.
func NewCommand(cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	return newPrecedentsCommand(defaultOrchestratorFactory, cfg, stdout, stderr)
}

func newPrecedentsCommand(factory orchestratorClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "precedents [signature]",
		Short: "Look up how a classified failure was rejected and fixed before",
		Long: `Look up how a classified failure was rejected and fixed before.

Use when an LLM needs to answer "how was this exact error solved before?" for
a validation rejection: has this same failure signature — or the same
(category, reason) pair — been seen and resolved on a prior release, and if
so, what changed to fix it.

Arguments:
  signature  Optional positional form of --signature. At most one positional
             argument is accepted.

Selectors (precedence: --signature, then the positional signature, then
--category/--reason):
  --signature  The exact classifier signature to match. Wins over a
               positional signature when both are given.
  --category   Together with --reason, matches every rejection sharing that
               (category, reason) pair. Both must be set to use this form.
  --reason     See --category.

Flags:
  --limit         Caps the number of precedents returned, resolved-first then
                  newest. 0 (the default) applies the server default of 5;
                  the server clamps the maximum to 20 — this command does not
                  re-implement that clamp client-side.
  --include-code  Also return the failing candidate's raw code and the
                  resolving version's raw code. The error excerpt and the
                  resolution diff are ALWAYS included regardless of this
                  flag; the diff is capped server-side at 8 KiB with a
                  truncated flag set on any diff that was cut.

Output (stdout, JSON):
  {"precedents":[{"release_id":string,"node_id":string,"stage":string,
   "category":string,"reason":string,"error_excerpt":string,
   "rejected_at":string,"failing_code":string,"resolved":bool,
   "resolving_version":{"version_seq":number,"content_hash":string,
   "release_id":string,"repo":string,"commit_sha":string,
   "promoted_at":string,"raw_code":string},"resolution_diff":string,
   "resolution_diff_truncated":bool,
   "proposals":[{"proposal_id":string,"pr_url":string,"pr_number":number,
   "pr_state":string}]}]}
  failing_code and resolving_version.raw_code are only populated when
  --include-code is set. resolving_version is omitted when resolved is false.
  proposals[].pr_state records the proposal's state when the PR was opened
  (e.g. "open") — it is NOT updated when the PR is later merged or closed, so
  it must not be read as the PR's current state.

An unknown signature — or an unseen (category, reason) pair — is NOT an
error: it returns an empty precedents list with exit 0, because "no
precedent" is a valid answer to the question this command asks.

Errors:
  usage      (exit 2)  more than one positional argument was given, or
                        neither a signature (--signature or positional) nor a
                        complete --category and --reason pair was given
  unavailable(exit 5)  the orchestrator service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo precedents 3f9c…\n  continuo precedents --signature 3f9c…\n  continuo precedents --category logic --reason logic:missing_object --limit 3",
		Annotations: map[string]string{
			"output_schema": `{"precedents":"array"}`,
			"exit_codes":    `[0,2,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("precedents accepts at most one positional argument (signature)"))
			}
			sig := resolveSignature(cmd, args)
			cat, _ := cmd.Flags().GetString("category")
			rsn, _ := cmd.Flags().GetString("reason")
			if sig == "" && (cat == "" || rsn == "") {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("precedents requires --signature, a positional signature, or both --category and --reason"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			sig := resolveSignature(cmd, args)
			cat, _ := cmd.Flags().GetString("category")
			rsn, _ := cmd.Flags().GetString("reason")
			limit, _ := cmd.Flags().GetInt32("limit")
			includeCode, _ := cmd.Flags().GetBool("include-code")

			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.OrchestratorEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.GetPrecedents(ctx, sig, cat, rsn, limit, includeCode)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return humanPrecedents(stderr, resp.GetPrecedents())
			}
			return output.EmitSuccess(stdout, map[string]any{"precedents": toPrecedentPayloads(resp.GetPrecedents())})
		},
	}
	cmd.Flags().String("signature", "", "Exact classifier signature to match; wins over a positional signature and over --category/--reason when both are given")
	cmd.Flags().String("category", "", "Together with --reason, matches rejections sharing this (category, reason) pair")
	cmd.Flags().String("reason", "", "Together with --category, matches rejections sharing this (category, reason) pair")
	cmd.Flags().Int32("limit", 0, "Caps the number of precedents returned; 0 applies the server default of 5, clamped server-side to a max of 20")
	cmd.Flags().Bool("include-code", false, "Also return the failing and resolving code bodies (the error excerpt and resolution diff are always included)")
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// resolveSignature returns the signature selector to use: the --signature
// flag when set, otherwise the sole positional argument, otherwise "" (the
// caller falls back to the --category/--reason selector).
func resolveSignature(cmd *cobra.Command, args []string) string {
	sig, _ := cmd.Flags().GetString("signature")
	if sig != "" {
		return sig
	}
	if len(args) == 1 {
		return args[0]
	}
	return ""
}

func defaultOrchestratorFactory(ctx context.Context, endpoint string) (client.OrchestratorClient, error) {
	return client.NewOrchestratorClient(ctx, endpoint)
}

// emit writes the CLIError envelope (stdout JSON, or stderr in human mode) and
// returns it so cobra's Execute preserves the exit-code contract.
func emit(stdout, stderr io.Writer, human bool, e output.CLIError) error {
	if human {
		_ = output.HumanError(stderr, e)
	} else {
		_ = output.EmitError(stdout, e)
	}
	return e
}

// humanOutput reports whether --human is set, read directly from the command's
// flags. Argument validators run before PersistentPreRunE populates cfg, so a
// pre-RunE usage error must read the flag itself.
func humanOutput(cmd *cobra.Command) bool {
	v, _ := cmd.Flags().GetBool("human")
	return v
}

func toPrecedentPayloads(precedents []*orchestratorv1.Precedent) []precedentPayload {
	out := make([]precedentPayload, 0, len(precedents))
	for _, p := range precedents {
		out = append(out, toPrecedentPayload(p))
	}
	return out
}

func toPrecedentPayload(p *orchestratorv1.Precedent) precedentPayload {
	payload := precedentPayload{
		ReleaseID:               p.GetReleaseId(),
		NodeID:                  p.GetNodeId(),
		Stage:                   p.GetStage(),
		Category:                p.GetCategory(),
		Reason:                  p.GetReason(),
		ErrorExcerpt:            p.GetErrorExcerpt(),
		RejectedAt:              p.GetRejectedAt(),
		FailingCode:             p.GetFailingCode(),
		Resolved:                p.GetResolved(),
		ResolutionDiff:          p.GetResolutionDiff(),
		ResolutionDiffTruncated: p.GetResolutionDiffTruncated(),
		Proposals:               toProposalPayloads(p.GetProposals()),
	}
	if p.GetResolved() && p.GetResolvingVersion() != nil {
		payload.ResolvingVersion = toResolvingVersionPayload(p.GetResolvingVersion())
	}
	return payload
}

func toResolvingVersionPayload(v *orchestratorv1.VersionView) *resolvingVersionPayload {
	return &resolvingVersionPayload{
		VersionSeq:  v.GetVersionSeq(),
		ContentHash: v.GetContentHash(),
		ReleaseID:   v.GetReleaseId(),
		Repo:        v.GetRepo(),
		CommitSHA:   v.GetCommitSha(),
		PromotedAt:  v.GetPromotedAt(),
		RawCode:     v.GetRawCode(),
	}
}

func toProposalPayloads(proposals []*orchestratorv1.PrecedentProposal) []proposalPayload {
	out := make([]proposalPayload, 0, len(proposals))
	for _, pr := range proposals {
		out = append(out, proposalPayload{
			ProposalID: pr.GetProposalId(),
			PrURL:      pr.GetPrUrl(),
			PrNumber:   pr.GetPrNumber(),
			PrState:    pr.GetPrState(),
		})
	}
	return out
}

// humanPrecedents writes one summary line per precedent to stderr:
//
//	<node_id> [<reason>] resolved=<bool> pr=<first pr_url or "-">
func humanPrecedents(stderr io.Writer, precedents []*orchestratorv1.Precedent) error {
	for _, p := range precedents {
		pr := "-"
		if proposals := p.GetProposals(); len(proposals) > 0 {
			pr = proposals[0].GetPrUrl()
		}
		if _, err := fmt.Fprintf(stderr, "%s [%s] resolved=%t pr=%s\n", p.GetNodeId(), p.GetReason(), p.GetResolved(), pr); err != nil {
			return err
		}
	}
	return nil
}
