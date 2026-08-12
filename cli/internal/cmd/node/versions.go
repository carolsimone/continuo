package node

import (
	"context"
	"fmt"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	orchestratorv1 "github.com/carolsimone/continuo/cli/proto/orchestrator/v1"
	"github.com/spf13/cobra"
)

// versionEntry is the JSON-serialisable representation of one recorded code
// version of a node. Provenance fields the write path leaves blank for a
// version with no release stamp (e.g. a backfilled row) are omitted.
type versionEntry struct {
	UniqueID          string `json:"unique_id"`
	VersionSeq        int64  `json:"version_seq"`
	ContentHash       string `json:"content_hash"`
	SourceHash        string `json:"source_hash"`
	SharedCodeHash    string `json:"shared_code_hash"`
	ConfigHash        string `json:"config_hash"`
	Runtime           string `json:"runtime"`
	RawCode           string `json:"raw_code"`
	CompiledCode      string `json:"compiled_code,omitempty"`
	CompiledTruncated bool   `json:"compiled_truncated,omitempty"`
	ConfigJSON        string `json:"config_json"`
	Repo              string `json:"repo,omitempty"`
	CommitSHA         string `json:"commit_sha,omitempty"`
	ReleaseID         string `json:"release_id,omitempty"`
	PromotedAt        string `json:"promoted_at,omitempty"`
	Healed            bool   `json:"healed,omitempty"`
	Backfilled        bool   `json:"backfilled,omitempty"`
	IsCurrent         bool   `json:"is_current,omitempty"`
}

type versionsPayload struct {
	Versions []versionEntry `json:"versions"`
}

// defaultVersionsLimit is the CLI's own default for --limit. The server
// applies the identical default (<= 0 becomes 20) when a caller sends 0, so
// this only exists to put a documented number in --help and in describe's
// output rather than leave the flag looking unbounded.
const defaultVersionsLimit int32 = 20

// NewVersionsCommand builds `continuo node versions <service> <schema> <table>`.
func NewVersionsCommand(factory OrchestratorClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "versions <service> <schema> <table>",
		Short: "List a model node's recorded code-version history",
		Long: `List a model node's recorded code-version history, newest first.

Use when the user wants to see what code a dbt model has run over time: every
recorded version's hashes, the release that promoted it, and the code itself.

Arguments:
  <service>  The owning service name.
  <schema>   The schema name.
  <table>    The table (model) name.

The node is addressed by its unique_id, "<schema>.<table>"; <service> is
accepted for consistency with the other node subcommands but does not appear
in the unique_id, matching how the orchestrator tracks version history.

Flags:
  --limit  Maximum number of versions returned, newest first. Default 20; the
           server also applies this default when sent 0, and clamps any value
           above 200 down to 200.

Output (stdout, JSON):
  {"versions":[{"unique_id":string,"version_seq":number,"content_hash":string,
   "source_hash":string,"shared_code_hash":string,"config_hash":string,
   "runtime":string,"raw_code":string,"compiled_code":string,
   "compiled_truncated":bool,"config_json":string,"repo":string,
   "commit_sha":string,"release_id":string,"promoted_at":string,
   "healed":bool,"backfilled":bool,"is_current":bool}]}
  compiled_code, compiled_truncated, repo, commit_sha, release_id,
  promoted_at, healed, backfilled, and is_current are omitted when the
  version carries no value for them (e.g. a version with no release
  provenance, or one that is not the node's current version).

Errors:
  usage      (exit 2)  wrong number of arguments
  not_found  (exit 3)  the node's unique_id is not recorded
  unavailable(exit 5)  the orchestrator service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo node versions finance analytics orders --limit 5",
		Annotations: map[string]string{
			"output_schema": `{"versions":"array"}`,
			"exit_codes":    `[0,2,3,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 3 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("versions requires exactly three arguments: <service> <schema> <table>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			schema, table := args[1], args[2]
			uniqueID := schema + "." + table
			limit, _ := cmd.Flags().GetInt32("limit")

			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.OrchestratorEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.GetNodeVersions(ctx, uniqueID, limit)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return humanVersions(stderr, resp.GetVersions())
			}
			return output.EmitSuccess(stdout, toVersionsPayload(resp.GetVersions()))
		},
	}
	cmd.Flags().Int32("limit", defaultVersionsLimit, "Maximum number of versions returned, newest first (default 20, server max 200)")
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

func toVersionEntry(v *orchestratorv1.VersionView) versionEntry {
	return versionEntry{
		UniqueID:          v.GetUniqueId(),
		VersionSeq:        v.GetVersionSeq(),
		ContentHash:       v.GetContentHash(),
		SourceHash:        v.GetSourceHash(),
		SharedCodeHash:    v.GetSharedCodeHash(),
		ConfigHash:        v.GetConfigHash(),
		Runtime:           v.GetRuntime(),
		RawCode:           v.GetRawCode(),
		CompiledCode:      v.GetCompiledCode(),
		CompiledTruncated: v.GetCompiledTruncated(),
		ConfigJSON:        v.GetConfigJson(),
		Repo:              v.GetRepo(),
		CommitSHA:         v.GetCommitSha(),
		ReleaseID:         v.GetReleaseId(),
		PromotedAt:        v.GetPromotedAt(),
		Healed:            v.GetHealed(),
		Backfilled:        v.GetBackfilled(),
		IsCurrent:         v.GetIsCurrent(),
	}
}

func toVersionsPayload(versions []*orchestratorv1.VersionView) versionsPayload {
	out := make([]versionEntry, 0, len(versions))
	for _, v := range versions {
		out = append(out, toVersionEntry(v))
	}
	return versionsPayload{Versions: out}
}

// humanVersions writes one line per version to stderr:
//
//	<version_seq>  <content_hash>  <promoted_at>  [current]
func humanVersions(stderr io.Writer, versions []*orchestratorv1.VersionView) error {
	for _, v := range versions {
		current := ""
		if v.GetIsCurrent() {
			current = "  [current]"
		}
		if _, err := fmt.Fprintf(stderr, "%d  %s  %s%s\n", v.GetVersionSeq(), v.GetContentHash(), v.GetPromotedAt(), current); err != nil {
			return err
		}
	}
	return nil
}
