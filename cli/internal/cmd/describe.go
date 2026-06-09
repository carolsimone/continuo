package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type describeFlag struct {
	Name      string `json:"name"`
	Shorthand string `json:"shorthand"`
	Usage     string `json:"usage"`
	Default   string `json:"default"`
}

type describeCmd struct {
	Path         string          `json:"path"`
	Short        string          `json:"short"`
	Long         string          `json:"long"`
	Args         []string        `json:"args"`
	Flags        []describeFlag  `json:"flags"`
	Examples     []string        `json:"examples"`
	OutputSchema json.RawMessage `json:"output_schema,omitempty"`
	ExitCodes    json.RawMessage `json:"exit_codes,omitempty"`
}

type describePayload struct {
	Commands []describeCmd `json:"commands"`
}

// NewDescribeCommand builds `continuo describe`, which serializes the cobra
// command tree into a machine-readable catalog for the LLM.
func NewDescribeCommand(cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "describe",
		Short: "Emit a machine-readable catalog of every command",
		Long: `Emit a machine-readable catalog of every command.

Use to discover the entire CLI surface in one call: command paths, purpose,
arguments, flags, examples, output schema, and exit codes.

Arguments: none.

Output (stdout, JSON):
  {"commands":[{"path":string,"short":string,"long":string,"args":[string],
   "flags":[{"name":string,"shorthand":string,"usage":string,"default":string}],
   "examples":[string],"output_schema":object,"exit_codes":[number]}]}

Errors: none under normal use (exit 0).`,
		Example: "  continuo describe",
		Annotations: map[string]string{
			"output_schema": `{"commands":"array"}`,
			"exit_codes":    `[0]`,
		},
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			payload := describePayload{Commands: collectCommands(cmd.Root())}
			if cfg.Human {
				for _, c := range payload.Commands {
					if _, err := fmt.Fprintf(stderr, "%s\t%s\n", c.Path, c.Short); err != nil {
						return err
					}
				}
				return nil
			}
			return output.EmitSuccess(stdout, payload)
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// collectCommands walks the tree and returns every runnable command, sorted by path.
func collectCommands(root *cobra.Command) []describeCmd {
	out := []describeCmd{}
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		if c.Runnable() && c.Name() != "help" {
			out = append(out, toDescribeCmd(root, c))
		}
		for _, child := range c.Commands() {
			walk(child)
		}
	}
	walk(root)
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

func toDescribeCmd(root, c *cobra.Command) describeCmd {
	dc := describeCmd{
		Path:     strings.TrimPrefix(c.CommandPath(), root.Name()+" "),
		Short:    c.Short,
		Long:     c.Long,
		Args:     parseArgs(c.Use),
		Examples: parseExamples(c.Example),
		Flags:    []describeFlag{},
	}
	c.Flags().VisitAll(func(f *pflag.Flag) {
		dc.Flags = append(dc.Flags, describeFlag{
			Name:      f.Name,
			Shorthand: f.Shorthand,
			Usage:     f.Usage,
			Default:   f.DefValue,
		})
	})
	if v, ok := c.Annotations["output_schema"]; ok && json.Valid([]byte(v)) {
		dc.OutputSchema = json.RawMessage(v)
	}
	if v, ok := c.Annotations["exit_codes"]; ok && json.Valid([]byte(v)) {
		dc.ExitCodes = json.RawMessage(v)
	}
	return dc
}

// parseArgs returns the positional-arg spec from a Use string ("status <name>" -> ["<name>"]).
func parseArgs(use string) []string {
	parts := strings.Fields(use)
	if len(parts) <= 1 {
		return []string{}
	}
	return parts[1:]
}

// parseExamples splits a multi-line Example into trimmed, non-empty lines.
func parseExamples(ex string) []string {
	out := []string{}
	for _, line := range strings.Split(ex, "\n") {
		if t := strings.TrimSpace(line); t != "" {
			out = append(out, t)
		}
	}
	return out
}
