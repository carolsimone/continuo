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

// chatFlagsAnnotation names the cobra annotation in which a command lists, comma
// separated, the flags the chat may set on it. Every other flag stays out of
// the chat's reach.
const chatFlagsAnnotation = "chat_flags"

type describeFlag struct {
	Name      string `json:"name"`
	Shorthand string `json:"shorthand"`
	Usage     string `json:"usage"`
	Default   string `json:"default"`
	Chat      bool   `json:"chat,omitempty"`
	Repeated  bool   `json:"repeated,omitempty"`
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
	Mutating     bool            `json:"mutating,omitempty"`
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
arguments, flags, examples, output schema, and exit codes. A flag marked
"chat" is one the chat may set on that command; a flag marked "repeated" may
be given more than once. Both are left out when false.

Arguments: none.

Output (stdout, JSON):
  {"commands":[{"path":string,"short":string,"long":string,"args":[string],
   "flags":[{"name":string,"shorthand":string,"usage":string,"default":string,
             "chat":bool,"repeated":bool}],
   "examples":[string],"output_schema":object,"exit_codes":[number]}]}

Errors:
  usage (exit 2)  unexpected arguments`,
		Example: "  continuo describe",
		Annotations: map[string]string{
			"output_schema": `{"commands":"array"}`,
			"exit_codes":    `[0,2]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				e := output.NewUsageError("describe takes no arguments")
				// Args validation runs before cfg is populated, so read --human directly.
				human, _ := cmd.Flags().GetBool("human")
				if human {
					_ = output.HumanError(stderr, e)
				} else {
					_ = output.EmitError(stdout, e)
				}
				return e
			}
			return nil
		},
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
// Cobra's auto-generated framework commands (help, completion) and any hidden commands
// and their subtrees are skipped: they carry no documentation standard and are not part
// of the LLM-facing CLI surface. Real commands are NOT filtered by annotation presence,
// so the documentation-standard test can still catch an undocumented first-class command.
func collectCommands(root *cobra.Command) []describeCmd {
	out := []describeCmd{}
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		if c.Name() == "help" || c.Name() == "completion" || c.Hidden {
			return
		}
		// A group command (schedule, node) is runnable only so that an unknown
		// subcommand name is reported through the usage envelope; it is not a
		// command an LLM invokes, so it is walked for its children and not listed.
		if c.Runnable() && !c.HasSubCommands() {
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
	// Surface the command's full flag surface: local flags plus persistent
	// flags inherited from ancestors (e.g. the global --endpoint/--timeout/--human
	// on the root). c.Flags() alone omits inherited flags on an unexecuted command.
	// Only the command's own flags can be chat flags; the inherited global
	// flags never are.
	chat := chatFlags(c)
	seen := map[string]bool{}
	addFlag := func(local bool) func(*pflag.Flag) {
		return func(f *pflag.Flag) {
			if seen[f.Name] {
				return
			}
			seen[f.Name] = true
			_, repeated := f.Value.(pflag.SliceValue)
			dc.Flags = append(dc.Flags, describeFlag{
				Name:      f.Name,
				Shorthand: f.Shorthand,
				Usage:     f.Usage,
				Default:   f.DefValue,
				Chat:      local && chat[f.Name],
				Repeated:  repeated,
			})
		}
	}
	c.LocalFlags().VisitAll(addFlag(true))
	c.InheritedFlags().VisitAll(addFlag(false))
	if v, ok := c.Annotations["output_schema"]; ok && json.Valid([]byte(v)) {
		dc.OutputSchema = json.RawMessage(v)
	}
	if v, ok := c.Annotations["exit_codes"]; ok && json.Valid([]byte(v)) {
		dc.ExitCodes = json.RawMessage(v)
	}
	if c.Annotations["mutating"] == "true" {
		dc.Mutating = true
	}
	return dc
}

// chatFlags returns the flag names a command lists in its chat_flags annotation.
func chatFlags(c *cobra.Command) map[string]bool {
	out := map[string]bool{}
	raw, ok := c.Annotations[chatFlagsAnnotation]
	if !ok {
		return out
	}
	for _, name := range strings.Split(raw, ",") {
		if name = strings.TrimSpace(name); name != "" {
			out[name] = true
		}
	}
	return out
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
