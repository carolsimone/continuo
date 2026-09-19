// Package cmd wires the CLI's top-level cobra command and global flags.
package cmd

import (
	"errors"
	"io"
	"os"

	"github.com/carolsimone/continuo/cli/internal/cmd/node"
	"github.com/carolsimone/continuo/cli/internal/cmd/precedents"
	"github.com/carolsimone/continuo/cli/internal/cmd/schedule"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// Execute parses os.Args, runs the selected command, and returns an exit code.
func Execute() int {
	return executeWith(os.Args[1:], os.Stdout, os.Stderr)
}

func executeWith(args []string, stdout, stderr io.Writer) int {
	in := config.Inputs{
		EnvStateAddr:        os.Getenv("CONTINUO_STATE_ADDR"),
		EnvOrchestratorAddr: os.Getenv("CONTINUO_ORCHESTRATOR_ADDR"),
		EnvTimeout:          os.Getenv("CONTINUO_TIMEOUT"),
		EnvActor:            os.Getenv("CONTINUO_ACTOR"),
	}
	var (
		flagEndpoint             string
		flagOrchestratorEndpoint string
		flagTimeout              string
		flagHuman                bool
	)

	// Shared config pointer. The schedule subcommand and its children capture
	// this pointer at registration time; PersistentPreRunE populates *cfg
	// after flags are parsed and before any RunE fires.
	cfg := &config.Config{}

	root := &cobra.Command{
		Use:           "continuo",
		Short:         "LLM-friendly CLI for Continuo",
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	root.PersistentFlags().StringVar(&flagEndpoint, "endpoint", "", "gRPC address of the state service (env: CONTINUO_STATE_ADDR)")
	root.PersistentFlags().StringVar(&flagOrchestratorEndpoint, "orchestrator-endpoint", "", "gRPC address of the orchestrator service (env: CONTINUO_ORCHESTRATOR_ADDR)")
	root.PersistentFlags().StringVar(&flagTimeout, "timeout", "", "gRPC deadline (env: CONTINUO_TIMEOUT)")
	root.PersistentFlags().BoolVar(&flagHuman, "human", false, "emit human text on stderr instead of JSON on stdout")
	root.PersistentFlags().Bool("json", true, "forward-compat no-op; JSON is the default")

	root.PersistentPreRunE = func(_ *cobra.Command, _ []string) error {
		in.FlagEndpoint = flagEndpoint
		in.FlagOrchestratorEndpoint = flagOrchestratorEndpoint
		in.FlagTimeout = flagTimeout
		in.FlagHuman = flagHuman
		*cfg = config.Resolve(in)
		return nil
	}

	// A value pflag cannot parse (a non-numeric or overflowing --limit) or a
	// flag the command does not define fails before any command's Args
	// validator runs, so it is mapped to the usage envelope here, once for
	// every command. Parsing stopped at the bad flag, so --human is honoured
	// only when it appeared before it; otherwise the JSON envelope is emitted.
	root.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		e := output.NewUsageError(err.Error())
		human, _ := cmd.Flags().GetBool("human")
		if human {
			_ = output.HumanError(stderr, e)
		} else {
			_ = output.EmitError(stdout, e)
		}
		return e
	})

	root.AddCommand(schedule.NewCommand(cfg, stdout, stderr))
	root.AddCommand(node.NewCommand(cfg, stdout, stderr))
	root.AddCommand(precedents.NewCommand(cfg, stdout, stderr))
	root.AddCommand(NewDescribeCommand(cfg, stdout, stderr))

	root.SetArgs(args)
	root.SetOut(stdout)
	root.SetErr(stderr)
	err := root.Execute()
	if err == nil {
		return 0
	}
	var cliErr output.CLIError
	if errors.As(err, &cliErr) {
		return cliErr.ExitCode()
	}
	// Every command and the flag-error handler return a CLIError, so anything
	// else comes from cobra's own dispatch: a command name it cannot resolve.
	// That is rejected before any flag is parsed, so --human is read from the
	// raw arguments rather than from the flag set.
	e := output.NewUsageError(err.Error())
	if rawHumanFlag(args) {
		_ = output.HumanError(stderr, e)
	} else {
		_ = output.EmitError(stdout, e)
	}
	return e.ExitCode()
}

// rawHumanFlag reports whether --human appears among the unparsed arguments.
func rawHumanFlag(args []string) bool {
	for _, a := range args {
		if a == "--human" || a == "--human=true" {
			return true
		}
	}
	return false
}
