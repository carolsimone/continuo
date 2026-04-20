package main

import (
	"os"

	"github.com/carolsimone/continuo/cli/internal/cmd"
)

func main() {
	os.Exit(cmd.Execute())
}
