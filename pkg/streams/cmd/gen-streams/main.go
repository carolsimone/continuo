// Command gen-streams reads pkg/streams/contract.yaml and writes
// pkg/streams/streams.gen.go and manifest-controller/streams_contract.py.
package main

import (
	"fmt"
	"os"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gen-streams:", err)
		os.Exit(1)
	}
}

func run() error {
	return fmt.Errorf("not implemented")
}
