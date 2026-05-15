// Command gen-streams reads pkg/streams/contract.yaml and writes
// pkg/streams/streams.gen.go and manifest-controller/streams_contract.py.
package main

import (
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

type Contract struct {
	Streams []Stream `yaml:"streams"`
}

type Stream struct {
	Name        string     `yaml:"name"`
	Const       string     `yaml:"const"`
	Description string     `yaml:"description"`
	Producers   []string   `yaml:"producers"`
	Consumers   []Consumer `yaml:"consumers"`
}

type Consumer struct {
	Service string `yaml:"service"`
	Group   string `yaml:"group"`
	Const   string `yaml:"const"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gen-streams:", err)
		os.Exit(1)
	}
}

func run() error {
	return fmt.Errorf("not implemented")
}

func parseContract(r io.Reader) (*Contract, error) {
	var c Contract
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	return &c, nil
}
