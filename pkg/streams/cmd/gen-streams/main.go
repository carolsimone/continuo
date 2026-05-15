// Command gen-streams reads pkg/streams/contract.yaml and writes
// pkg/streams/streams.gen.go and manifest-controller/streams_contract.py.
package main

import (
	"fmt"
	"io"
	"os"
	"regexp"

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

func loadAndValidate(r io.Reader) (*Contract, error) {
	c, err := parseContract(r)
	if err != nil {
		return nil, err
	}
	if err := validate(c); err != nil {
		return nil, err
	}
	return c, nil
}

func validate(c *Contract) error {
	groupRe := regexp.MustCompile(`^[a-z][a-z0-9-]+$`)
	identRe := regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	knownServices := map[string]struct{}{
		"state":               {},
		"orchestrator":        {},
		"executor-controller": {},
		"k8s-controller":      {},
		"manifest-controller": {},
	}

	streamNames := map[string]int{}
	streamConsts := map[string]int{}
	groupNames := map[string]int{}
	groupConsts := map[string]int{}

	for i, s := range c.Streams {
		if !identRe.MatchString(s.Const) {
			return fmt.Errorf("stream %q: const %q is not a valid identifier", s.Name, s.Const)
		}
		if prev, ok := streamNames[s.Name]; ok {
			return fmt.Errorf("duplicate stream name %q at index %d (first seen at %d)", s.Name, i, prev)
		}
		streamNames[s.Name] = i
		if prev, ok := streamConsts[s.Const]; ok {
			return fmt.Errorf("duplicate stream const %q at index %d (first seen at %d)", s.Const, i, prev)
		}
		streamConsts[s.Const] = i

		for j, cons := range s.Consumers {
			if _, ok := knownServices[cons.Service]; !ok {
				return fmt.Errorf("stream %q consumer[%d]: unknown service %q", s.Name, j, cons.Service)
			}
			if !groupRe.MatchString(cons.Group) {
				return fmt.Errorf("stream %q consumer[%d]: group %q must match %s", s.Name, j, cons.Group, groupRe.String())
			}
			if !identRe.MatchString(cons.Const) {
				return fmt.Errorf("stream %q consumer[%d]: const %q is not a valid identifier", s.Name, j, cons.Const)
			}
			if prev, ok := groupNames[cons.Group]; ok {
				return fmt.Errorf("duplicate consumer group %q on stream %q[%d] (first seen at stream index %d)", cons.Group, s.Name, j, prev)
			}
			groupNames[cons.Group] = i
			if prev, ok := groupConsts[cons.Const]; ok {
				return fmt.Errorf("duplicate consumer const %q on stream %q[%d] (first seen at stream index %d)", cons.Const, s.Name, j, prev)
			}
			groupConsts[cons.Const] = i
		}
	}
	return nil
}
