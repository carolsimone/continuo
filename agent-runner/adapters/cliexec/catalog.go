package cliexec

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"

	"github.com/carolsimone/continuo/agent-runner/service/ports"
)

type describePayload struct {
	Commands []describeCmd `json:"commands"`
}

type describeCmd struct {
	Path     string   `json:"path"`
	Short    string   `json:"short"`
	Long     string   `json:"long"`
	Args     []string `json:"args"`
	Mutating bool     `json:"mutating"`
}

// Catalog implements ports.ToolCatalog from the CLI's self-description.
type Catalog struct {
	tools  []ports.ToolDef
	byName map[string]ports.ToolDef
}

var _ ports.ToolCatalog = (*Catalog)(nil)

// LoadCatalog runs `continuo describe` and builds the catalog from its output.
func LoadCatalog(ctx context.Context, cliPath string) (*Catalog, error) {
	out, err := exec.CommandContext(ctx, cliPath, "describe").Output()
	if err != nil {
		return nil, fmt.Errorf("run %s describe: %w", cliPath, err)
	}
	return NewCatalogFromDescribe(out)
}

// NewCatalogFromDescribe parses a describe JSON document into a tool catalog.
// `describe` itself is excluded: the catalog IS the menu, the model never
// needs to re-read it.
func NewCatalogFromDescribe(raw []byte) (*Catalog, error) {
	var p describePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("parse describe output: %w", err)
	}
	c := &Catalog{byName: map[string]ports.ToolDef{}}
	for _, cmd := range p.Commands {
		if cmd.Path == "describe" {
			continue
		}
		def := ports.ToolDef{
			Name:        strings.ReplaceAll(cmd.Path, " ", "_"),
			Description: cmd.Long,
			Mutating:    cmd.Mutating,
			CLIPath:     strings.Fields(cmd.Path),
		}
		for _, a := range cmd.Args {
			name := strings.Trim(a, "<>[]")
			required := strings.HasPrefix(a, "<")
			def.Params = append(def.Params, ports.ToolParam{
				Name:        name,
				Description: name,
				Required:    required,
			})
			def.ParamOrder = append(def.ParamOrder, name)
		}
		c.tools = append(c.tools, def)
		c.byName[def.Name] = def
	}
	return c, nil
}

func (c *Catalog) Tools() []ports.ToolDef { return c.tools }

func (c *Catalog) Lookup(name string) (ports.ToolDef, bool) {
	d, ok := c.byName[name]
	return d, ok
}
