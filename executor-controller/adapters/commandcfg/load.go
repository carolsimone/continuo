package commandcfg

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Load builds a Resolver from the dbt-commands.yaml at path. An empty path or
// a missing file yields the built-in plain-dbt defaults. A file that exists
// but fails to parse or validate returns an error; the caller treats that as
// fatal so a config typo surfaces at boot, never mid-release.
func Load(path string, logger *slog.Logger) (*Resolver, error) {
	if path == "" {
		logger.Info("DBT_COMMANDS_CONFIG_PATH not set, using built-in dbt commands")
		return Defaults(), nil
	}
	raw, err := os.ReadFile(filepath.Clean(path)) //nolint:gosec // G304: path comes from DBT_COMMANDS_CONFIG_PATH, a trusted operator-set config path, not external/user input.
	if errors.Is(err, os.ErrNotExist) {
		logger.Info("dbt commands config not found, using built-in dbt commands", "path", path)
		return Defaults(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read dbt commands config %s: %w", path, err)
	}

	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	var cfg fileConfig
	if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("parse dbt commands config %s: %w", path, err)
	}
	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("dbt commands config %s: %w", path, err)
	}
	return &Resolver{cfg: &cfg}, nil
}

// validate enforces the config schema: the default block is required and
// complete, every service override is complete, and each present template
// passes the per-template checks (non-empty argv, required/allowed placeholders,
// absolute literal compile manifest_path). Per-template checks run before the
// completeness check so a malformed present key reports its specific error.
func validate(cfg *fileConfig) error {
	if cfg.Default == nil {
		return fmt.Errorf("default: required and must define every command")
	}
	if err := validateOpSet("default", cfg.Default); err != nil {
		return err
	}
	if err := requireComplete("default", cfg.Default); err != nil {
		return err
	}
	for name, ops := range cfg.Services {
		if ops == nil {
			return fmt.Errorf("services.%s: required and must define every command", name)
		}
		if err := validateOpSet("services."+name, ops); err != nil {
			return err
		}
		if err := requireComplete("services."+name, ops); err != nil {
			return err
		}
	}
	return nil
}

// requireComplete reports the first incompleteness: the missing required keys.
func requireComplete(path string, ops *opSet) error {
	if missing := ops.missingKeys(); len(missing) > 0 {
		return fmt.Errorf("%s: incomplete command set, missing %v", path, missing)
	}
	return nil
}

func validateOpSet(path string, ops *opSet) error {
	nodeOps := []struct {
		name string
		argv []string
	}{
		{"run", ops.Run}, {"seed", ops.Seed}, {"snapshot", ops.Snapshot},
		{"test", ops.Test}, {"build", ops.Build},
	}
	for _, op := range nodeOps {
		if op.argv == nil {
			continue
		}
		if err := validateTemplate(path+"."+op.name, op.argv,
			map[string]bool{"node": true}, []string{"node"}); err != nil {
			return err
		}
	}
	if ops.SeedBuild != nil {
		if err := validateTemplate(path+".seed_build", ops.SeedBuild,
			map[string]bool{"node": true, "target_schema": true}, []string{"node"}); err != nil {
			return err
		}
	}
	if ops.Parse != nil {
		if err := validateTemplate(path+".parse", ops.Parse,
			map[string]bool{}, nil); err != nil {
			return err
		}
	}
	if ops.Compile != nil {
		if err := validateTemplate(path+".compile.command", ops.Compile.Command,
			map[string]bool{}, nil); err != nil {
			return err
		}
		mp := ops.Compile.ManifestPath
		switch {
		case mp == "":
			return fmt.Errorf("%s.compile.manifest_path: required", path)
		case placeholderRe.MatchString(mp):
			return fmt.Errorf("%s.compile.manifest_path: placeholders are not allowed", path)
		case !strings.HasPrefix(mp, "/"):
			return fmt.Errorf("%s.compile.manifest_path: must be an absolute path, got %q", path, mp)
		}
		if pp := ops.Compile.PartialParsePath; pp != "" {
			switch {
			case placeholderRe.MatchString(pp):
				return fmt.Errorf("%s.compile.partial_parse_path: placeholders are not allowed", path)
			case !strings.HasPrefix(pp, "/"):
				return fmt.Errorf("%s.compile.partial_parse_path: must be an absolute path, got %q", path, pp)
			}
		}
	}
	return nil
}

// validateTemplate checks one argv template: non-empty, every placeholder in
// the allowed set, every required placeholder present.
func validateTemplate(path string, argv []string, allowed map[string]bool, required []string) error {
	if len(argv) == 0 {
		return fmt.Errorf("%s: must not be empty", path)
	}
	found := map[string]bool{}
	for _, elem := range argv {
		for _, m := range placeholderRe.FindAllStringSubmatch(elem, -1) {
			name := m[1]
			if !allowed[name] {
				return fmt.Errorf("%s: unknown placeholder {{ %s }}", path, name)
			}
			found[name] = true
		}
	}
	for _, req := range required {
		if !found[req] {
			return fmt.Errorf("%s: missing required {{ %s }} placeholder", path, req)
		}
	}
	return nil
}
