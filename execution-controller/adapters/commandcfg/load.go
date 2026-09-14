package commandcfg

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	stdpath "path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Load builds a Resolver from the dbt-commands.yaml at path. An empty path or
// a missing file yields the built-in plain-dbt defaults. A field this build
// doesn't recognize (e.g. a newer operation key an older rolling-deploy
// binary hasn't caught up to yet) is logged as a warning and ignored, not a
// startup error — the fail-closed guarantee is unchanged, since a block that
// is incomplete once unknown fields are dropped still fails completeness
// validation. A file that exists but otherwise fails to parse or validate
// (malformed YAML, an unrecognised placeholder, an incomplete block, etc.)
// returns an error; the caller treats that as fatal so a real config problem
// surfaces at boot, never mid-release.
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

	var cfg fileConfig
	strictDec := yaml.NewDecoder(bytes.NewReader(raw))
	strictDec.KnownFields(true)
	if decErr := strictDec.Decode(&cfg); decErr != nil && !errors.Is(decErr, io.EOF) {
		fields := unknownFieldNames(decErr)
		if fields == nil {
			// Not exclusively "field X not found" errors — a genuine parse
			// or type problem, which must stay fatal.
			return nil, fmt.Errorf("parse dbt commands config %s: %w", path, decErr)
		}
		logger.Warn("dbt commands config has fields this build does not recognize; ignoring them for forward/backward compatibility across rolling deploys",
			"path", path, "unknown_fields", fields)
		cfg = fileConfig{}
		lenientDec := yaml.NewDecoder(bytes.NewReader(raw))
		if decErr := lenientDec.Decode(&cfg); decErr != nil && !errors.Is(decErr, io.EOF) {
			return nil, fmt.Errorf("parse dbt commands config %s: %w", path, decErr)
		}
	}
	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("dbt commands config %s: %w", path, err)
	}
	return &Resolver{cfg: &cfg}, nil
}

// unknownFieldRe matches one yaml.v3 KnownFields TypeError line, e.g. "line 3:
// field parse not found in type commandcfg.opSet".
var unknownFieldRe = regexp.MustCompile(`^line \d+: field (\S+) not found in type \S+$`)

// unknownFieldNames returns the sorted, de-duplicated field names named in
// err, or nil if err is not EXCLUSIVELY yaml.v3 "known fields" TypeError
// entries — a mix of an unknown-field message and any other decode problem
// (a real type mismatch, malformed YAML, ...) must stay fatal, so a single
// non-matching entry forces a nil result.
func unknownFieldNames(err error) []string {
	var terr *yaml.TypeError
	if !errors.As(err, &terr) || len(terr.Errors) == 0 {
		return nil
	}
	seen := map[string]bool{}
	for _, msg := range terr.Errors {
		m := unknownFieldRe.FindStringSubmatch(msg)
		if m == nil {
			return nil
		}
		seen[m[1]] = true
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
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
			case stdpath.Dir(pp) != stdpath.Dir(mp):
				return fmt.Errorf(
					"%s.compile.partial_parse_path: must live in the same directory as manifest_path (got %q vs %q) — "+
						"the executor mounts the parse-cache volume at that directory in every run/seed/build pod for "+
						"this team's image, which would replace (shadow) it if the two artifacts don't share a directory",
					path, pp, mp)
			}
		}
	}
	if ops.Parse != nil {
		if err := validateParseContext(path, ops); err != nil {
			return err
		}
	}
	return nil
}

// parseAffectingValueFlags is the set of dbt CLI flags whose value dbt folds
// into its partial-parse validity check: a cached partial_parse.msgpack is
// only reused when the invocation that reads it carries the same values dbt
// recorded when it was written.
var parseAffectingValueFlags = map[string]bool{
	"--vars":         true,
	"--target":       true,
	"--profile":      true,
	"--profiles-dir": true,
	"--project-dir":  true,
}

// noPartialParseFlag disables partial parsing outright; its mere presence
// (with no value) is itself parse-affecting.
const noPartialParseFlag = "--no-partial-parse"

// parseContextFlags extracts the parse-affecting flags (and, for the five
// value flags, their values) from a dbt argv template. Both "--flag value"
// and "--flag=value" forms are recognized; the last occurrence of a flag
// wins. A value that is itself an unsubstituted {{ placeholder }} is ignored
// (the flag is treated as absent) — its real value is only known once the
// executor substitutes it per-dispatch, so it can never be compared
// statically against a literal value in the parse argv (which cannot itself
// contain placeholders — validateTemplate rejects that).
func parseContextFlags(argv []string) map[string]string {
	flags := map[string]string{}
	for i := 0; i < len(argv); i++ {
		elem := argv[i]
		if elem == noPartialParseFlag {
			flags[noPartialParseFlag] = "true"
			continue
		}
		if name, value, ok := strings.Cut(elem, "="); ok && parseAffectingValueFlags[name] {
			if !placeholderRe.MatchString(value) {
				flags[name] = value
			}
			continue
		}
		if parseAffectingValueFlags[elem] && i+1 < len(argv) {
			value := argv[i+1]
			i++
			if !placeholderRe.MatchString(value) {
				flags[elem] = value
			}
		}
	}
	return flags
}

// firstDivergentFlag returns the lexicographically-first flag name whose
// value differs between a and b (including one side lacking the flag
// entirely), or "" if the two flag sets are equal. Sorted iteration keeps
// the reported flag deterministic when more than one differs.
func firstDivergentFlag(a, b map[string]string) string {
	seen := make(map[string]bool, len(a)+len(b))
	for k := range a {
		seen[k] = true
	}
	for k := range b {
		seen[k] = true
	}
	names := make([]string, 0, len(seen))
	for k := range seen {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, name := range names {
		av, aok := a[name]
		bv, bok := b[name]
		if aok != bok || av != bv {
			return name
		}
	}
	return ""
}

// validateParseContext requires that every node-dispatch op (run, seed,
// snapshot, test, build, seed_build) in ops carries the exact same
// parse-affecting flags (--vars, --target, --profile, --profiles-dir,
// --project-dir, --no-partial-parse) as ops.Parse. dbt folds these into its
// partial-parse validity check; if a runtime op's set differs from the parse
// argv the compile-time rehearsal exercised, the rehearsal can pass while
// every runtime pod re-parses from scratch (and parse_cache_reason is still
// recorded as hydrated, since the executor never re-checks at dispatch time).
func validateParseContext(path string, ops *opSet) error {
	parseFlags := parseContextFlags(ops.Parse)
	nodeOps := []struct {
		name string
		argv []string
	}{
		{"run", ops.Run}, {"seed", ops.Seed}, {"snapshot", ops.Snapshot},
		{"test", ops.Test}, {"build", ops.Build}, {"seed_build", ops.SeedBuild},
	}
	for _, op := range nodeOps {
		if op.argv == nil {
			continue
		}
		opFlags := parseContextFlags(op.argv)
		if flag := firstDivergentFlag(opFlags, parseFlags); flag != "" {
			opVal, opHas := opFlags[flag]
			parseVal, parseHas := parseFlags[flag]
			return fmt.Errorf(
				"%s.%s: parse-context flag %s does not match %s.parse (%s vs %s) — "+
					"dbt folds this flag into its partial-parse validity check, so a mismatch "+
					"means the compile rehearsal validates a context the runtime dispatch never reproduces",
				path, op.name, flag, path, describeFlag(flag, opVal, opHas), describeFlag(flag, parseVal, parseHas))
		}
	}
	return nil
}

// describeFlag formats a parse-context flag's value for an error message,
// distinguishing "not set" from an explicit (possibly empty) value.
func describeFlag(flag, value string, present bool) string {
	if !present {
		return fmt.Sprintf("%s absent", flag)
	}
	if flag == noPartialParseFlag {
		return flag
	}
	return fmt.Sprintf("%s %q", flag, value)
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
