package repofs

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// maxProjectFileBytes caps a file the model search reads: a dbt model or a
// properties file is small, so a larger one is generated output and skipped.
const maxProjectFileBytes = 1 << 20

// skippedProjectDirs are the directories of a dbt project that hold dbt's own
// output or installed packages, never the project's own models.
var skippedProjectDirs = map[string]bool{"target": true, "dbt_packages": true, "logs": true}

// ModelReferenceFinder reads a dbt project from a repository checkout on disk.
type ModelReferenceFinder struct{}

var _ ports.ModelReferenceFinder = ModelReferenceFinder{}

// propertiesFile is the part of a dbt properties file the search reads: each
// models entry's name and its meta, written directly or under config.
type propertiesFile struct {
	Models []struct {
		Name   string         `yaml:"name"`
		Meta   map[string]any `yaml:"meta"`
		Config struct {
			Meta map[string]any `yaml:"meta"`
		} `yaml:"config"`
	} `yaml:"models"`
}

// Find walks the project at projectDir under rootDir. A .sql or .py file names
// a model by its base name, and a .sql file that refs model is a reference.
// A .yml or .yaml file is read as a dbt properties file: each models entry
// names a model, the one named model is a reference, and its meta, or its
// config.meta, says whether the model declares continuo.entity or
// continuo.grain. A file that does not parse as YAML, or whose models is not
// a list, is not a properties file and is skipped. Hidden directories and
// dbt's own target, dbt_packages and logs are not walked.
func (ModelReferenceFinder) Find(rootDir, projectDir, model string) (ports.ModelReferences, error) {
	refs := ports.ModelReferences{Files: map[string]string{}, ModelNames: map[string]bool{}}
	start := filepath.Join(rootDir, filepath.FromSlash(projectDir))
	info, err := os.Stat(start)
	if err != nil {
		return refs, fmt.Errorf("dbt project %s: %w", projectDir, err)
	}
	if !info.IsDir() {
		return refs, fmt.Errorf("dbt project %s is not a directory", projectDir)
	}
	err = filepath.WalkDir(start, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != start && (strings.HasPrefix(d.Name(), ".") || skippedProjectDirs[d.Name()]) {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(d.Name()))
		switch ext {
		case ".sql", ".py":
			refs.ModelNames[strings.TrimSuffix(d.Name(), filepath.Ext(d.Name()))] = true
		case ".yml", ".yaml":
		default:
			return nil
		}
		text, ok, err := readSmall(path)
		if err != nil || !ok {
			return err
		}
		rel, err := filepath.Rel(rootDir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if ext == ".sql" {
			if remediation.RefsModel(text, model) {
				refs.Files[rel] = text
			}
			return nil
		}
		if ext == ".py" {
			return nil
		}
		var props propertiesFile
		if yaml.Unmarshal([]byte(text), &props) != nil {
			return nil
		}
		for _, m := range props.Models {
			if m.Name == "" {
				continue
			}
			refs.ModelNames[m.Name] = true
			kind := declaresKind(m.Meta) || declaresKind(m.Config.Meta)
			if m.Name == model {
				refs.Files[rel] = text
				refs.DeclaresKind = refs.DeclaresKind || kind
			} else if kind {
				refs.OthersDeclareKind = true
			}
		}
		return nil
	})
	if err != nil {
		return refs, fmt.Errorf("walk dbt project %s: %w", projectDir, err)
	}
	for p := range refs.Files {
		refs.Order = append(refs.Order, p)
	}
	sort.Strings(refs.Order)
	return refs, nil
}

// declaresKind reports whether a meta block's continuo map sets entity or
// grain.
func declaresKind(meta map[string]any) bool {
	continuo, ok := meta["continuo"].(map[string]any)
	if !ok {
		return false
	}
	return continuo["entity"] != nil || continuo["grain"] != nil
}

// readSmall reads a file of at most maxProjectFileBytes; ok is false for a
// larger one, which is skipped.
func readSmall(path string) (string, bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", false, nil
		}
		return "", false, err
	}
	if info.Size() > maxProjectFileBytes {
		return "", false, nil
	}
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is produced by filepath.WalkDir over the caller's repository checkout.
	if err != nil {
		return "", false, err
	}
	return string(b), true, nil
}
