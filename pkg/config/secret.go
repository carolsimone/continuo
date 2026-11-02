package config

import (
	"fmt"
	"os"
	"strings"
)

// CredentialVars lists every credential variable a service can read through
// a Secret. Each one is given as the environment variable itself or as a file
// named by <NAME>_FILE.
var CredentialVars = []string{
	"POSTGRES_PASSWORD",
	"REDIS_PASSWORD",
	"NEO4J_PASSWORD",
	"AWS_ACCESS_KEY_ID",
	"AWS_SECRET_ACCESS_KEY",
	"LLM_API_KEY",
	"GITHUB_TOKEN",
	"GITHUB_APP_PRIVATE_KEY",
	"AUTH_OIDC_CLIENT_SECRET",
}

// Secret is one credential, given as the environment variable <Name> or as a
// file named by <Name>_FILE. A Secret is immutable, so Value is safe to call
// from many goroutines.
type Secret struct {
	name string
	// value is the environment value, or the file's content as read at load.
	value string
	// file is the path named by <Name>_FILE, empty when the credential comes
	// from the environment or is unset.
	file string
}

// StaticSecret builds a Secret that holds value, for tests and hand-built
// configs.
func StaticSecret(name, value string) Secret {
	return Secret{name: name, value: value}
}

// Name returns the environment variable the credential is read from.
func (s Secret) Name() string { return s.name }

// IsSet reports whether the credential was given, as a value or as a file.
func (s Secret) IsSet() bool { return s.value != "" || s.file != "" }

// Value returns the credential. A credential given as a file is read again on
// every call, so a rotated file is picked up by the next connection. It
// returns an error when that file can no longer be read or has become empty.
func (s Secret) Value() (string, error) {
	if s.file == "" {
		return s.value, nil
	}
	return readSecretFile(s.file)
}

// RequireSecret reads a credential that must be given. It records a problem
// on v when neither form is set, when both are, or when the file is missing or
// empty; the returned Secret is then not usable.
func (v *Validator) RequireSecret(name string) Secret {
	s := v.loadSecret(name)
	if !s.IsSet() && !v.reported(name) {
		v.Add(name)
	}
	return s
}

// OptionalSecret reads a credential that may be absent. Setting both forms, or
// naming a missing or empty file, is still recorded as a problem on v.
func (v *Validator) OptionalSecret(name string) Secret {
	return v.loadSecret(name)
}

// RequireSecretValue reads a required credential once, at load. It returns ""
// when the problem was recorded on v.
func (v *Validator) RequireSecretValue(name string) string {
	return v.RequireSecret(name).value
}

// OptionalSecretValue reads an optional credential once, at load. It returns
// "" when the credential is unset or the problem was recorded on v.
func (v *Validator) OptionalSecretValue(name string) string {
	return v.OptionalSecret(name).value
}

// loadSecret resolves name from the environment or from <name>_FILE and
// records any problem on v. The Secret it returns is the zero credential
// (named, unset) whenever a problem was recorded.
func (v *Validator) loadSecret(name string) Secret {
	fileVar := name + "_FILE"
	val := os.Getenv(name)
	path := os.Getenv(fileVar)
	switch {
	case val != "" && path != "":
		v.Add(fmt.Sprintf("%s: set %s or %s, not both", name, name, fileVar))
		return Secret{name: name}
	case path != "":
		content, err := readSecretFile(path)
		if err != nil {
			v.Add(fmt.Sprintf("%s: %s", fileVar, err))
			return Secret{name: name}
		}
		return Secret{name: name, value: content, file: path}
	default:
		return Secret{name: name, value: val}
	}
}

// reported reports whether v already holds a problem about name or name_FILE.
func (v *Validator) reported(name string) bool {
	for _, m := range v.missing {
		if strings.HasPrefix(m, name+": ") || strings.HasPrefix(m, name+"_FILE: ") {
			return true
		}
	}
	return false
}

// readSecretFile returns the content of path with one trailing newline ("\n"
// or "\r\n") removed. A file that is empty after that is an error.
func readSecretFile(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	content := strings.TrimSuffix(string(raw), "\n")
	if len(content) != len(raw) {
		content = strings.TrimSuffix(content, "\r")
	}
	if content == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return content, nil
}
