// Package loginfiles reads the Redis logins streams-init checks from a
// directory of password files (REDIS_LOGINS_DIR).
package loginfiles

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
)

// Dir is a directory of Redis logins: each file is named after a username and
// holds that login's password, with one trailing newline trimmed. Entries whose
// name starts with "." (the ..data link and timestamped directory of a mounted
// Kubernetes Secret) and directories are not logins; a symbolic link, which is
// how a mounted Secret holds each of its keys, is followed.
type Dir struct {
	path string
}

var _ ports.RedisLogins = (*Dir)(nil)

// New returns the logins of the directory at path.
func New(path string) *Dir { return &Dir{path: path} }

// Logins reads every login of the directory, in username order.
func (d *Dir) Logins(context.Context) ([]ports.RedisLogin, error) {
	entries, err := os.ReadDir(d.path)
	if err != nil {
		return nil, fmt.Errorf("read the Redis logins of REDIS_LOGINS_DIR: %w", err)
	}
	var logins []ports.RedisLogin
	for _, entry := range entries {
		username := entry.Name()
		if strings.HasPrefix(username, ".") {
			continue
		}
		path := filepath.Join(d.path, username)
		info, err := os.Stat(path)
		if err == nil && info.IsDir() {
			continue
		}
		login := ports.RedisLogin{Username: username, ReadErr: err}
		if err == nil {
			login.Password, login.ReadErr = pkgconfig.ReadSecretFile(path)
		}
		logins = append(logins, login)
	}
	return logins, nil
}
