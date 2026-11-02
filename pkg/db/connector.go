package db

import (
	"context"
	"database/sql/driver"
	"fmt"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/lib/pq"
)

// connector opens every new pool connection with the password the config
// holds at that moment, so a rotated POSTGRES_PASSWORD_FILE reaches the next
// connection the pool opens without a restart.
type connector struct {
	// dsn is the connection string without the password.
	dsn      string
	password pkgconfig.Secret
}

var _ driver.Connector = connector{}

func newConnector(cfg pkgconfig.PostgresConfig) connector {
	return connector{dsn: cfg.DSNWithoutPassword(), password: cfg.Password}
}

// Connect reads the password, parses the connection string and opens one
// connection. Its errors never carry the password.
func (c connector) Connect(ctx context.Context) (driver.Conn, error) {
	pw, err := c.password.Value()
	if err != nil {
		return nil, fmt.Errorf("%s_FILE: %w", c.password.Name(), err)
	}
	pc, err := pq.NewConfig(c.dsn)
	if err != nil {
		return nil, fmt.Errorf("parse the postgres connection string: %w", err)
	}
	pc.Password = pw
	pqc, err := pq.NewConnectorConfig(pc)
	if err != nil {
		return nil, fmt.Errorf("build the postgres connector: %w", err)
	}
	return pqc.Connect(ctx)
}

// Driver returns the pq driver.
func (connector) Driver() driver.Driver { return &pq.Driver{} }
