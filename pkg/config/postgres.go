package config

import "fmt"

// PostgresConfig holds connection parameters for a PostgreSQL instance.
type PostgresConfig struct {
	Host     string
	Port     int
	DB       string
	User     string
	Password string
	SSLMode  string
}

// DSN returns a libpq-style connection string.
func (c PostgresConfig) DSN() string {
	sslMode := c.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		c.Host, c.Port, c.DB, c.User, c.Password, sslMode,
	)
}

// LoadPostgres reads PostgreSQL connection config from standard env vars.
func LoadPostgres() PostgresConfig {
	return PostgresConfig{
		Host:     env("POSTGRES_HOST", "localhost"),
		Port:     envInt("POSTGRES_PORT", 5432),
		DB:       env("POSTGRES_DB", "continuo"),
		User:     env("POSTGRES_USER", "continuo_svc"),
		Password: env("POSTGRES_PASSWORD", ""),
		SSLMode:  env("DB_SSLMODE", "disable"),
	}
}
