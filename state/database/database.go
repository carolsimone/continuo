package database

import (
	"errors"
	"fmt"
	"time"

	pkgConf "github.com/carolsimone/continuo/pkg/config"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// SqlxConfig holds database configuration
type SqlxConfig struct {
	Host           string
	Port           int
	User           string
	Password       string
	DBName         string
	SSLMode        string
	DBInstanceType string
}

func newDbConnection(config *SqlxConfig) (*sqlx.DB, error) {

	connectionString := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.User, config.Password, config.DBName, config.SSLMode,
	)
	db, err := sqlx.Connect(config.DBInstanceType, connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	maxOpenConns := pkgConf.GetEnvIntOrDefault("DB_MAX_OVERFLOW", 10)
	maxIdleConns := pkgConf.GetEnvIntOrDefault("DB_POOL_SIZE", 4)
	db.SetMaxOpenConns(maxOpenConns)
	db.SetMaxIdleConns(maxIdleConns)
	db.SetConnMaxLifetime(2 * time.Minute)

	// Verify it's working
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	return db, nil
}

func GetPostgresConnection() (*sqlx.DB, error) {
	postgresCredsConfig := SqlxConfig{
		Host:           pkgConf.GetEnvOrDefault("POSTGRES_HOST", "localhost"),
		Port:           pkgConf.GetEnvIntOrDefault("POSTGRES_PORT", 5432),
		User:           pkgConf.GetEnvOrDefault("POSTGRES_USER", "postgres"),
		Password:       pkgConf.GetEnvOrDefault("POSTGRES_PASSWORD", ""),
		DBName:         pkgConf.GetEnvOrDefault("POSTGRES_DB", "postgres"),
		SSLMode:        pkgConf.GetEnvOrDefault("DB_SSLMODE", "disable"),
		DBInstanceType: "postgres",
	}
	newDb, err := newDbConnection(&postgresCredsConfig)
	if err != nil {
		return nil, errors.New("database connection failed")
	}
	return newDb, nil

}
