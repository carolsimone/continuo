// Package config provides shared infrastructure configuration types and loaders.
//
// Shared infra structs (RedisConfig, PostgresConfig, S3Config) live here.
// Service-specific config belongs in each service's own config/ package,
// composing these shared types via their Load*() constructors.
package config
