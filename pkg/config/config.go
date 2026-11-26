// Package config provides shared infrastructure configuration types and loaders.
//
// Shared infra structs (RedisConfig, PostgresConfig, S3Config) live here.
// Component-specific config belongs in each component's own config/ package,
// composing these shared types via their Load*() constructors.
package config
