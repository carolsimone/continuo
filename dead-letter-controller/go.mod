module github.com/carolsimone/continuo/dead-letter-controller

go 1.26.8

require (
	github.com/carolsimone/continuo/pkg v0.0.0
	github.com/google/uuid v1.6.0
	github.com/jmoiron/sqlx v1.4.0
	github.com/lib/pq v1.12.3
)

replace github.com/carolsimone/continuo/pkg => ../pkg
