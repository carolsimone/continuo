package ports

import "context"

// RedisLogin is one Redis login streams-init signs in as: its username and
// password, or, in ReadErr, why its password could not be read.
type RedisLogin struct {
	Username string
	Password string
	ReadErr  error
}

// RedisLogins lists the Redis logins streams-init signs in as before it
// creates any stream.
type RedisLogins interface {
	// Logins returns every login in username order. A login whose password
	// cannot be read is listed with ReadErr set; the error is for a list that
	// cannot be read at all.
	Logins(ctx context.Context) ([]RedisLogin, error)
}
