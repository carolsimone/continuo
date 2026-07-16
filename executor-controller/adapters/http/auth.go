package http

import (
	"errors"
	"net/http"
	"strings"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/workerapi"
)

// poolKeyHeader names the pool a caller claims to be. The credential proves it.
const poolKeyHeader = "X-Continuo-Pool-Key"

// bearerScheme is the one authorization scheme the worker API accepts.
const bearerScheme = "bearer"

// maxLoggedPoolKeyBytes bounds how much of a caller-supplied pool key reaches a
// log line. A rejected caller has proved nothing, so the key it names is
// arbitrary text of its choosing; logging it whole would let an
// unauthenticated caller write as much into the executor's logs as it can send.
const maxLoggedPoolKeyBytes = 128

// poolHandler is a handler that only runs for an authenticated pool. The pool
// is a parameter rather than a context value, so a handler cannot be written
// that forgets to authenticate: there is no way to build one without a pool.
type poolHandler func(w http.ResponseWriter, r *http.Request, pool *model.WorkerPool)

// authenticate rejects every caller that does not present a registered pool's
// credential, and hands the handler the pool the caller proved it is.
//
// The raw credential stops here: it is compared against the pool's stored digest
// and never logged, echoed, or passed on.
func (s *Server) authenticate(next poolHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		poolKey := r.Header.Get(poolKeyHeader)
		credential, ok := bearerToken(r.Header.Get("Authorization"))
		if poolKey == "" || !ok {
			s.writeError(w, r, http.StatusUnauthorized, "unauthenticated",
				"a registered pool credential is required")
			return
		}

		pool, err := s.auth.Authenticate(r.Context(), poolKey, credential)
		if errors.Is(err, workerapi.ErrUnauthenticated) {
			// The pool key is safe to log; the credential is not, and is not
			// what this line reports.
			s.logger.Warn("rejected a worker request with no valid pool credential",
				"pool_key", loggedPoolKey(poolKey), "path", r.URL.Path)
			s.writeError(w, r, http.StatusUnauthorized, "unauthenticated",
				"a registered pool credential is required")
			return
		}
		if err != nil {
			// The pool could not be looked up at all. That is the executor's
			// fault, not the caller's, and the caller may retry.
			s.logger.Error("could not authenticate a worker request",
				"pool_key", loggedPoolKey(poolKey), "path", r.URL.Path, "error", err)
			s.writeError(w, r, http.StatusInternalServerError, "internal",
				"the request could not be authenticated")
			return
		}

		next(w, r, pool)
	}
}

// loggedPoolKey renders a pool key a caller supplied but has not proved, cut to
// a bounded length so no one request can flood the log with a single header.
func loggedPoolKey(poolKey string) string {
	if len(poolKey) <= maxLoggedPoolKeyBytes {
		return poolKey
	}
	return poolKey[:maxLoggedPoolKeyBytes] + "...(truncated)"
}

// bearerToken extracts the credential from an Authorization header. The scheme
// is matched case-insensitively per RFC 7235; anything else, or an empty
// credential, is not a bearer token.
func bearerToken(header string) (string, bool) {
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, bearerScheme) {
		return "", false
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return "", false
	}
	return token, true
}
