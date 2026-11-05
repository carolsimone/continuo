package db

import (
	"context"
	"crypto/tls"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	"github.com/carolsimone/continuo/pkg/startupwait"
	"github.com/lib/pq"
	"github.com/lib/pq/pqerror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// replyAccept lets the connection in; replyHang reads the startup message and
	// never answers it. Any other script entry is the SQLSTATE of an error reply.
	replyAccept = ""
	replyHang   = "hang"
)

// fakePostgres speaks just enough of the Postgres protocol for the real driver
// to connect and ping, or to receive an error reply during its startup. Each
// connection takes the next entry of the script, and connections past its end
// are accepted.
type fakePostgres struct {
	lis    net.Listener
	mu     sync.Mutex
	script []string
	conns  int
	done   chan struct{}
}

func startFakePostgres(t *testing.T, script ...string) *fakePostgres {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f := &fakePostgres{lis: lis, script: script, done: make(chan struct{})}
	go f.serve()
	t.Cleanup(func() {
		close(f.done)
		_ = lis.Close()
	})
	return f
}

func (f *fakePostgres) port() int { return f.lis.Addr().(*net.TCPAddr).Port }

func (f *fakePostgres) connections() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.conns
}

func (f *fakePostgres) serve() {
	for {
		conn, err := f.lis.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		reply := replyAccept
		if f.conns < len(f.script) {
			reply = f.script[f.conns]
		}
		f.conns++
		f.mu.Unlock()
		go f.handle(conn, reply)
	}
}

func (f *fakePostgres) handle(conn net.Conn, reply string) {
	defer func() { _ = conn.Close() }()
	if !readStartupMessage(conn) {
		return
	}
	switch reply {
	case replyHang:
		<-f.done
	case replyAccept:
		// AuthenticationOk, then ReadyForQuery.
		_, _ = conn.Write(message('R', []byte{0, 0, 0, 0}))
		_, _ = conn.Write(message('Z', []byte{'I'}))
		answerQueries(conn)
	default:
		body := "SFATAL\x00C" + reply + "\x00Mrefused by the fake server\x00\x00"
		_, _ = conn.Write(message('E', []byte(body)))
	}
}

func readStartupMessage(conn net.Conn) bool {
	var size [4]byte
	if _, err := io.ReadFull(conn, size[:]); err != nil {
		return false
	}
	_, err := io.CopyN(io.Discard, conn, int64(binary.BigEndian.Uint32(size[:]))-4)
	return err == nil
}

// answerQueries answers every simple query, which is all a ping sends, with
// EmptyQueryResponse and ReadyForQuery, until the client goes away.
func answerQueries(conn net.Conn) {
	for {
		var header [5]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return
		}
		if _, err := io.CopyN(io.Discard, conn, int64(binary.BigEndian.Uint32(header[1:]))-4); err != nil {
			return
		}
		if header[0] != 'Q' {
			return
		}
		_, _ = conn.Write(message('I', nil))
		_, _ = conn.Write(message('Z', []byte{'I'}))
	}
}

func message(kind byte, body []byte) []byte {
	out := []byte{kind}
	out = binary.BigEndian.AppendUint32(out, uint32(len(body)+4)) //nolint:gosec // a test message is far below 4 GiB.
	return append(out, body...)
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func configFor(port int) pkgconfig.PostgresConfig {
	return pkgconfig.PostgresConfig{Host: "127.0.0.1", Port: port, DB: "d", User: "u", SSLMode: "disable"}
}

var (
	testPool   = pkgconfig.PoolConfig{MaxOpenConns: 2, MaxIdleConns: 1}
	fastPolicy = startupwait.Policy{Timeout: 5 * time.Second, InitialDelay: 10 * time.Millisecond, MaxDelay: 20 * time.Millisecond}
)

// closedPort returns a port nothing listens on, so every dial to it is refused.
func closedPort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := lis.Addr().(*net.TCPAddr).Port
	require.NoError(t, lis.Close())
	return port
}

// A refused connection is what a new pod sees until the NetworkPolicy in front
// of Postgres admits its IP, while Postgres itself is up.
func TestConnect_WaitsUntilARefusedConnectionIsAccepted(t *testing.T) {
	port := closedPort(t)
	started := make(chan *fakePostgres, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			started <- nil
			return
		}
		f := &fakePostgres{lis: lis, done: make(chan struct{})}
		go f.serve()
		started <- f
	}()

	db, err := Connect(context.Background(), configFor(port), testPool, fastPolicy, quietLogger())

	f := <-started
	require.NotNil(t, f, "the port must be free again for the fake server")
	t.Cleanup(func() {
		close(f.done)
		_ = f.lis.Close()
	})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	assert.Equal(t, 2, db.Stats().MaxOpenConnections, "the pool limits apply to the pool Connect returns")
	assert.NoError(t, db.PingContext(context.Background()))
}

func TestConnect_WaitsThroughAServerThatIsStillStarting(t *testing.T) {
	f := startFakePostgres(t, string(pqerror.CannotConnectNow), string(pqerror.TooManyConnections))

	db, err := Connect(context.Background(), configFor(f.port()), testPool, fastPolicy, quietLogger())

	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	assert.GreaterOrEqual(t, f.connections(), 3)
}

func TestConnect_FailsAtOnceOnAnAnswerWaitingCannotFix(t *testing.T) {
	for _, code := range []pqerror.Code{pqerror.InvalidPassword, pqerror.InvalidCatalogName, pqerror.InsufficientPrivilege} {
		t.Run(string(code), func(t *testing.T) {
			f := startFakePostgres(t, string(code))

			db, err := Connect(context.Background(), configFor(f.port()), testPool, fastPolicy, quietLogger())

			require.Error(t, err)
			assert.Nil(t, db)
			var reply *pq.Error
			require.ErrorAs(t, err, &reply)
			assert.Equal(t, code, reply.Code)
			assert.Equal(t, 1, f.connections(), "an answer waiting cannot fix must not be retried")
			assert.Contains(t, err.Error(), fmt.Sprintf("connect to postgres 127.0.0.1:%d/d", f.port()))
		})
	}
}

func TestConnect_GivesUpAtTheBudgetWithTheLastError(t *testing.T) {
	policy := startupwait.Policy{Timeout: 200 * time.Millisecond, InitialDelay: 10 * time.Millisecond, MaxDelay: 20 * time.Millisecond}
	start := time.Now()

	db, err := Connect(context.Background(), configFor(closedPort(t)), testPool, policy, quietLogger())

	require.Error(t, err)
	assert.Nil(t, db)
	var dial *net.OpError
	require.ErrorAs(t, err, &dial, "the last dial error must stay inspectable")
	assert.NotErrorIs(t, err, context.Canceled, "a spent budget must not read as a shutdown")
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond, "the refused dial must be retried")
}

func TestConnect_ReportsAShutdownDuringTheWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	policy := startupwait.Policy{Timeout: time.Minute, InitialDelay: 10 * time.Millisecond, MaxDelay: 20 * time.Millisecond}

	db, err := Connect(ctx, configFor(closedPort(t)), testPool, policy, quietLogger())

	assert.Nil(t, db)
	require.ErrorIs(t, err, context.Canceled)
}

func TestConnect_RefusesAPoolItCannotHonour(t *testing.T) {
	cfg := configFor(closedPort(t))
	cfg.Pool = pkgconfig.PoolConfig{MaxIdleConns: 5}

	_, err := Connect(context.Background(), cfg, testPool, fastPolicy, quietLogger())

	require.ErrorContains(t, err, "DB_MAX_IDLE_CONNS")
}

// A server that accepts the connection and never answers must not use the whole
// budget on one attempt.
func TestConnect_BoundsAnAttemptThatHangs(t *testing.T) {
	f := startFakePostgres(t, replyHang)

	db, err := connect(context.Background(), configFor(f.port()), testPool, fastPolicy, 100*time.Millisecond, quietLogger())

	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	assert.GreaterOrEqual(t, f.connections(), 2)
}

func TestOpen_MakesOneAttempt(t *testing.T) {
	f := startFakePostgres(t, string(pqerror.CannotConnectNow))

	_, err := Open(context.Background(), configFor(f.port()), testPool)

	require.Error(t, err)
	assert.Equal(t, 1, f.connections())
}

func TestIsStartupRetryable(t *testing.T) {
	replyOf := func(code pqerror.Code) error { return &pq.Error{Severity: "FATAL", Code: code} }
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"refused dial", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: connection refused")}, true},
		{"unresolvable host", &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "continuo-pg-rw"}}, true},
		{"attempt timed out", context.DeadlineExceeded, true},
		{"server dropped the connection", io.ErrUnexpectedEOF, true},
		{"server closed the connection", io.EOF, true},
		{"wrapped reply still starting", fmt.Errorf("ping: %w", replyOf(pqerror.CannotConnectNow)), true},
		{"too many connections", replyOf(pqerror.TooManyConnections), true},
		{"wrong password", replyOf(pqerror.InvalidPassword), false},
		{"unknown database", replyOf(pqerror.InvalidCatalogName), false},
		{"unknown role", replyOf(pqerror.InvalidAuthorizationSpecification), false},
		{"unreadable password file", fmt.Errorf("POSTGRES_PASSWORD_FILE: %w", os.ErrNotExist), false},
		{"unparsable connection string", errors.New("parse the postgres connection string: bad"), false},
		{"server without TLS", pq.ErrSSLNotSupported, false},
		{"untrusted certificate", &tls.CertificateVerificationError{Err: errors.New("x509: unknown authority")}, false},
		{"TLS alert from the server", &net.OpError{Op: "remote error", Err: errors.New("tls: bad certificate")}, false},
		{"attempt cut short by a shutdown", context.Canceled, true},
		{"connection unusable", driver.ErrBadConn, true},
	}
	for _, tc := range cases {
		t.Run(strings.ReplaceAll(tc.name, " ", "_"), func(t *testing.T) {
			assert.Equal(t, tc.want, isStartupRetryable(tc.err))
		})
	}
}
