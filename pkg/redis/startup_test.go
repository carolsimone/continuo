package redis

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

func quietStartupLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// dnsErr is what a pod sees while the Redis Service name has not propagated.
func dnsErr() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "continuo-redis", IsNotFound: true}}
}

// replyErr is a Redis error reply, as go-redis surfaces one.
type replyErr string

func (e replyErr) Error() string { return string(e) }
func (replyErr) RedisError()     {}

var _ goredis.Error = replyErr("")

// recordingSleep records the requested pauses instead of sleeping.
type recordingSleep struct{ delays []time.Duration }

func (r *recordingSleep) sleep(_ context.Context, d time.Duration) error {
	r.delays = append(r.delays, d)
	return nil
}

var startupTestPolicy = StartupPolicy{Timeout: time.Minute, InitialDelay: time.Second, MaxDelay: 4 * time.Second}

func assertDelays(t *testing.T, got, want []time.Duration) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("delays = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delays = %v, want %v", got, want)
		}
	}
}

func TestWaitForPing_SucceedsOnceRedisComesUp(t *testing.T) {
	rec := &recordingSleep{}
	calls := 0
	ping := func(context.Context) error {
		calls++
		if calls <= 4 {
			return dnsErr()
		}
		return nil
	}

	if err := waitForPing(context.Background(), ping, startupTestPolicy, rec.sleep, quietStartupLogger()); err != nil {
		t.Fatalf("expected success after redis came up, got %v", err)
	}
	if calls != 5 {
		t.Fatalf("expected 5 attempts, got %d", calls)
	}
	assertDelays(t, rec.delays, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second})
}

// A Redis that is up but still loading its dataset answers PING with LOADING;
// the wait must ride that out rather than treat it as a bad configuration.
func TestWaitForPing_RetriesTransientReplies(t *testing.T) {
	for _, reply := range []string{
		"LOADING Redis is loading the dataset in memory",
		"BUSY Redis is busy running a script",
		"MASTERDOWN Link with MASTER is down",
		"TRYAGAIN Multiple keys request during rehashing of slot",
		"CLUSTERDOWN The cluster is down",
		"ERR max number of clients reached",
	} {
		t.Run(reply, func(t *testing.T) {
			rec := &recordingSleep{}
			calls := 0
			ping := func(context.Context) error {
				calls++
				if calls == 1 {
					return replyErr(reply)
				}
				return nil
			}
			if err := waitForPing(context.Background(), ping, startupTestPolicy, rec.sleep, quietStartupLogger()); err != nil {
				t.Fatalf("expected %q to be retried, got %v", reply, err)
			}
			if calls != 2 {
				t.Fatalf("expected a retry after %q, got %d attempts", reply, calls)
			}
		})
	}
}

func TestWaitForPing_GivesUpAtTimeoutWithTheLastError(t *testing.T) {
	rec := &recordingSleep{}
	last := dnsErr()
	ping := func(context.Context) error { return last }
	policy := StartupPolicy{Timeout: 7 * time.Second, InitialDelay: time.Second, MaxDelay: 4 * time.Second}

	err := waitForPing(context.Background(), ping, policy, rec.sleep, quietStartupLogger())

	if err == nil {
		t.Fatal("expected an error once the timeout elapsed")
	}
	var dns *net.DNSError
	if !errors.As(err, &dns) {
		t.Fatalf("the last dial error must stay inspectable, got %v", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("a spent budget must not read as a shutdown: %v", err)
	}
	// 1s + 2s + 4s = 7s; the next 4s pause would overrun the budget.
	assertDelays(t, rec.delays, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second})
}

// A ping that blocks (a dial that hangs on an unroutable address) must not
// outlive the policy budget: the timeout is wall-clock, not a sum of pauses.
func TestWaitForPing_TimeoutBoundsABlockingPing(t *testing.T) {
	policy := StartupPolicy{Timeout: 150 * time.Millisecond, InitialDelay: 10 * time.Millisecond, MaxDelay: 10 * time.Millisecond}
	ping := func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	start := time.Now()

	err := waitForPing(context.Background(), ping, policy, sleepCtx, quietStartupLogger())

	if err == nil {
		t.Fatal("expected an error once the budget ran out")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("waited %s: a blocking ping outlived the %s budget", elapsed, policy.Timeout)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("a budget timeout must not read as a caller cancel: %v", err)
	}
}

func TestWaitForPing_FailsFastOnAnAnswerWaitingCannotFix(t *testing.T) {
	for _, reply := range []string{
		"WRONGPASS invalid username-password pair or user is disabled.",
		"NOAUTH Authentication required.",
		"ERR unknown command 'PING'",
	} {
		t.Run(reply, func(t *testing.T) {
			rec := &recordingSleep{}
			calls := 0
			bad := replyErr(reply)
			ping := func(context.Context) error { calls++; return bad }

			err := waitForPing(context.Background(), ping, startupTestPolicy, rec.sleep, quietStartupLogger())

			if !errors.Is(err, bad) {
				t.Fatalf("expected the reply error back, got %v", err)
			}
			if calls != 1 || len(rec.delays) != 0 {
				t.Fatalf("%q must not be retried: calls=%d delays=%v", reply, calls, rec.delays)
			}
		})
	}
}

func TestWaitForPing_StopsWhenTheContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	ping := func(context.Context) error { calls++; return dnsErr() }
	cancelling := func(context.Context, time.Duration) error { cancel(); return ctx.Err() }

	err := waitForPing(ctx, ping, startupTestPolicy, cancelling, quietStartupLogger())

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected a single attempt before the cancel, got %d", calls)
	}
}

func TestWaitForPing_CancelledBeforeTheFirstAttemptReturnsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ping := func(ctx context.Context) error { return ctx.Err() }

	err := waitForPing(ctx, ping, startupTestPolicy, sleepCtx, quietStartupLogger())

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// A real go-redis dial failure must be classified as retryable; the fakes
// above cannot prove the classifier recognises what the client returns.
func TestWaitForRedis_RetriesARealDialFailureThenGivesUp(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close() // nothing listens on addr now: every dial is refused

	client := goredis.NewClient(&goredis.Options{Addr: addr, MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	policy := StartupPolicy{Timeout: 300 * time.Millisecond, InitialDelay: 50 * time.Millisecond, MaxDelay: 100 * time.Millisecond}
	start := time.Now()

	err = WaitForRedis(context.Background(), client, policy, quietStartupLogger())

	if err == nil {
		t.Fatal("expected an error for an unreachable redis")
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("returned after %s: the dial failure was not retried", elapsed)
	}
}

// A real Redis error reply, parsed by go-redis, must fail at once: a wrong
// password is not something waiting can fix.
func TestWaitForRedis_FailsFastOnARealAuthReply(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				r := bufio.NewReader(c)
				for {
					// Answer every RESP command line-group with WRONGPASS; the
					// client only needs one reply per command it sends.
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if len(line) > 0 && line[0] == '*' {
						if _, err := c.Write([]byte("-WRONGPASS invalid username-password pair or user is disabled.\r\n")); err != nil {
							return
						}
					}
				}
			}(conn)
		}
	}()

	client := goredis.NewClient(&goredis.Options{Addr: lis.Addr().String(), Password: "wrong", MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	policy := StartupPolicy{Timeout: 5 * time.Second, InitialDelay: time.Second, MaxDelay: time.Second}
	start := time.Now()

	err = WaitForRedis(context.Background(), client, policy, quietStartupLogger())

	if err == nil {
		t.Fatal("expected an auth error")
	}
	var reply goredis.Error
	if !errors.As(err, &reply) {
		t.Fatalf("expected a Redis error reply, got %v", err)
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("returned after %s: an auth rejection was retried", elapsed)
	}
}
