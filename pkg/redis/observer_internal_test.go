package redis

import (
	"context"
	"fmt"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

type recordingObserver struct {
	mu     sync.Mutex
	events []string
}

func (o *recordingObserver) add(s string)                                 { o.mu.Lock(); o.events = append(o.events, s); o.mu.Unlock() }
func (o *recordingObserver) Watch(stream, group string)                   { o.add("watch:" + stream + "/" + group) }
func (o *recordingObserver) Handled(_, _, result string, _ time.Duration) { o.add("handled:" + result) }
func (o *recordingObserver) DeadLettered(_, _ string, kind model.DeadLetterKind) {
	o.add("dead:" + string(kind))
}
func (o *recordingObserver) Paused(string, string) { o.add("paused") }

func TestObserver_SeesHandlingPausesAndDeadLetters(t *testing.T) {
	rec := &recorder{}
	calls := 0
	c, _ := reliabilityConsumer(t, func(context.Context, goredis.XMessage) error {
		calls++
		if calls == 1 {
			return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
		}
		return fmt.Errorf("%w: bad", events.ErrPermanent)
	}, rec)
	obs := &recordingObserver{}
	c.SetObserver(obs)
	c.processOne(context.Background(), msg("1-0"))
	assert.Equal(t, []string{"handled:infrastructure", "paused", "handled:permanent", "dead:permanent"}, obs.events)
}
