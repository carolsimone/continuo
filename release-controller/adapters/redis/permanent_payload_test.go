package redis

import (
	"context"
	"io"
	"log/slog"
	"testing"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// A payload no redelivery can decode must reach the consumer as a permanent
// error, so it is dead-lettered instead of silently acknowledged.
func TestHandlers_UndecodablePayloadIsPermanent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handlers := map[string]pkgredis.MessageHandler{
		"manifest_loaded_candidate": newManifestLoadedCandidateHandler(nil, logger),
		"validation_result":         newValidationResultHandler(nil, logger),
		"seed_build_completed":      newSeedBuildCompletedHandler(nil, logger),
		"compile_completed":         newCompileCompletedHandler(nil, logger),
	}
	messages := map[string]goredis.XMessage{
		"missing payload": {ID: "1-0", Values: map[string]any{}},
		"undecodable":     {ID: "2-0", Values: map[string]any{"payload": "{not json"}},
	}
	for hname, h := range handlers {
		for mname, m := range messages {
			err := h(context.Background(), m)
			assert.ErrorIs(t, err, pkgevents.ErrPermanent, "%s / %s", hname, mname)
		}
	}
	unknownKind := goredis.XMessage{ID: "3-0", Values: map[string]any{"payload": `{"kind":"nope"}`}}
	assert.ErrorIs(t, newValidationResultHandler(nil, logger)(context.Background(), unknownKind), pkgevents.ErrPermanent)
}

// A validation.result message whose kind is known but whose body does not
// decode into that kind's input is permanent too.
func TestValidationResultHandler_UndecodableKindBodyIsPermanent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newValidationResultHandler(nil, logger)
	for _, kind := range []string{"node", "complete"} {
		// A JSON string where the input struct expects an object field.
		msg := goredis.XMessage{ID: "4-0", Values: map[string]any{
			"payload": `{"kind":"` + kind + `","release_id":["not","a","string"]}`,
		}}
		assert.ErrorIs(t, h(context.Background(), msg), pkgevents.ErrPermanent, "kind %s", kind)
	}
}
