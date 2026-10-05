package grpc

import (
	"time"

	deadletterv1 "github.com/carolsimone/continuo/dead-letter-controller/api/deadletter/v1"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/deadletter"
)

// ts renders t as an RFC 3339 string in UTC.
func ts(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func toProto(dl deadletter.DeadLetter) *deadletterv1.DeadLetter {
	out := &deadletterv1.DeadLetter{
		Id: dl.ID.String(), Source: string(dl.Source), FailureKind: string(dl.FailureKind), Stream: dl.Stream,
		ConsumerGroup: dl.Group, OriginalMessageId: dl.OriginalMessageID, Producer: dl.Producer, Error: dl.Error,
		DeliveryCount: dl.DeliveryCount, OriginalAt: ts(dl.OriginalAt), RecordedAt: ts(dl.RecordedAt),
		ExpiresAt: ts(dl.ExpiresAt()), Status: string(dl.Status), Redrivable: dl.Redrivable,
	}
	if dl.Redrive != nil {
		out.Redrive = &deadletterv1.Redrive{Actor: dl.Redrive.Actor, Reason: dl.Redrive.Reason, At: ts(dl.Redrive.At)}
	}
	return out
}
