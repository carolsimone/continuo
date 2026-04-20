package fakes

import (
	"context"

	"github.com/carolsimone/continuo/k8s-controller/domain/event"
)

// FakeMultiProducer is a fake implementation of EventMultiPublisher for testing
type FakeMultiProducer struct {
	PublishCheckFunc       func(ctx context.Context, evt event.JobCheckRequest) (string, error)
	PublishRetryFunc       func(ctx context.Context, evt event.TaskRetry) (string, error)
	PublishFailedFunc      func(ctx context.Context, evt event.TaskFailed) (string, error)
	PublishCheckCallCount  int
	PublishRetryCallCount  int
	PublishFailedCallCount int
	LastCheckEvent         *event.JobCheckRequest
	LastRetryEvent         *event.TaskRetry
	LastFailedEvent        *event.TaskFailed
}

// PublishCheck implements EventMultiPublisher
func (f *FakeMultiProducer) PublishCheck(ctx context.Context, evt event.JobCheckRequest) (string, error) {
	f.PublishCheckCallCount++
	f.LastCheckEvent = &evt
	if f.PublishCheckFunc != nil {
		return f.PublishCheckFunc(ctx, evt)
	}
	return "msg-id-check", nil
}

// PublishRetry implements EventMultiPublisher
func (f *FakeMultiProducer) PublishRetry(ctx context.Context, evt event.TaskRetry) (string, error) {
	f.PublishRetryCallCount++
	f.LastRetryEvent = &evt
	if f.PublishRetryFunc != nil {
		return f.PublishRetryFunc(ctx, evt)
	}
	return "msg-id-retry", nil
}

// PublishFailed implements EventMultiPublisher
func (f *FakeMultiProducer) PublishFailed(ctx context.Context, evt event.TaskFailed) (string, error) {
	f.PublishFailedCallCount++
	f.LastFailedEvent = &evt
	if f.PublishFailedFunc != nil {
		return f.PublishFailedFunc(ctx, evt)
	}
	return "msg-id-failed", nil
}

// FakeEventPublisher is a fake implementation of EventPublisher for testing
type FakeEventPublisher struct {
	PublishToStreamFunc func(ctx context.Context, streamName string, values map[string]interface{}) (string, error)
	PublishCallCount    int
	LastStreamName      string
	LastValues          map[string]interface{}
	// AllPublishes records every (streamName, values) pair in order.
	AllPublishes []PublishCall
}

// PublishCall records a single PublishToStream invocation.
type PublishCall struct {
	StreamName string
	Values     map[string]interface{}
}

// PublishToStream implements EventPublisher
func (f *FakeEventPublisher) PublishToStream(ctx context.Context, streamName string, values map[string]interface{}) (string, error) {
	f.PublishCallCount++
	f.LastStreamName = streamName
	f.LastValues = values
	f.AllPublishes = append(f.AllPublishes, PublishCall{StreamName: streamName, Values: values})
	if f.PublishToStreamFunc != nil {
		return f.PublishToStreamFunc(ctx, streamName, values)
	}
	return "msg-id-generic", nil
}

// FindPublish returns the first recorded call to the given stream, or nil.
func (f *FakeEventPublisher) FindPublish(streamName string) *PublishCall {
	for i := range f.AllPublishes {
		if f.AllPublishes[i].StreamName == streamName {
			return &f.AllPublishes[i]
		}
	}
	return nil
}
