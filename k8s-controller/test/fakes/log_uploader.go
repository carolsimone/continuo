package fakes

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/k8s-controller/service/ports"
)

var _ ports.LogUploader = (*FakeLogUploader)(nil)

// FakeLogUploader is a fake implementation of ports.LogUploader for testing
type FakeLogUploader struct {
	UploadLogFunc   func(ctx context.Context, key, content string) error
	UploadCallCount int
	LastKey         string
	LastContent     string
	ShouldFail      bool
}

func (f *FakeLogUploader) UploadLog(ctx context.Context, key, content string) error {
	f.UploadCallCount++
	f.LastKey = key
	f.LastContent = content
	if f.ShouldFail {
		return fmt.Errorf("simulated S3 upload failure")
	}
	if f.UploadLogFunc != nil {
		return f.UploadLogFunc(ctx, key, content)
	}
	return nil
}
