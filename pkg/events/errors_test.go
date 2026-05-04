package events

import (
	"errors"
	"fmt"
	"testing"
)

func TestErrPermanent_IsRecognisedViaErrorsIs(t *testing.T) {
	wrapped := fmt.Errorf("%w: image_tag missing for service svc-1", ErrPermanent)
	if !errors.Is(wrapped, ErrPermanent) {
		t.Fatalf("expected errors.Is(wrapped, ErrPermanent) == true, got false")
	}
}

func TestErrPermanent_PlainErrorIsNotPermanent(t *testing.T) {
	plain := errors.New("transient k8s api timeout")
	if errors.Is(plain, ErrPermanent) {
		t.Fatalf("expected errors.Is(plain, ErrPermanent) == false, got true")
	}
}
