package ingest

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/whyrusleeping/valsgo"
)

func TestValsBatchRetryPolicyBusyNeverDrops(t *testing.T) {
	wrapped := fmt.Errorf("batch below quorum: %w", valsgo.ErrBusy)
	for _, attempt := range []int{0, valsHardRetryLimit, 20, 1000} {
		policy := valsBatchRetryPolicy(wrapped, attempt, 0.5)
		if !policy.retry || !policy.backpressure {
			t.Fatalf("attempt %d: Busy selected drop/non-backpressure policy: %+v", attempt, policy)
		}
		if policy.refresh {
			t.Fatalf("attempt %d: Busy should not refresh healthy cluster routing", attempt)
		}
		if policy.delay < 750*time.Millisecond || policy.delay > valsBusyBackoffCap {
			t.Fatalf("attempt %d: Busy delay %s outside bounds", attempt, policy.delay)
		}
	}
}

func TestValsBatchRetryPolicyBusyBackoffIsCappedAndJittered(t *testing.T) {
	low := valsBatchRetryPolicy(valsgo.ErrBusy, 2, 0).delay
	high := valsBatchRetryPolicy(valsgo.ErrBusy, 2, 1).delay
	if low != 3*time.Second || high != 5*time.Second {
		t.Fatalf("attempt-2 jitter range = [%s,%s], want [3s,5s]", low, high)
	}
	if got := valsBatchRetryPolicy(valsgo.ErrBusy, 100, 1).delay; got != valsBusyBackoffCap {
		t.Fatalf("large-attempt Busy delay = %s, want cap %s", got, valsBusyBackoffCap)
	}
}

func TestValsBatchRetryPolicyHardFailuresRemainBounded(t *testing.T) {
	hard := errors.New("dial failed")
	for attempt := 0; attempt < valsHardRetryLimit; attempt++ {
		policy := valsBatchRetryPolicy(hard, attempt, 0.5)
		if !policy.retry || policy.backpressure || !policy.refresh {
			t.Fatalf("attempt %d: wrong hard-failure policy: %+v", attempt, policy)
		}
	}
	if policy := valsBatchRetryPolicy(hard, valsHardRetryLimit, 0.5); policy.retry {
		t.Fatalf("hard failure did not select drop path at limit: %+v", policy)
	}
}

func TestValsRetryStateBusyDoesNotConsumeHardBudget(t *testing.T) {
	state := valsRetryState{}
	for i := 0; i < 100; i++ {
		policy, _ := state.next(valsgo.ErrBusy, 0.5)
		if !policy.retry {
			t.Fatalf("Busy attempt %d selected drop path", i)
		}
	}
	policy, attempt := state.next(errors.New("dial failed"), 0.5)
	if attempt != 0 || !policy.retry {
		t.Fatalf("first hard failure after Busy retries = attempt %d, policy %+v", attempt, policy)
	}
}
