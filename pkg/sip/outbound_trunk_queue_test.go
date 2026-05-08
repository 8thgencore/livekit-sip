package sip

import (
	"context"
	"testing"
	"time"

	"github.com/livekit/protocol/rpc"
	"github.com/stretchr/testify/require"
)

func TestOutboundTrunkQueueKey(t *testing.T) {
	req := &rpc.InternalCreateSIPParticipantRequest{
		SipTrunkId: "trunk-1",
		Address:    "sip.example.com",
		Number:     "1001",
	}
	require.Equal(t, "id:trunk-1", outboundTrunkQueueKey(req))

	req.SipTrunkId = ""
	require.Equal(t, "addr:sip.example.com|from:1001", outboundTrunkQueueKey(req))
}

func TestOutboundTrunkQueueFIFOAndSpacing(t *testing.T) {
	mgr := newOutboundTrunkQueueManager(nil)
	t.Cleanup(mgr.Stop)

	ctx := context.Background()
	started := make(chan int, 2)
	released := make(chan struct{})
	firstStartedAt := make(chan time.Time, 1)

	go func() {
		release, err := mgr.Acquire(ctx, "trunk-a")
		require.NoError(t, err)
		firstStartedAt <- time.Now()
		started <- 1
		<-released
		release()
	}()

	select {
	case got := <-started:
		require.Equal(t, 1, got)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("first acquire did not start")
	}
	firstAt := <-firstStartedAt

	secondReadyAt := make(chan time.Time, 1)
	go func() {
		release, err := mgr.Acquire(ctx, "trunk-a")
		require.NoError(t, err)
		started <- 2
		secondReadyAt <- time.Now()
		release()
	}()

	select {
	case got := <-started:
		t.Fatalf("unexpected early start for acquire %d", got)
	case <-time.After(150 * time.Millisecond):
	}

	released <- struct{}{}

	select {
	case got := <-started:
		require.Equal(t, 2, got)
	case <-time.After(1500 * time.Millisecond):
		t.Fatal("second acquire did not start after release")
	}

	startedAt := <-secondReadyAt
	require.GreaterOrEqual(t, startedAt.Sub(firstAt), outboundPerTrunkDialSpacing)
}

func TestOutboundTrunkQueueContextCancel(t *testing.T) {
	mgr := newOutboundTrunkQueueManager(nil)
	t.Cleanup(mgr.Stop)

	firstRelease, err := mgr.Acquire(context.Background(), "trunk-a")
	require.NoError(t, err)
	defer firstRelease()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = mgr.Acquire(ctx, "trunk-a")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	mgr.mu.Lock()
	q := mgr.trunks["trunk-a"]
	waiters := 0
	if q != nil {
		waiters = len(q.waiters)
	}
	mgr.mu.Unlock()
	require.Equal(t, 0, waiters)
}

func TestOutboundTrunkQueueFull(t *testing.T) {
	mgr := newOutboundTrunkQueueManager(nil)
	t.Cleanup(mgr.Stop)

	firstRelease, err := mgr.Acquire(context.Background(), "trunk-a")
	require.NoError(t, err)
	defer firstRelease()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < outboundPerTrunkMaxQueuedCalls; i++ {
		go func() {
			_, _ = mgr.Acquire(ctx, "trunk-a")
		}()
	}

	require.Eventually(t, func() bool {
		mgr.mu.Lock()
		defer mgr.mu.Unlock()
		q := mgr.trunks["trunk-a"]
		return q != nil && len(q.waiters) == outboundPerTrunkMaxQueuedCalls
	}, time.Second, 10*time.Millisecond)

	_, err = mgr.Acquire(context.Background(), "trunk-a")
	require.Error(t, err)
	require.Contains(t, err.Error(), "outbound queue is full")
}
