package sip

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/livekit/protocol/rpc"
	"github.com/livekit/psrpc"
	"github.com/livekit/sip/pkg/stats"
)

const (
	outboundPerTrunkMaxConcurrentCalls = 1
	outboundPerTrunkMaxQueuedCalls     = 128
	outboundPerTrunkDialSpacing        = 2 * time.Second
)

type outboundTrunkQueueManager struct {
	mon    *stats.Monitor
	mu     sync.Mutex
	trunks map[string]*outboundTrunkQueue
}

type outboundTrunkQueueStatus struct {
	Running int
	Waiting int
}

type outboundTrunkQueue struct {
	running       int
	maxConcurrent int
	lastStart     time.Time
	waiters       []*outboundTrunkQueueWaiter
	timer         *time.Timer
}

type outboundTrunkQueueWaiter struct {
	ready chan struct{}
}

func newOutboundTrunkQueueManager(mon *stats.Monitor) *outboundTrunkQueueManager {
	return &outboundTrunkQueueManager{
		mon:    mon,
		trunks: make(map[string]*outboundTrunkQueue),
	}
}

func outboundTrunkQueueKey(req *rpc.InternalCreateSIPParticipantRequest) string {
	providerProfile := outboundProviderProfileForAddress(req.GetAddress())
	if providerProfile.OutboundQueueScope == outboundProviderQueueScopeProviderFrom {
		return fmt.Sprintf("provider:%s|from:%s", providerProfile.ID, req.GetNumber())
	}
	if req.GetSipTrunkId() != "" {
		return "id:" + req.GetSipTrunkId()
	}
	return fmt.Sprintf("addr:%s|from:%s", req.GetAddress(), req.GetNumber())
}

func outboundTrunkQueueMaxConcurrentCalls(req *rpc.InternalCreateSIPParticipantRequest) int {
	return outboundProviderProfileForAddress(req.GetAddress()).OutboundMaxConcurrentCalls
}

func (m *outboundTrunkQueueManager) Acquire(ctx context.Context, key string, maxConcurrent ...int) (func(), error) {
	maxCalls := outboundPerTrunkMaxConcurrentCalls
	if len(maxConcurrent) > 0 && maxConcurrent[0] > 0 {
		maxCalls = maxConcurrent[0]
	}
	m.mu.Lock()
	q := m.getOrCreateQueueLocked(key, maxCalls)
	if len(q.waiters) >= outboundPerTrunkMaxQueuedCalls {
		if m.mon != nil {
			m.mon.TrunkQueueRejected(key)
		}
		m.mu.Unlock()
		return nil, psrpc.NewErrorf(psrpc.ResourceExhausted, "outbound queue is full for trunk %q", key)
	}
	w := &outboundTrunkQueueWaiter{ready: make(chan struct{})}
	q.waiters = append(q.waiters, w)
	m.scheduleLocked(q)
	m.updateMetricsLocked(key, q)
	m.mu.Unlock()

	select {
	case <-w.ready:
		return m.releaseFunc(key), nil
	case <-ctx.Done():
		select {
		case <-w.ready:
			m.Release(key)
			return nil, ctx.Err()
		default:
		}
		m.mu.Lock()
		q := m.trunks[key]
		if q != nil {
			for i, cur := range q.waiters {
				if cur == w {
					q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
					break
				}
			}
			m.cleanupQueueLocked(key, q)
			m.updateMetricsLocked(key, q)
		}
		m.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (m *outboundTrunkQueueManager) releaseFunc(key string) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			m.Release(key)
		})
	}
}

func (m *outboundTrunkQueueManager) Release(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	q := m.trunks[key]
	if q == nil {
		return
	}
	if q.running > 0 {
		q.running--
	}
	m.scheduleLocked(q)
	m.cleanupQueueLocked(key, q)
	m.updateMetricsLocked(key, q)
}

func (m *outboundTrunkQueueManager) Status(key string) outboundTrunkQueueStatus {
	if m == nil {
		return outboundTrunkQueueStatus{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	q := m.trunks[key]
	if q == nil {
		return outboundTrunkQueueStatus{}
	}
	return outboundTrunkQueueStatus{
		Running: q.running,
		Waiting: len(q.waiters),
	}
}

func (m *outboundTrunkQueueManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key, q := range m.trunks {
		if q.timer != nil {
			q.timer.Stop()
			q.timer = nil
		}
		delete(m.trunks, key)
	}
}

func (m *outboundTrunkQueueManager) getOrCreateQueueLocked(key string, maxConcurrent int) *outboundTrunkQueue {
	if maxConcurrent <= 0 {
		maxConcurrent = outboundPerTrunkMaxConcurrentCalls
	}
	q := m.trunks[key]
	if q == nil {
		q = &outboundTrunkQueue{maxConcurrent: maxConcurrent}
		m.trunks[key] = q
	} else if q.maxConcurrent <= 0 {
		q.maxConcurrent = maxConcurrent
	}
	return q
}

func (m *outboundTrunkQueueManager) scheduleLocked(q *outboundTrunkQueue) {
	maxConcurrent := q.maxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = outboundPerTrunkMaxConcurrentCalls
	}
	if q.running >= maxConcurrent || len(q.waiters) == 0 {
		if q.timer != nil {
			q.timer.Stop()
			q.timer = nil
		}
		return
	}

	wait := time.Duration(0)
	if !q.lastStart.IsZero() {
		wait = time.Until(q.lastStart.Add(outboundPerTrunkDialSpacing))
	}
	if wait > 0 {
		if q.timer == nil {
			q.timer = time.AfterFunc(wait, func() {
				m.mu.Lock()
				defer m.mu.Unlock()
				m.scheduleLocked(q)
			})
			return
		}
		q.timer.Reset(wait)
		return
	}

	if q.timer != nil {
		q.timer.Stop()
		q.timer = nil
	}

	w := q.waiters[0]
	q.waiters = q.waiters[1:]
	q.running++
	q.lastStart = time.Now()
	close(w.ready)
}

func (m *outboundTrunkQueueManager) cleanupQueueLocked(key string, q *outboundTrunkQueue) {
	if q == nil || q.running > 0 || len(q.waiters) > 0 {
		return
	}
	if q.timer != nil {
		q.timer.Stop()
		q.timer = nil
	}
	delete(m.trunks, key)
}

func (m *outboundTrunkQueueManager) updateMetricsLocked(key string, q *outboundTrunkQueue) {
	if m.mon == nil {
		return
	}
	if q == nil {
		m.mon.TrunkQueueLength(key, 0)
		m.mon.TrunkQueueActive(key, 0)
		return
	}
	m.mon.TrunkQueueLength(key, len(q.waiters))
	m.mon.TrunkQueueActive(key, q.running)
}
