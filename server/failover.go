package server

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/status"
)

type failoverExpiredHandler func()

type failoverHandler func(context.Context) *status.Status

// failover represents a resource that is controlled by a leader that can be
// failed over to another broker, such as a stream partition or consumer group.
type failover interface {
	// Quorum returns the number of failure reports needed from distinct
	// witnesses in order to trigger a failover to a new leader.
	Quorum() int

	// Timeout returns the time elapsed before expiring a failover. Each time a
	// report is made, the failover's timeout is reset. Upon timing out, the
	// timer for the leader failover is removed.
	Timeout() time.Duration

	// OnExpired is invoked when the timeout is reached indicating the failover
	// has expired.
	OnExpired()

	// Failover selects a new leader. It returns a Status if selecting a new
	// leader fails.
	Failover(context.Context) *status.Status
}

// failoverStatus tracks witnesses for a leader failover. Witnesses are
// replicas which have reported the leader as unresponsive. If a quorum of
// witnesses report the leader within a bounded period of time, the controller
// will select a new leader.
type failoverStatus struct {
	mu        sync.Mutex
	failover  failover
	timer     *time.Timer
	witnesses map[string]struct{}
}

func newFailoverStatus(f failover) *failoverStatus {
	return &failoverStatus{
		failover:  f,
		witnesses: make(map[string]struct{}),
	}
}

// report adds the given witness to the failoverStatus witnesses. If a quorum
// of witnesses have reported the leader, a new leader will be selected.
// Otherwise, the expiration timer is reset. A Status is returned if selecting
// a new leader fails.
func (f *failoverStatus) report(ctx context.Context, witness string) *status.Status {
	f.mu.Lock()

	f.witnesses[witness] = struct{}{}
	leaderFailed := len(f.witnesses) > f.failover.Quorum()

	if leaderFailed {
		if f.timer != nil {
			f.timer.Stop()
		}
		f.mu.Unlock()
		return f.failover.Failover(ctx)
	}

	if f.timer != nil {
		f.timer.Reset(f.failover.Timeout())
	} else {
		f.timer = time.AfterFunc(f.failover.Timeout(), f.failover.OnExpired)
	}
	f.mu.Unlock()
	return nil
}

// cancel stops the expiration timer, if there is one.
func (f *failoverStatus) cancel() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.timer != nil {
		f.timer.Stop()
	}
}

// partitionFailover implements the failover interface for a stream partition
// leader. When a majority of a partition's ISR report the leader as failed, a
// new leader is selected.
type partitionFailover struct {
	partition  *partition
	timeout    time.Duration
	onExpired  failoverExpiredHandler
	onFailover failoverHandler
}

func newPartitionFailoverStatus(partition *partition, timeout time.Duration,
	onExpired failoverExpiredHandler, onFailover failoverHandler) *failoverStatus {

	return newFailoverStatus(&partitionFailover{
		partition:  partition,
		timeout:    timeout,
		onExpired:  onExpired,
		onFailover: onFailover,
	})
}

// Quorum returns (partition ISR size - 1) / 2. One is subtracted from the ISR
// size to exclude the leader.
func (p *partitionFailover) Quorum() int {
	return (p.partition.ISRSize() - 1) / 2
}

// Timeout returns the configured ReplicaMaxLeaderTimeout.
func (p *partitionFailover) Timeout() time.Duration {
	return p.timeout
}

// OnExpired expires the failover.
func (p *partitionFailover) OnExpired() {
	p.onExpired()
}

// Failover selects a new leader.
func (p *partitionFailover) Failover(ctx context.Context) *status.Status {
	return p.onFailover(ctx)
}

// groupFailover implements the failover interface for a consumer group
// coordinator. When a majority of a consumer group's members report the
// coordinator as failed, a new coordinator is selected.
type groupFailover struct {
	group      *consumerGroup
	timeout    time.Duration
	onExpired  failoverExpiredHandler
	onFailover failoverHandler
}

func newGroupFailoverStatus(group *consumerGroup, timeout time.Duration,
	onExpired failoverExpiredHandler, onFailover failoverHandler) *failoverStatus {

	return newFailoverStatus(&groupFailover{
		group:      group,
		timeout:    timeout,
		onExpired:  onExpired,
		onFailover: onFailover,
	})
}

// Quorum returns members / 2.
func (g *groupFailover) Quorum() int {
	return len(g.group.GetMembers()) / 2
}

// Timeout returns the configured GroupsCoordinatorTimeout.
func (g *groupFailover) Timeout() time.Duration {
	return g.timeout
}

// OnExpired expires the failover.
func (g *groupFailover) OnExpired() {
	g.onExpired()
}

// Failover selects a new coordinator.
func (g *groupFailover) Failover(ctx context.Context) *status.Status {
	return g.onFailover(ctx)
}
