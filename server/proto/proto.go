package proto

import (
	"fmt"
	"sync"
)

// StreamWrapper is a lightweight wrapper around a stream protobuf to provide
// thread-safe access to fields. All stream access should go through this
// wrapper and NOT directly access the proto.
type StreamWrapper struct {
	sync.RWMutex
	wrapped *Stream
}

func NewStreamWrapper(stream *Stream) *StreamWrapper {
	return &StreamWrapper{wrapped: stream}
}

// Unwrap returns the wrapped protobuf. Use this carefully since thread-safe
// access should go through the wrapper.
func (s *StreamWrapper) Unwrap() *Stream {
	return s.wrapped
}

// Marshal the StreamWrapper to its bytes representation.
func (s *StreamWrapper) Marshal() []byte {
	s.RLock()
	data, err := s.wrapped.Marshal()
	s.RUnlock()
	if err != nil {
		panic(err)
	}
	return data
}

// GetLeader returns the replica that is the stream leader and the leader
// epoch.
func (s *StreamWrapper) GetLeader() (string, uint64) {
	s.RLock()
	defer s.RUnlock()
	return s.wrapped.Leader, s.wrapped.LeaderEpoch
}

// SetLeader sets the replica that is the stream leader and the leader epoch.
func (s *StreamWrapper) SetLeader(leader string, leaderEpoch, epoch uint64) {
	s.Lock()
	s.wrapped.Leader = leader
	s.wrapped.LeaderEpoch = leaderEpoch
	s.wrapped.Epoch = epoch
	s.Unlock()
}

// GetReplicas returns the stream replicas.
func (s *StreamWrapper) GetReplicas() []string {
	s.RLock()
	defer s.RUnlock()
	return s.wrapped.Replicas
}

// GetISR returns the stream in-sync replicas set.
func (s *StreamWrapper) GetISR() []string {
	s.RLock()
	defer s.RUnlock()
	return s.wrapped.Isr
}

// GetEpoch returns the current stream epoch. The epoch is a monotonically
// increasing number which increases when a change is made to the stream. This
// is used to determine if an operation is outdated.
func (s *StreamWrapper) GetEpoch() uint64 {
	s.RLock()
	defer s.RUnlock()
	return s.wrapped.Epoch
}

// RemoveFromISR removes the given replica from the ISR.
func (s *StreamWrapper) RemoveFromISR(replica string, epoch uint64) {
	s.Lock()
	for i, r := range s.wrapped.Isr {
		if r == replica {
			s.wrapped.Isr = append(s.wrapped.Isr[:i], s.wrapped.Isr[i+1:]...)
			break
		}
	}
	s.wrapped.Epoch = epoch
	s.Unlock()
}

// AddToISR adds the given replica to the ISR.
func (s *StreamWrapper) AddToISR(replica string, epoch uint64) {
	s.Lock()
	s.wrapped.Isr = append(s.wrapped.Isr, replica)
	s.wrapped.Epoch = epoch
	s.Unlock()
}

// GetSubject returns the stream NATS subject.
func (s *StreamWrapper) GetSubject() string {
	s.RLock()
	defer s.RUnlock()
	return s.wrapped.Subject
}

// GetName returns the stream name.
func (s *StreamWrapper) GetName() string {
	s.RLock()
	defer s.RUnlock()
	return s.wrapped.Name
}

// GetGroup returns the stream group.
func (s *StreamWrapper) GetGroup() string {
	s.RLock()
	defer s.RUnlock()
	return s.wrapped.Group
}

// GetReplicationFactor returns the current stream replication factor, which is
// how many nodes to replicate the stream to.
func (s *StreamWrapper) GetReplicationFactor() int32 {
	s.RLock()
	defer s.RUnlock()
	return s.wrapped.ReplicationFactor
}

func (s *StreamWrapper) String() string {
	return fmt.Sprintf("[subject=%s, name=%s]", s.GetSubject(), s.GetName())
}
