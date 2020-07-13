package server

import (
	"fmt"
	"sync"

	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

// stream is a message stream consisting of one or more partitions. Each
// partition maps to a NATS subject and is the unit of replication.
type stream struct {
	name       string
	subject    string
	config     *proto.StreamConfig
	partitions map[int32]*partition
	resumeAll  bool // When partition(s) are paused, this indicates if all should be resumed
	mu         sync.RWMutex
}

// newStream creates a stream for the given NATS subject. All stream
// interactions should only go through the exported functions.
func newStream(name, subject string, config *proto.StreamConfig) *stream {
	return &stream{
		name:       name,
		subject:    subject,
		config:     config,
		partitions: make(map[int32]*partition),
	}
}

// String returns a human-readable representation of the stream.
func (s *stream) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("[name=%s, subject=%s, partitions=%d]",
		s.name, s.subject, len(s.partitions))
}

// GetName returns the stream's globally unique name.
func (s *stream) GetName() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.name
}

// GetSubject returns the stream's NATS subject.
func (s *stream) GetSubject() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.subject
}

// GetConfig returns the stream's custom configuration.
func (s *stream) GetConfig() *proto.StreamConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.config
}

// GetResumeAll returns a bool indicating if the stream was paused with
// ResumeAll enabled. This means a message published to any of the stream's
// partitions will resume any paused partitions.
func (s *stream) GetResumeAll() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.resumeAll
}

// SetResumeAll sets the bool used to indicate if the stream was paused with
// ResumeAll enabled. This means a message published to any of the stream's
// partitions will resume any paused partitions.
func (s *stream) SetResumeAll(resumeAll bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resumeAll = resumeAll
}

// GetPartitions returns a map of partition ID to partition.
func (s *stream) GetPartitions() map[int32]*partition {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.partitions
}

// GetPartition returns the partition with the given ID or nil if there is no
// such partition.
func (s *stream) GetPartition(id int32) *partition {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.partitions[id]
}

// SetPartition sets the partition with the given id on the stream.
func (s *stream) SetPartition(id int32, p *partition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.partitions[id] = p
}

// Close the stream by closing each of its partitions.
func (s *stream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, partition := range s.partitions {
		if err := partition.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Pause some or all the partitions of this stream.
func (s *stream) Pause(partitions []int32, resumeAll bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	toPause := make([]*partition, 0, len(partitions))
	if len(partitions) == 0 {
		for _, partition := range s.partitions {
			toPause = append(toPause, partition)
		}
	} else {
		for _, partitionID := range partitions {
			partition, ok := s.partitions[partitionID]
			if !ok {
				return ErrPartitionNotFound
			}
			toPause = append(toPause, partition)
		}
	}

	for _, partition := range toPause {
		if err := partition.Pause(); err != nil {
			return err
		}
	}

	s.resumeAll = resumeAll
	return nil
}

// Delete the stream by closing and deleting each of its partitions.
func (s *stream) Delete() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, partition := range s.partitions {
		if err := partition.Delete(); err != nil {
			return err
		}
	}
	return nil
}

// SetStreamReadonly sets the readonly flag on some or all the partitions of
// this stream.
func (s *stream) SetStreamReadonly(partitions []int32, readonly bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	toSetReadonly := make([]*partition, 0, len(partitions))
	if len(partitions) == 0 {
		for _, partition := range s.partitions {
			toSetReadonly = append(toSetReadonly, partition)
		}
	} else {
		for _, partitionID := range partitions {
			partition, ok := s.partitions[partitionID]
			if !ok {
				return ErrPartitionNotFound
			}
			toSetReadonly = append(toSetReadonly, partition)
		}
	}

	for _, partition := range toSetReadonly {
		partition.SetReadonly(readonly)
	}

	return nil
}
