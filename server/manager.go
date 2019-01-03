package server

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"

	"github.com/liftbridge-io/liftbridge/server/proto"
)

type StreamUpdate interface {
	Stream() *proto.StreamWrapper
}

type baseStreamUpdate struct {
	stream *proto.StreamWrapper
}

func (b *baseStreamUpdate) Stream() *proto.StreamWrapper {
	return b.stream
}

type createStreamUpdate struct {
	*baseStreamUpdate
}

type shrinkISRUpdate struct {
	*baseStreamUpdate
	replica string
}

type expandISRUpdate struct {
	*baseStreamUpdate
	replica string
}

type changeLeaderUpdate struct {
	*baseStreamUpdate
}

// streamManagerAPI is the internal API for interacting with in-memory stream
// objects. All stream config access should go through the metadataAPI as
// replicated by Raft. The streamManagerAPI is materialized from the
// metadataAPI store. All stream access should go through the exported methods
// of the streamManagerAPI.
type streamManagerAPI struct {
	*Server
	streams   map[string]*stream
	mu        sync.RWMutex
	updates   chan StreamUpdate
	running   bool
	stopped   chan struct{}
	recovered int
}

func newStreamManagerAPI(s *Server) *streamManagerAPI {
	return &streamManagerAPI{
		Server:  s,
		streams: make(map[string]*stream),
		updates: make(chan StreamUpdate),
		stopped: make(chan struct{}),
	}
}

// Start will start any existing streams (if this server is a replica) and
// start a background loop to begin processing metadata updates. It returns the
// number of streams recovered from the metadata store. Stop should be called
// to stop the processing loop and terminate the manager. This call is
// idempotent.
func (s *streamManagerAPI) Start() (int, error) {
	s.mu.Lock()
	if s.running {
		recovered := s.recovered
		s.mu.Unlock()
		return recovered, nil
	}

	s.startGoroutine(s.startLoop)
	s.running = true
	s.mu.Unlock()

	count := 0
	for _, stream := range s.metadata.GetStreamProtos() {
		s.StreamCreated(stream)
		count++
	}
	s.mu.Lock()
	s.recovered = count
	s.mu.Unlock()
	return count, nil
}

// Stop closes all streams and stops the streamManagerAPI.
func (s *streamManagerAPI) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return nil
	}
	for _, stream := range s.streams {
		if err := stream.Close(); err != nil {
			return err
		}
	}
	s.running = false
	close(s.stopped)
	return nil
}

// StreamCreated signals the streamManagerAPI that a stream was created.
func (s *streamManagerAPI) StreamCreated(stream *proto.StreamWrapper) {
	s.update(&createStreamUpdate{&baseStreamUpdate{stream}})
}

// ISRShrunk signals the streamManagerAPI that a replica was removed from a
// stream's ISR.
func (s *streamManagerAPI) ISRShrunk(stream *proto.StreamWrapper, replica string) {
	s.update(&shrinkISRUpdate{&baseStreamUpdate{stream}, replica})
}

// ISRExpanded signals the streamManagerAPI that a replica was added to a
// stream's ISR.
func (s *streamManagerAPI) ISRExpanded(stream *proto.StreamWrapper, replica string) {
	s.update(&expandISRUpdate{&baseStreamUpdate{stream}, replica})
}

// LeaderChanges signals the streamManagerAPI that the leader for a stream was
// changed.
func (s *streamManagerAPI) LeaderChanged(stream *proto.StreamWrapper, leader string, epoch uint64) {
	s.update(&changeLeaderUpdate{&baseStreamUpdate{stream}})
}

// GetStream returns the stream with the given subject and name. It returns nil
// if no such stream exists.
func (s *streamManagerAPI) GetStream(subject, name string) *stream {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.streams[fmt.Sprintf("%s:%s", subject, name)]
}

// GetStream returns the stream described by the given proto. If returns nil if
// no such stream exists.
func (s *streamManagerAPI) GetStreamByProto(proto *proto.StreamWrapper) *stream {
	subject := proto.GetSubject()
	name := proto.GetName()
	return s.GetStream(subject, name)
}

func (s *streamManagerAPI) startLoop() {
	for {
		select {
		case <-s.stopped:
			return
		case update := <-s.updates:
			if err := s.applyUpdate(update); err != nil {
				panic(err)
			}
		}
	}
}

func (s *streamManagerAPI) applyUpdate(update StreamUpdate) error {
	// If this server is not a replica for the stream being updated, there's
	// nothing to do.
	if !s.isReplica(update.Stream()) {
		return nil
	}

	switch u := update.(type) {
	case *createStreamUpdate:
		err := s.createStream(u)
		return errors.Wrap(err, "failed to create stream")
	case *shrinkISRUpdate:
		err := s.shrinkISR(u)
		return errors.Wrap(err, "failed to shrink ISR")
	case *expandISRUpdate:
		err := s.expandISR(u)
		return errors.Wrap(err, "failed to expand ISR")
	case *changeLeaderUpdate:
		err := s.changeLeader(u)
		return errors.Wrap(err, "failed to change stream leader")
	default:
		return fmt.Errorf("Unknown metadata update: %+v", update)
	}
}

func (s *streamManagerAPI) createStream(update *createStreamUpdate) error {
	// Create the in-memory stream object. This will initialize/recover the
	// durable commit log.
	stream, err := s.newStream(update.Stream())
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.streams[fmt.Sprintf("%s:%s", stream.GetSubject(), stream.GetName())] = stream
	s.mu.Unlock()

	// Start leader/follower loop.
	return stream.LeaderChanged()
}

func (s *streamManagerAPI) shrinkISR(update *shrinkISRUpdate) error {
	stream := s.GetStreamByProto(update.Stream())
	if stream == nil {
		return ErrNoSuchStream
	}
	return stream.RemoveFromISR(update.replica)
}

func (s *streamManagerAPI) expandISR(update *expandISRUpdate) error {
	stream := s.GetStreamByProto(update.Stream())
	if stream == nil {
		return ErrNoSuchStream
	}
	return stream.AddToISR(update.replica)
}

func (s *streamManagerAPI) changeLeader(update *changeLeaderUpdate) error {
	stream := s.GetStreamByProto(update.Stream())
	if stream == nil {
		return ErrNoSuchStream
	}
	return stream.LeaderChanged()
}

// isReplica indicates if this server is a replica for the given stream.
func (s *streamManagerAPI) isReplica(stream *proto.StreamWrapper) bool {
	for _, replica := range stream.GetReplicas() {
		if s.config.Clustering.ServerID == replica {
			return true
		}
	}
	return false
}

func (s *streamManagerAPI) update(update StreamUpdate) {
	s.mu.RLock()
	running := s.running
	s.mu.RUnlock()
	if !running {
		return
	}
	s.updates <- update
}
