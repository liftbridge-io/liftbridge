package metadata

import (
	"sync"

	"github.com/tylertreat/jetbridge/server/proto"
)

type MetadataStore interface {
	AddStream(stream *proto.Stream) error
}

type subjectStreams map[string]*proto.Stream

type memMetadataStore struct {
	streams map[string]subjectStreams
	mu      sync.RWMutex
}

func NewMetadataStore() MetadataStore {
	return &memMetadataStore{streams: make(map[string]subjectStreams)}
}

func (m *memMetadataStore) AddStream(stream *proto.Stream) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	streams := m.streams[stream.Subject]
	if streams == nil {
		streams = make(subjectStreams)
		m.streams[stream.Subject] = streams
	}
	if _, ok := streams[stream.Name]; ok {
		// Stream for subject with name already exists.
		return nil
	}
	streams[stream.Name] = stream
	return nil
}
