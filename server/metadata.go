package server

import "sync"

type MetadataStore interface {
	AddStream(stream *stream) error
	GetStreams() []*stream
	GetStream(subject, name string) *stream
	Reset() error
}

type subjectStreams map[string]*stream

type memMetadataStore struct {
	streams map[string]subjectStreams
	mu      sync.RWMutex
}

func newMetadataStore() MetadataStore {
	return &memMetadataStore{streams: make(map[string]subjectStreams)}
}

func (m *memMetadataStore) AddStream(stream *stream) error {
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

func (m *memMetadataStore) GetStreams() []*stream {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ret := make([]*stream, 0, len(m.streams))
	for _, streams := range m.streams {
		for _, stream := range streams {
			ret = append(ret, stream)
		}
	}
	return ret
}

func (m *memMetadataStore) GetStream(subject, name string) *stream {
	m.mu.RLock()
	defer m.mu.RUnlock()
	streams := m.streams[subject]
	if streams == nil {
		return nil
	}
	stream := streams[name]
	if stream == nil {
		return nil
	}
	return stream
}

func (m *memMetadataStore) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, streams := range m.streams {
		for _, stream := range streams {
			if err := stream.close(); err != nil {
				return err
			}
		}
	}
	m.streams = make(map[string]subjectStreams)
	return nil
}
