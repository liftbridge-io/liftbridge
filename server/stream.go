package server

import (
	"bytes"
	"fmt"
	"path/filepath"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"

	client "github.com/tylertreat/go-jetbridge/proto"

	"github.com/tylertreat/jetbridge/server/commitlog"
	"github.com/tylertreat/jetbridge/server/proto"
)

type stream struct {
	*proto.Stream
	sub *nats.Subscription
	log CommitLog
	srv *Server
}

func (s *Server) newStream(st *proto.Stream) (*stream, error) {
	log, err := commitlog.New(commitlog.Options{
		Path:            filepath.Join(s.config.Clustering.RaftPath, st.Subject, st.Name),
		MaxSegmentBytes: s.config.Log.MaxSegmentBytes,
		MaxLogBytes:     s.config.Log.RetentionBytes,
		Compact:         s.config.Log.Compact,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create commit log")
	}

	return &stream{Stream: st, log: log, srv: s}, nil
}

func (s *stream) String() string {
	return fmt.Sprintf("[subject=%s, name=%s]", s.Subject, s.Name)
}

func (s *stream) close() error {
	if err := s.log.Close(); err != nil {
		return err
	}
	if s.sub != nil {
		return s.sub.Unsubscribe()
	}
	return nil
}

func (s *stream) handleMsg(msg *nats.Msg) {
	envelope := getEnvelope(msg.Data)
	m := &proto.Message{
		MagicByte: 2,
		Timestamp: time.Now(),
		Headers:   make(map[string][]byte),
	}
	if envelope != nil {
		m.Key = envelope.Key
		m.Value = envelope.Value
		for key, value := range envelope.Headers {
			m.Headers[key] = value
		}
	} else {
		m.Value = msg.Data
	}
	m.Headers["subject"] = []byte(msg.Subject)
	if msg.Reply != "" {
		m.Headers["reply"] = []byte(msg.Reply)
	}

	ms := &proto.MessageSet{Messages: []*proto.Message{m}}
	data, err := proto.Encode(ms)
	if err != nil {
		panic(err)
	}
	offset, err := s.log.Append(data)
	if err != nil {
		s.srv.logger.Errorf("Failed to append to log %s: %v", s, err)
		return
	}

	// Publish ack.
	if envelope != nil && envelope.AckInbox != "" {
		ack := &client.Ack{Offset: offset}
		data, err := ack.Marshal()
		if err != nil {
			panic(err)
		}
		s.srv.nats.Publish(envelope.AckInbox, data)
	}
}

func getEnvelope(data []byte) *client.Message {
	if len(data) <= 4 {
		return nil
	}
	if !bytes.Equal(data[0:4], envelopeCookie) {
		return nil
	}
	msg := &client.Message{}
	if err := msg.Unmarshal(data[4:]); err != nil {
		return nil
	}
	return msg
}
