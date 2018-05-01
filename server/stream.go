package server

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"

	"github.com/tylertreat/jetbridge/server/commitlog"
	"github.com/tylertreat/jetbridge/server/proto"
)

const (
	defaultMaxSegmentBytes = 1073741824
	defaultRetentionBytes  = -1
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
		MaxSegmentBytes: defaultMaxSegmentBytes, // TODO: make configurable
		MaxLogBytes:     defaultRetentionBytes,  // TODO: make configurable
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
	if s.sub != nil {
		return s.sub.Unsubscribe()
	}
	return nil
}

func (s *stream) handleMsg(msg *nats.Msg) {
	// TODO: do envelope check.

	replyLen := len(msg.Reply)
	buf := make([]byte, 4+replyLen+len(msg.Data))
	proto.Encoding.PutUint32(buf[0:], uint32(replyLen))
	copy(buf[4:], msg.Reply)
	copy(buf[4+replyLen:], msg.Data)
	ms := &proto.MessageSet{Messages: []*proto.Message{
		&proto.Message{
			MagicByte: 1,
			Value:     buf,
			Timestamp: time.Now(),
			// TODO: CRC
		},
	}}
	data, err := proto.Encode(ms)
	if err != nil {
		panic(err)
	}
	if _, err := s.log.Append(data); err != nil {
		s.srv.logger.Errorf("Failed to append to log %s: %v", s, err)
		return
	}
}
