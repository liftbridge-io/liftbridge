package server

import (
	"fmt"
	"path/filepath"

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

	buf := make([]byte, len(msg.Data)+8)
	copy(buf[8:], msg.Data)
	if _, err := s.log.Append(buf); err != nil {
		s.srv.logger.Errorf("Failed to append to log %s: %v", s, err)
		return
	}
}
