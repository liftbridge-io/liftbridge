// Copyright 2017-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package natslog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/nats-io/go-nats"
)

const (
	natsConnectInbox = "%s%s.accept"
	natsRequestInbox = "%s%s.request.%s"
)

// natsAddr implements the net.Addr interface. An address for the NATS
// transport is simply a node id, which is then used to construct an inbox.
type natsAddr string

func (n natsAddr) Network() string {
	return "nats"
}

func (n natsAddr) String() string {
	return string(n)
}

type connectRequestProto struct {
	ID    string `json:"id"`
	Inbox string `json:"inbox"`
}

type connectResponseProto struct {
	Inbox string `json:"inbox"`
}

// natsConn implements the net.Conn interface by simulating a stream-oriented
// connection between two peers. It does this by establishing a unique inbox at
// each endpoint which the peers use to stream data to each other.
type natsConn struct {
	conn       *nats.Conn
	localAddr  natsAddr
	remoteAddr natsAddr
	sub        *nats.Subscription
	outbox     string
	mu         sync.RWMutex
	closed     bool
	reader     *timeoutReader
	writer     io.WriteCloser
	parent     *natsStreamLayer
}

func (n *natsConn) Read(b []byte) (int, error) {
	n.mu.RLock()
	closed := n.closed
	n.mu.RUnlock()
	if closed {
		return 0, errors.New("read from closed conn")
	}
	return n.reader.Read(b)
}

func (n *natsConn) Write(b []byte) (int, error) {
	n.mu.RLock()
	closed := n.closed
	n.mu.RUnlock()
	if closed {
		return 0, errors.New("write to closed conn")
	}

	if len(b) == 0 {
		return 0, nil
	}

	// Send data in chunks to avoid hitting max payload.
	for i := 0; i < len(b); {
		chunkSize := min(int64(len(b[i:])), n.conn.MaxPayload())
		if err := n.conn.Publish(n.outbox, b[i:int64(i)+chunkSize]); err != nil {
			return i, err
		}
		i += int(chunkSize)
	}

	return len(b), nil
}

func (n *natsConn) Close() error {
	return n.close(true)
}

func (n *natsConn) close(signalRemote bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return nil
	}

	if err := n.sub.Unsubscribe(); err != nil {
		return err
	}

	if signalRemote {
		// Send empty message to signal EOF for a graceful disconnect. Not
		// concerned with errors here as this is best effort.
		n.conn.Publish(n.outbox, nil)
		// Best effort, don't block for too long and don't check returned error.
		n.conn.FlushTimeout(500 * time.Millisecond)
	}

	n.closed = true
	n.parent.mu.Lock()
	delete(n.parent.conns, n)
	n.parent.mu.Unlock()
	n.writer.Close()

	return nil
}

func (n *natsConn) LocalAddr() net.Addr {
	return n.localAddr
}

func (n *natsConn) RemoteAddr() net.Addr {
	return n.remoteAddr
}

func (n *natsConn) SetDeadline(t time.Time) error {
	if err := n.SetReadDeadline(t); err != nil {
		return err
	}
	return n.SetWriteDeadline(t)
}

func (n *natsConn) SetReadDeadline(t time.Time) error {
	n.reader.SetDeadline(t)
	return nil
}

func (n *natsConn) SetWriteDeadline(t time.Time) error {
	return nil
}

func (n *natsConn) msgHandler(msg *nats.Msg) {
	// Check if remote peer disconnected.
	if len(msg.Data) == 0 {
		n.close(false)
		return
	}

	n.writer.Write(msg.Data)
}

// natsStreamLayer implements the raft.StreamLayer interface.
type natsStreamLayer struct {
	conn          *nats.Conn
	localAddr     natsAddr
	sub           *nats.Subscription
	logger        *log.Logger
	conns         map[*natsConn]struct{}
	mu            sync.Mutex
	timeout       time.Duration
	subjectPrefix string
}

func newNATSStreamLayer(
	id, subjectPrefix string,
	conn *nats.Conn,
	logger *log.Logger,
	timeout time.Duration) (*natsStreamLayer, error) {
	n := &natsStreamLayer{
		localAddr:     natsAddr(id),
		conn:          conn,
		logger:        logger,
		conns:         map[*natsConn]struct{}{},
		timeout:       timeout,
		subjectPrefix: subjectPrefix,
	}
	sub, err := conn.SubscribeSync(fmt.Sprintf(natsConnectInbox, subjectPrefix, id))
	if err != nil {
		return nil, err
	}
	sub.SetPendingLimits(-1, -1)
	if err := conn.FlushTimeout(timeout); err != nil {
		sub.Unsubscribe()
		return nil, err
	}
	n.sub = sub
	return n, nil
}

func (n *natsStreamLayer) newNATSConn(address string) *natsConn {
	// TODO: probably want a buffered pipe.
	reader, writer := io.Pipe()
	return &natsConn{
		conn:       n.conn,
		localAddr:  n.localAddr,
		remoteAddr: natsAddr(address),
		reader:     newTimeoutReader(reader),
		writer:     writer,
		parent:     n,
	}
}

// Dial creates a new net.Conn with the remote address. This is implemented by
// performing a handshake over NATS which establishes unique inboxes at each
// endpoint for streaming data.
func (n *natsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	if !n.conn.IsConnected() {
		return nil, errors.New("raft-nats: dial failed, not connected")
	}

	// QUESTION: The Raft NetTransport does connection pooling, which is useful
	// for TCP sockets. The NATS transport simulates a socket using a
	// subscription at each endpoint, but everything goes over the same NATS
	// socket. This means there is little advantage to pooling here currently.
	// Should we actually Dial a new NATS connection here and rely on pooling?

	connect := &connectRequestProto{
		ID:    n.localAddr.String(),
		Inbox: fmt.Sprintf(natsRequestInbox, n.subjectPrefix, n.localAddr.String(), nats.NewInbox()),
	}
	data, err := json.Marshal(connect)
	if err != nil {
		panic(err)
	}

	peerConn := n.newNATSConn(string(address))

	// Setup inbox.
	sub, err := n.conn.Subscribe(connect.Inbox, peerConn.msgHandler)
	if err != nil {
		return nil, err
	}
	sub.SetPendingLimits(-1, -1)
	if err := n.conn.FlushTimeout(n.timeout); err != nil {
		sub.Unsubscribe()
		return nil, err
	}

	// Make connect request to peer.
	msg, err := n.conn.Request(fmt.Sprintf(natsConnectInbox, n.subjectPrefix, address), data, timeout)
	if err != nil {
		sub.Unsubscribe()
		return nil, err
	}
	var resp connectResponseProto
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		sub.Unsubscribe()
		return nil, err
	}

	peerConn.sub = sub
	peerConn.outbox = resp.Inbox
	n.mu.Lock()
	n.conns[peerConn] = struct{}{}
	n.mu.Unlock()
	return peerConn, nil
}

// Accept waits for and returns the next connection to the listener.
func (n *natsStreamLayer) Accept() (net.Conn, error) {
	for {
		msg, err := n.sub.NextMsgWithContext(context.TODO())
		if err != nil {
			return nil, err
		}
		if msg.Reply == "" {
			n.logger.Println("[ERR] raft-nats: Invalid connect message (missing reply inbox)")
			continue
		}

		var connect connectRequestProto
		if err := json.Unmarshal(msg.Data, &connect); err != nil {
			n.logger.Println("[ERR] raft-nats: Invalid connect message (invalid data)")
			continue
		}

		peerConn := n.newNATSConn(connect.ID)
		peerConn.outbox = connect.Inbox

		// Setup inbox for peer.
		inbox := fmt.Sprintf(natsRequestInbox, n.subjectPrefix, n.localAddr.String(), nats.NewInbox())
		sub, err := n.conn.Subscribe(inbox, peerConn.msgHandler)
		if err != nil {
			n.logger.Printf("[ERR] raft-nats: Failed to create inbox for remote peer: %v", err)
			continue
		}
		sub.SetPendingLimits(-1, -1)
		// Reply to peer.
		resp := &connectResponseProto{Inbox: inbox}
		data, err := json.Marshal(resp)
		if err != nil {
			panic(err)
		}
		if err := n.conn.Publish(msg.Reply, data); err != nil {
			n.logger.Printf("[ERR] raft-nats: Failed to send connect response to remote peer: %v", err)
			sub.Unsubscribe()
			continue
		}
		if err := n.conn.FlushTimeout(n.timeout); err != nil {
			n.logger.Printf("[ERR] raft-nats: Failed to flush connect response to remote peer: %v", err)
			sub.Unsubscribe()
			continue
		}
		peerConn.sub = sub
		n.mu.Lock()
		n.conns[peerConn] = struct{}{}
		n.mu.Unlock()
		return peerConn, nil
	}
}

func (n *natsStreamLayer) Close() error {
	n.mu.Lock()
	conns := make(map[*natsConn]struct{}, len(n.conns))
	for conn, s := range n.conns {
		conns[conn] = s
	}
	n.mu.Unlock()
	for c := range conns {
		c.Close()
	}
	return n.sub.Unsubscribe()
}

func (n *natsStreamLayer) Addr() net.Addr {
	return n.localAddr
}

// NewNATSTransport creates a new raft.NetworkTransport implemented with NATS
// as the transport layer.
func NewNATSTransport(
	id, subjectPrefix string,
	conn *nats.Conn,
	timeout time.Duration,
	logOutput io.Writer) (*raft.NetworkTransport, error) {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	return NewNATSTransportWithLogger(id, subjectPrefix, conn, timeout, log.New(logOutput, "", log.LstdFlags))
}

// NewNATSTransportWithLogger creates a new raft.NetworkTransport implemented
// with NATS as the transport layer using the provided Logger.
func NewNATSTransportWithLogger(
	id, subjectPrefix string,
	conn *nats.Conn,
	timeout time.Duration,
	logger *log.Logger) (*raft.NetworkTransport, error) {
	return createNATSTransport(id, subjectPrefix, conn, logger, timeout, func(stream raft.StreamLayer) *raft.NetworkTransport {
		return raft.NewNetworkTransportWithLogger(stream, 3, timeout, logger)
	})
}

// NewNATSTransportWithConfig returns a raft.NetworkTransport implemented
// with NATS as the transport layer, using the given config struct.
func NewNATSTransportWithConfig(
	id, subjectPrefix string,
	conn *nats.Conn,
	config *raft.NetworkTransportConfig) (*raft.NetworkTransport, error) {
	if config.Timeout == 0 {
		config.Timeout = 2 * time.Second
	}
	return createNATSTransport(id, subjectPrefix, conn, config.Logger, config.Timeout, func(stream raft.StreamLayer) *raft.NetworkTransport {
		config.Stream = stream
		return raft.NewNetworkTransportWithConfig(config)
	})
}

func createNATSTransport(
	id, subjectPrefix string,
	conn *nats.Conn,
	logger *log.Logger,
	timeout time.Duration,
	transportCreator func(stream raft.StreamLayer) *raft.NetworkTransport) (*raft.NetworkTransport, error) {

	stream, err := newNATSStreamLayer(id, subjectPrefix, conn, logger, timeout)
	if err != nil {
		return nil, err
	}

	return transportCreator(stream), nil
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
