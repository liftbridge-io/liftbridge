//go:generate protoc --gofast_out=plugins=grpc:. ./liftbridge-grpc/api.proto

package liftbridge

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/tylertreat/go-liftbridge/liftbridge-grpc"
)

// TODO: make these configurable.
const (
	maxConnsPerBroker = 2
	keepAliveTime     = 30 * time.Second
)

var (
	// ErrStreamExists is returned by CreateStream if the specified stream
	// already exists in the Liftbridge cluster.
	ErrStreamExists = errors.New("stream already exists")

	// ErrNoSuchStream is returned by Subscribe if the specified stream does
	// not exist in the Liftbridge cluster.
	ErrNoSuchStream = errors.New("stream does not exist")

	envelopeCookie    = []byte("LIFT")
	envelopeCookieLen = len(envelopeCookie)
)

// Handler is the callback invoked by Subscribe when a message is received on
// the specified stream. If err is not nil, the subscription will be terminated
// and no more messages will be received.
type Handler func(msg *proto.Message, err error)

// StreamInfo is used to describe a stream to create.
type StreamInfo struct {
	// Subject is the NATS subject the stream is attached to (required).
	Subject string

	// Name is the stream identifier, unique per subject (required).
	Name string

	// Group is the name of a load-balance group (optional). When there are
	// multiple streams in the same group, the messages will be balanced among
	// them.
	Group string

	// ReplicationFactor controls the number of servers to replicate a stream
	// to (optional). E.g. a value of 1 would mean only 1 server would have the
	// data, and a value of 3 would be 3 servers would have it. If this is not
	// set, it defaults to 1.
	ReplicationFactor int32
}

// Client is the main API used to communicate with a Liftbridge cluster. Call
// Connect to get a Client instance.
type Client interface {
	// Close the client connection.
	Close() error

	// CreateStream creates a new stream attached to a NATS subject. It returns
	// ErrStreamExists if a stream with the given subject and name already
	// exists.
	CreateStream(ctx context.Context, stream StreamInfo) error

	// Subscribe creates an ephemeral subscription for the given stream. It
	// begins receiving messages starting at the given offset and waits for new
	// messages when it reaches the end of the stream. It returns an
	// ErrNoSuchStream if the given stream does not exist. Use a cancelable
	// Context to close a subscription.
	Subscribe(ctx context.Context, subject, name string, offset int64, handler Handler) error
}

// NewEnvelope returns a serialized message envelope for the given key-value
// pair. Message keys are optional, so you may pass in nil for the key.
func NewEnvelope(key, value []byte, ackInbox string) []byte {
	msg := &proto.Message{
		Key:      key,
		Value:    value,
		AckInbox: ackInbox,
	}
	m, err := msg.Marshal()
	if err != nil {
		panic(err)
	}
	buf := make([]byte, envelopeCookieLen+len(m))
	copy(buf[0:], envelopeCookie)
	copy(buf[envelopeCookieLen:], m)
	return buf
}

// UnmarshalAck deserializes an Ack from the given byte slice. It returns an
// error if the given data is not actually an Ack.
func UnmarshalAck(data []byte) (*proto.Ack, error) {
	var (
		ack = &proto.Ack{}
		err = ack.Unmarshal(data)
	)
	return ack, err
}

// UnmarshalEnvelope deserializes a Message envelope from the given byte slice.
// It returns a bool indicating if the given data was actually a Message
// envelope or not.
func UnmarshalEnvelope(data []byte) (*proto.Message, bool) {
	if len(data) <= envelopeCookieLen {
		return nil, false
	}
	if !bytes.Equal(data[:envelopeCookieLen], envelopeCookie) {
		return nil, false
	}
	var (
		msg = &proto.Message{}
		err = msg.Unmarshal(data[envelopeCookieLen:])
	)
	if err != nil {
		return nil, false
	}
	return msg, true
}

// client implements the Client interface. It maintains a pool of connections
// for each broker in the cluster, limiting the number of connections and
// closing them when they go unused for a prolonged period of time.
type client struct {
	mu          sync.RWMutex
	apiClient   proto.APIClient
	conn        *grpc.ClientConn
	streamAddrs map[string]map[string]string
	brokerAddrs map[string]string
	pools       map[string]*connPool
	addrs       map[string]struct{}
}

// Connect creates a Client connection for the given Liftbridge cluster.
// Multiple addresses can be provided. Connect will use whichever it connects
// successfully to first in random order. The Client will use the pool of
// addresses for failover purposes. Note that only one seed address needs to be
// provided as the Client will discover the other brokers when fetching
// metadata for the cluster.
func Connect(addrs ...string) (Client, error) {
	if len(addrs) == 0 {
		return nil, errors.New("no addresses provided")
	}
	var (
		conn *grpc.ClientConn
		err  error
	)
	perm := rand.Perm(len(addrs))
	for _, i := range perm {
		addr := addrs[i]
		conn, err = grpc.Dial(addr, grpc.WithInsecure())
		if err == nil {
			break
		}
	}
	if conn == nil {
		return nil, err
	}
	addrMap := make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		addrMap[addr] = struct{}{}
	}
	c := &client{
		conn:      conn,
		apiClient: proto.NewAPIClient(conn),
		pools:     make(map[string]*connPool),
		addrs:     addrMap,
	}
	if err := c.updateMetadata(); err != nil {
		return nil, err
	}
	return c, nil
}

// Close the client connection.
func (c *client) Close() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, pool := range c.pools {
		if err := pool.close(); err != nil {
			return err
		}
	}
	return c.conn.Close()
}

// CreateStream creates a new stream attached to a NATS subject. It returns
// ErrStreamExists if a stream with the given subject and name already exists.
func (c *client) CreateStream(ctx context.Context, info StreamInfo) error {
	req := &proto.CreateStreamRequest{
		Subject:           info.Subject,
		Name:              info.Name,
		ReplicationFactor: info.ReplicationFactor,
		Group:             info.Group,
	}
	err := c.doResilientRPC(func(client proto.APIClient) error {
		_, err := client.CreateStream(ctx, req)
		return err
	})
	if status.Code(err) == codes.AlreadyExists {
		return ErrStreamExists
	}
	return err
}

// Subscribe creates an ephemeral subscription for the given stream. It begins
// receiving messages starting at the given offset and waits for new messages
// when it reaches the end of the stream. It returns an ErrNoSuchStream if the
// given stream does not exist. Use a cancelable Context to close a
// subscription.
func (c *client) Subscribe(ctx context.Context, subject, name string, offset int64, handler Handler) (err error) {
	var (
		pool   *connPool
		addr   string
		conn   *grpc.ClientConn
		stream proto.API_SubscribeClient
	)
	for i := 0; i < 5; i++ {
		pool, addr, err = c.getPoolAndAddr(subject, name)
		if err != nil {
			c.updateMetadata()
			continue
		}
		conn, err = pool.get(c.connFactory(addr))
		if err != nil {
			c.updateMetadata()
			continue
		}
		var (
			client = proto.NewAPIClient(conn)
			req    = &proto.SubscribeRequest{
				Subject: subject,
				Name:    name,
				Offset:  offset,
			}
		)
		stream, err = client.Subscribe(ctx, req)
		if err != nil {
			if status.Code(err) == codes.Unavailable {
				time.Sleep(25 * time.Millisecond)
				c.updateMetadata()
				continue
			}
			return err
		}

		// The server will either send an empty message, indicating the
		// subscription was successfully created, or an error.
		_, err = stream.Recv()
		if status.Code(err) == codes.FailedPrecondition {
			// This indicates the server was not the stream leader. Refresh
			// metadata and retry after waiting a bit.
			time.Sleep(time.Duration(10+i*25) * time.Millisecond)
			c.updateMetadata()
			continue
		}
		if err != nil {
			if status.Code(err) == codes.NotFound {
				err = ErrNoSuchStream
			}
			return err
		}
		break
	}

	if stream == nil {
		return err
	}

	go func() {
		defer pool.put(conn)
		for {
			var (
				msg, err = stream.Recv()
				code     = status.Code(err)
			)
			if err == nil || (err != nil && code != codes.Canceled) {
				handler(msg, err)
			}
			if err != nil {
				break
			}
		}
	}()

	return nil
}

// connFactory returns a pool connFactory for the given address. The
// connFactory dials the address to create a gRPC ClientConn.
func (c *client) connFactory(addr string) connFactory {
	return func() (*grpc.ClientConn, error) {
		return grpc.Dial(addr, grpc.WithInsecure())
	}
}

// updateMetadata fetches the latest cluster metadata, including stream and
// broker information. This maintains a map from broker ID to address and a map
// from stream to broker address.
func (c *client) updateMetadata() error {
	var resp *proto.FetchMetadataResponse
	if err := c.doResilientRPC(func(client proto.APIClient) (err error) {
		resp, err = client.FetchMetadata(context.Background(), &proto.FetchMetadataRequest{})
		return err
	}); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	brokerAddrs := make(map[string]string)
	for _, broker := range resp.Brokers {
		addr := fmt.Sprintf("%s:%d", broker.Host, broker.Port)
		brokerAddrs[broker.Id] = addr
		c.addrs[addr] = struct{}{}
	}
	c.brokerAddrs = brokerAddrs

	streamAddrs := make(map[string]map[string]string)
	for _, metadata := range resp.Metadata {
		subjectStreams, ok := streamAddrs[metadata.Stream.Subject]
		if !ok {
			subjectStreams = make(map[string]string)
			streamAddrs[metadata.Stream.Subject] = subjectStreams
		}
		subjectStreams[metadata.Stream.Name] = c.brokerAddrs[metadata.Leader]
	}
	c.streamAddrs = streamAddrs
	return nil
}

// getPoolAndAddr returns the connPool and broker address for the given stream.
func (c *client) getPoolAndAddr(subject, name string) (*connPool, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	streamAddrs, ok := c.streamAddrs[subject]
	if !ok {
		return nil, "", errors.New("no known broker for stream")
	}
	addr, ok := streamAddrs[name]
	if !ok {
		return nil, "", errors.New("no known broker for stream")
	}
	pool, ok := c.pools[addr]
	if !ok {
		pool = newConnPool(maxConnsPerBroker, keepAliveTime)
		c.pools[addr] = pool
	}
	return pool, addr, nil
}

// doResilientRPC executes the given RPC and performs retries if it fails due
// to the broker being unavailable, cycling through the known broker list.
func (c *client) doResilientRPC(rpc func(client proto.APIClient) error) (err error) {
	c.mu.RLock()
	client := c.apiClient
	c.mu.RUnlock()

	for i := 0; i < 5; i++ {
		err = rpc(client)
		if status.Code(err) == codes.Unavailable {
			conn, err := c.dialBroker()
			if err != nil {
				return err
			}
			client = proto.NewAPIClient(conn)
			c.mu.Lock()
			c.apiClient = client
			c.conn.Close()
			c.conn = conn
			c.mu.Unlock()
		} else {
			break
		}
	}
	return
}

// dialBroker dials each broker in the cluster, in random order, returning a
// gRPC ClientConn to the first one that is successful.
func (c *client) dialBroker() (*grpc.ClientConn, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	addrs := make([]string, len(c.addrs))
	i := 0
	for addr, _ := range c.addrs {
		addrs[i] = addr
		i++
	}
	var (
		conn *grpc.ClientConn
		err  error
		perm = rand.Perm(len(addrs))
	)
	for _, i := range perm {
		conn, err = grpc.Dial(addrs[i], grpc.WithInsecure())
		if err != nil {
			continue
		}
	}
	if conn == nil {
		return nil, err
	}
	return conn, nil
}
