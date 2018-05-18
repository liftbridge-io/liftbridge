package commitlog_test

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tylertreat/liftbridge/server/commitlog"
	"github.com/tylertreat/liftbridge/server/proto"
)

func TestCompactCleaner(t *testing.T) {
	req := require.New(t)
	var err error

	var msgSets []commitlog.MessageSet
	msgSets = append(msgSets, newMessageSet(0, &proto.Message{
		Key:       []byte("travisjeffery"),
		Value:     []byte("one tj"),
		MagicByte: 2,
		Timestamp: time.Now(),
	}))

	msgSets = append(msgSets, newMessageSet(1, &proto.Message{
		Key:       []byte("another"),
		Value:     []byte("one another"),
		MagicByte: 2,
		Timestamp: time.Now(),
	}))

	msgSets = append(msgSets, newMessageSet(2, &proto.Message{
		Key:       []byte("travisjeffery"),
		Value:     []byte("two tj"),
		MagicByte: 2,
		Timestamp: time.Now(),
	}))

	msgSets = append(msgSets, newMessageSet(3, &proto.Message{
		Key:       []byte("again another"),
		Value:     []byte("again another"),
		MagicByte: 2,
		Timestamp: time.Now(),
	}))

	path, err := ioutil.TempDir("", "cleanerTests")
	require.NoError(t, err)
	defer os.RemoveAll(path)

	opts := commitlog.Options{
		Path:            path,
		MaxSegmentBytes: int64(len(msgSets[0]) + len(msgSets[1])),
		MaxLogBytes:     1000,
	}
	l, err := commitlog.New(opts)
	require.NoError(t, err)

	for _, msgSet := range msgSets {
		_, err = l.Append(msgSet)
		require.NoError(t, err)
	}

	segments := l.Segments()
	req.Equal(2, len(l.Segments()))
	segment := segments[0]

	scanner := commitlog.NewSegmentScanner(segment)
	ms, err := scanner.Scan()
	require.NoError(t, err)
	require.Equal(t, msgSets[0], ms)

	cc := commitlog.NewCompactCleaner()
	cleaned, err := cc.Clean(segments)
	req.NoError(err)
	req.Equal(2, len(cleaned))

	scanner = commitlog.NewSegmentScanner(cleaned[0])

	var count int
	for i := 0; i < 1; i++ {
		ms, err = scanner.Scan()
		if err != nil {
			break
		}
		req.Equal(1, len(ms.Messages()))
		req.Equal([]byte("another"), ms.Messages()[0].Key())
		req.Equal([]byte("one another"), ms.Messages()[0].Value())
		count++
	}
	req.Equal(1, count)

	scanner = commitlog.NewSegmentScanner(cleaned[1])
	count = 0
	for i := 0; i < 1; i++ {
		ms, err = scanner.Scan()
		if err != nil {
			break
		}
		req.Equal(1, len(ms.Messages()))
		req.Equal([]byte("travisjeffery"), ms.Messages()[0].Key())
		req.Equal([]byte("two tj"), ms.Messages()[0].Value())
		count++
	}
	req.Equal(1, count)

	count = 0
	for i := 0; i < 1; i++ {
		ms, err = scanner.Scan()
		if err != nil {
			break
		}
		req.Equal(1, len(ms.Messages()))
		req.Equal([]byte("again another"), ms.Messages()[0].Key())
		req.Equal([]byte("again another"), ms.Messages()[0].Value())
		count++
	}
	req.Equal(1, count)
}

func newMessageSet(offset uint64, pmsgs ...*proto.Message) commitlog.MessageSet {
	cmsgs := make([]commitlog.Message, 0, len(pmsgs))
	for _, msg := range pmsgs {
		b, err := proto.Encode(msg)
		if err != nil {
			panic(err)
		}
		cmsgs = append(cmsgs, b)
	}
	return commitlog.NewMessageSet(offset, cmsgs...)
}
