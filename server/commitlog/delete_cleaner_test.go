package commitlog

import (
	"io/ioutil"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/liftbridge-io/liftbridge/server/logger"
	"github.com/liftbridge-io/liftbridge/server/proto"
)

func noopLogger() logger.Logger {
	return &log.Logger{Out: ioutil.Discard}
}

func createSegment(t require.TestingT, dir string, baseOffset, maxBytes int64) *Segment {
	s, err := NewSegment(dir, baseOffset, maxBytes, false)
	require.NoError(t, err)
	return s
}

// Ensure Clean is a no-op when there are no segments.
func TestDeleteCleanerNoSegments(t *testing.T) {
	opts := DeleteCleanerOptions{Name: "foo", Logger: noopLogger()}
	opts.Retention.Bytes = 100
	cleaner := NewDeleteCleaner(opts)
	segments, err := cleaner.Clean(nil)
	require.NoError(t, err)
	require.Nil(t, segments)
}

// Ensure Clean is a no-op when bytes and messages are 0.
func TestDeleteCleanerNoRetentionSet(t *testing.T) {
	opts := DeleteCleanerOptions{Name: "foo", Logger: noopLogger()}
	cleaner := NewDeleteCleaner(opts)
	dir := tempDir(t)
	defer remove(t, dir)

	expected := []*Segment{createSegment(t, dir, 0, 100)}
	actual, err := cleaner.Clean(expected)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

// Ensure Clean is a no-op when there is only one segment.
func TestDeleteCleanerOneSegment(t *testing.T) {
	opts := DeleteCleanerOptions{Name: "foo", Logger: noopLogger()}
	opts.Retention.Bytes = 100
	cleaner := NewDeleteCleaner(opts)
	dir := tempDir(t)
	defer remove(t, dir)

	expected := []*Segment{createSegment(t, dir, 0, 100)}
	actual, err := cleaner.Clean(expected)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

// Ensure Clean deletes segments to maintain the bytes limit.
func TestDeleteCleanerBytes(t *testing.T) {
	opts := DeleteCleanerOptions{Name: "foo", Logger: noopLogger()}
	opts.Retention.Bytes = 50
	cleaner := NewDeleteCleaner(opts)
	dir := tempDir(t)
	defer remove(t, dir)

	segs := make([]*Segment, 5)
	for i := 0; i < 5; i++ {
		segs[i] = createSegment(t, dir, int64(i), 20)
		segs[i].Write(make([]byte, 20), []*Entry{&Entry{}})
	}
	actual, err := cleaner.Clean(segs)
	require.NoError(t, err)
	require.Len(t, actual, 2)
	require.Equal(t, int64(3), actual[0].BaseOffset)
	require.Equal(t, int64(4), actual[1].BaseOffset)
}

// Ensure Clean is a no-op when there are segments and a bytes limit but the
// segments don't exceed the limit.
func TestDeleteCleanerBytesBelowLimit(t *testing.T) {
	opts := DeleteCleanerOptions{Name: "foo", Logger: noopLogger()}
	opts.Retention.Bytes = 50
	cleaner := NewDeleteCleaner(opts)
	dir := tempDir(t)
	defer remove(t, dir)

	expected := make([]*Segment, 5)
	for i := 0; i < 5; i++ {
		expected[i] = createSegment(t, dir, int64(i), 20)
	}
	actual, err := cleaner.Clean(expected)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

// Ensure Clean deletes segments to maintain the messages limit.
func TestDeleteCleanerMessages(t *testing.T) {
	opts := DeleteCleanerOptions{Name: "foo", Logger: noopLogger()}
	opts.Retention.Messages = 10
	cleaner := NewDeleteCleaner(opts)
	dir := tempDir(t)
	defer remove(t, dir)

	segs := make([]*Segment, 20)
	for i := 0; i < 20; i++ {
		segs[i] = createSegment(t, dir, int64(i), 20)
		segs[i].Write(make([]byte, 20), []*Entry{&Entry{}})
	}
	actual, err := cleaner.Clean(segs)
	require.NoError(t, err)
	require.Len(t, actual, 10)
	for i := 0; i < 10; i++ {
		require.Equal(t, int64(i+10), actual[i].BaseOffset)
	}
}

// Ensure Clean is a no-op when there are segments and a messages limit but the
// segments don't exceed the limit.
func TestDeleteCleanerMessagesBelowLimit(t *testing.T) {
	opts := DeleteCleanerOptions{Name: "foo", Logger: noopLogger()}
	opts.Retention.Messages = 100
	cleaner := NewDeleteCleaner(opts)
	dir := tempDir(t)
	defer remove(t, dir)

	expected := make([]*Segment, 5)
	for i := 0; i < 5; i++ {
		expected[i] = createSegment(t, dir, int64(i), 20)
	}
	actual, err := cleaner.Clean(expected)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

// Ensure Clean deletes segments to maintain the messages and bytes limits.
func TestDeleteCleanerBytesMessages(t *testing.T) {
	opts := DeleteCleanerOptions{Name: "foo", Logger: noopLogger()}
	opts.Retention.Messages = 15
	opts.Retention.Bytes = 100
	cleaner := NewDeleteCleaner(opts)
	dir := tempDir(t)
	defer remove(t, dir)

	segs := make([]*Segment, 20)
	for i := 0; i < 20; i++ {
		segs[i] = createSegment(t, dir, int64(i), 20)
		segs[i].Write(make([]byte, 20), []*Entry{&Entry{}})
	}
	actual, err := cleaner.Clean(segs)
	require.NoError(t, err)
	require.Len(t, actual, 5)
	for i := 0; i < 5; i++ {
		require.Equal(t, int64(i+15), actual[i].BaseOffset)
	}
}

// Ensure Clean deletes segments to maintain the message age limit.
func TestDeleteCleanerAge(t *testing.T) {
	computeTTLBefore := computeTTL
	computeTTL = func(age time.Duration) int64 {
		return 200 - int64(age)
	}
	defer func() {
		computeTTL = computeTTLBefore
	}()

	opts := DeleteCleanerOptions{Name: "foo", Logger: noopLogger()}
	opts.Retention.Age = 100
	cleaner := NewDeleteCleaner(opts)
	dir := tempDir(t)
	defer remove(t, dir)

	segs := make([]*Segment, 20)
	for i := 0; i < 20; i++ {
		segs[i] = createSegment(t, dir, int64(i), 20)
		ms, entries, err := NewMessageSetFromProto(int64(i), 0,
			[]*proto.Message{&proto.Message{Timestamp: int64(i * 10)}})
		require.NoError(t, err)
		require.NoError(t, segs[i].WriteMessageSet(ms, entries))
	}
	actual, err := cleaner.Clean(segs)
	require.NoError(t, err)
	require.Len(t, actual, 10)
	for i := 0; i < 10; i++ {
		require.Equal(t, int64(i+10), actual[i].BaseOffset)
	}
}

// Ensure Clean is a no-op when there are segments and an age limit but the
// segments don't exceed the limit.
func TestDeleteCleanerMessagesBelowAgeLimit(t *testing.T) {
	computeTTLBefore := computeTTL
	computeTTL = func(age time.Duration) int64 {
		return 50 - int64(age)
	}
	defer func() {
		computeTTL = computeTTLBefore
	}()

	opts := DeleteCleanerOptions{Name: "foo", Logger: noopLogger()}
	opts.Retention.Age = 50
	cleaner := NewDeleteCleaner(opts)
	dir := tempDir(t)
	defer remove(t, dir)

	expected := make([]*Segment, 5)
	for i := 0; i < 5; i++ {
		expected[i] = createSegment(t, dir, int64(i), 20)
		ms, entries, err := NewMessageSetFromProto(int64(i), 0,
			[]*proto.Message{&proto.Message{Timestamp: int64(i * 10)}})
		require.NoError(t, err)
		require.NoError(t, expected[i].WriteMessageSet(ms, entries))
	}
	actual, err := cleaner.Clean(expected)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
