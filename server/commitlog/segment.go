package commitlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"
)

const (
	fileFormat      = "%020d%s"
	logSuffix       = ".log"
	cleanedSuffix   = ".cleaned"
	truncatedSuffix = ".truncated"
	indexSuffix     = ".index"
)

var (
	// ErrEntryNotFound is returned when a segment search cannot find a
	// specific entry.
	ErrEntryNotFound = errors.New("entry not found")

	// ErrSegmentClosed is returned on writes to a closed segment.
	ErrSegmentClosed = errors.New("segment has been closed")

	// ErrSegmentExists is returned when attempting to create a segment that
	// already exists.
	ErrSegmentExists = errors.New("segment already exists")

	// timestamp returns the current time in Unix nanoseconds. This function
	// exists for mocking purposes.
	timestamp = func() int64 { return time.Now().UnixNano() }
)

type Segment struct {
	writer         io.Writer
	reader         io.Reader
	log            *os.File
	Index          *Index
	BaseOffset     int64
	nextOffset     int64
	firstWriteTime int64
	lastWriteTime  int64
	position       int64
	maxBytes       int64
	path           string
	suffix         string
	waiters        map[io.Reader]chan struct{}
	sealed         bool
	closed         bool

	sync.RWMutex
}

func NewSegment(path string, baseOffset, maxBytes int64, isNew bool, args ...interface{}) (*Segment, error) {
	var suffix string
	if len(args) != 0 {
		suffix = args[0].(string)
	}
	s := &Segment{
		maxBytes:   maxBytes,
		BaseOffset: baseOffset,
		nextOffset: baseOffset,
		path:       path,
		suffix:     suffix,
		waiters:    make(map[io.Reader]chan struct{}),
	}
	// If this is a new segment, ensure the file doesn't already exist.
	if isNew {
		if _, err := os.Stat(s.logPath()); err == nil {
			// Segment file already exists.
			return nil, ErrSegmentExists
		}
	}
	log, err := os.OpenFile(s.logPath(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}
	info, err := log.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat file failed")
	}
	s.log = log
	s.position = info.Size()
	s.writer = log
	s.reader = log
	err = s.setupIndex()
	return s, err
}

// setupIndex creates and initializes an Index.
// Initialization is:
// - Initialize Index position
// - Initialize Segment nextOffset
// - Initialize firstWriteTime/lastWriteTime
func (s *Segment) setupIndex() (err error) {
	s.Index, err = NewIndex(options{
		path:       s.indexPath(),
		baseOffset: s.BaseOffset,
	})
	if err != nil {
		return err
	}
	lastEntry, err := s.Index.InitializePosition()
	if err != nil {
		return err
	}
	// If lastEntry is nil, the index is empty.
	if lastEntry != nil {
		s.nextOffset = lastEntry.Offset + 1
		s.lastWriteTime = lastEntry.Timestamp
		// Read the first entry to get firstWriteTime.
		var firstEntry Entry
		if err := s.Index.ReadEntryAtFileOffset(&firstEntry, 0); err != nil {
			return err
		}
		s.firstWriteTime = firstEntry.Timestamp
	}
	return nil
}

// CheckSplit determines if a new log segment should be rolled out either
// because this segment is full or LogRollTime has passed since the first
// message was written to the segment.
func (s *Segment) CheckSplit(logRollTime time.Duration) bool {
	s.RLock()
	defer s.RUnlock()
	if s.position >= s.maxBytes {
		return true
	}
	if logRollTime == 0 || s.firstWriteTime == 0 {
		// Don't roll a new segment if there have been no writes to the segment
		// or LogRollTime is disabled.
		return false
	}
	// Check if LogRollTime has passed since first write.
	return timestamp()-s.firstWriteTime >= int64(logRollTime)
}

// Seal a segment from being written to. This is called on the former active
// segment after a new segment is rolled. This is a no-op if the segment is
// already sealed.
func (s *Segment) Seal() {
	s.Lock()
	defer s.Unlock()
	if s.sealed {
		return
	}
	s.sealed = true
	// Notify any readers waiting for data.
	s.notifyWaiters()
}

func (s *Segment) NextOffset() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.nextOffset
}

func (s *Segment) Position() int64 {
	s.RLock()
	defer s.RUnlock()
	return s.position
}

func (s *Segment) WriteMessageSet(ms []byte, entries []*Entry) error {
	if _, err := s.Write(ms, entries); err != nil {
		return err
	}
	return s.Index.WriteEntries(entries)
}

// Write writes a byte slice to the log at the current position.
// It increments the offset as well as sets the position to the new tail.
func (s *Segment) Write(p []byte, entries []*Entry) (n int, err error) {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return 0, ErrSegmentClosed
	}
	n, err = s.writer.Write(p)
	if err != nil {
		return n, errors.Wrap(err, "log write failed")
	}
	numMsgs := len(entries)
	s.nextOffset += int64(numMsgs)
	s.position += int64(n)
	if s.firstWriteTime == 0 {
		s.firstWriteTime = entries[0].Timestamp
	}
	s.lastWriteTime = entries[numMsgs-1].Timestamp
	s.notifyWaiters()
	return n, nil
}

func (s *Segment) ReadAt(p []byte, off int64) (n int, err error) {
	s.RLock()
	defer s.RUnlock()
	if s.closed {
		// Return EOF since this typically happens as a result of reading from
		// a segment that has been sealed, so the caller knows to jump to the
		// next segment.
		return 0, io.EOF
	}
	return s.log.ReadAt(p, off)
}

func (s *Segment) notifyWaiters() {
	for r, ch := range s.waiters {
		close(ch)
		delete(s.waiters, r)
	}
}

func (s *Segment) waitForData(r io.Reader, pos int64) <-chan struct{} {
	wait := make(chan struct{})
	s.Lock()
	// Check if data has been written and/or the segment was filled.
	if s.position > pos || s.position >= s.maxBytes {
		close(wait)
	} else {
		s.waiters[r] = wait
	}
	s.Unlock()
	return wait
}

func (s *Segment) removeWaiter(r io.Reader) {
	s.Lock()
	delete(s.waiters, r)
	s.Unlock()
}

func (s *Segment) Close() error {
	s.Lock()
	defer s.Unlock()
	if err := s.log.Close(); err != nil {
		return err
	}
	if err := s.Index.Close(); err != nil {
		return err
	}
	s.closed = true
	return nil
}

// Cleaner creates a cleaner segment for this segment.
func (s *Segment) Cleaner() (*Segment, error) {
	return NewSegment(s.path, s.BaseOffset, s.maxBytes, true, cleanedSuffix)
}

// Replace replaces the given segment with the callee.
func (s *Segment) Replace(old *Segment) (err error) {
	if err = old.Close(); err != nil {
		return err
	}
	if err = s.Close(); err != nil {
		return err
	}
	if err = os.Rename(s.logPath(), old.logPath()); err != nil {
		return err
	}
	if err = os.Rename(s.indexPath(), old.indexPath()); err != nil {
		return err
	}
	s.suffix = ""
	log, err := os.OpenFile(s.logPath(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return errors.Wrap(err, "open file failed")
	}
	s.log = log
	s.writer = log
	s.reader = log
	s.closed = false
	return s.setupIndex()
}

// findEntry returns the first entry whose offset is greater than or equal to
// the given offset.
func (s *Segment) findEntry(offset int64) (e *Entry, err error) {
	s.RLock()
	defer s.RUnlock()
	e = &Entry{}
	n := int(s.Index.Position() / entryWidth)
	idx := sort.Search(n, func(i int) bool {
		if err := s.Index.ReadEntryAtFileOffset(e, int64(i*entryWidth)); err != nil {
			panic(err)
		}
		return e.Offset >= offset
	})
	if idx == n {
		return nil, ErrEntryNotFound
	}
	err = s.Index.ReadEntryAtFileOffset(e, int64(idx*entryWidth))
	return e, err
}

// findEntryByTimestamp returns the first entry whose timestamp is greater than
// or equal to the given offset.
func (s *Segment) findEntryByTimestamp(timestamp int64) (e *Entry, err error) {
	s.RLock()
	defer s.RUnlock()
	e = &Entry{}
	n := int(s.Index.Position() / entryWidth)
	idx := sort.Search(n, func(i int) bool {
		if err := s.Index.ReadEntryAtFileOffset(e, int64(i*entryWidth)); err != nil {
			panic(err)
		}
		return e.Timestamp >= timestamp
	})
	if idx == n {
		return nil, ErrEntryNotFound
	}
	err = s.Index.ReadEntryAtFileOffset(e, int64(idx*entryWidth))
	return e, err
}

// Delete closes the segment and then deletes its log and index files.
func (s *Segment) Delete() error {
	if err := s.Close(); err != nil {
		return err
	}
	s.Lock()
	defer s.Unlock()
	if err := os.Remove(s.log.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.Index.Name()); err != nil {
		return err
	}
	return nil
}

type SegmentScanner struct {
	s  *Segment
	is *IndexScanner
}

func NewSegmentScanner(segment *Segment) *SegmentScanner {
	return &SegmentScanner{s: segment, is: NewIndexScanner(segment.Index)}
}

// Scan should be called repeatedly to iterate over the messages in the
// segment, it will return io.EOF when there are no more messages.
func (s *SegmentScanner) Scan() (MessageSet, *Entry, error) {
	entry, err := s.is.Scan()
	if err != nil {
		return nil, nil, err
	}
	header := make(MessageSet, msgSetHeaderLen)
	_, err = s.s.ReadAt(header, entry.Position)
	if err != nil {
		return nil, nil, err
	}
	payload := make([]byte, header.Size())
	_, err = s.s.ReadAt(payload, entry.Position+msgSetHeaderLen)
	if err != nil {
		return nil, nil, err
	}
	msgSet := append(header, payload...)
	return msgSet, entry, nil
}

func (s *Segment) logPath() string {
	return filepath.Join(s.path, fmt.Sprintf(fileFormat, s.BaseOffset, logSuffix+s.suffix))
}

func (s *Segment) indexPath() string {
	return filepath.Join(s.path, fmt.Sprintf(fileFormat, s.BaseOffset, indexSuffix+s.suffix))
}
