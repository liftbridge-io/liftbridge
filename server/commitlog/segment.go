package commitlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/pkg/errors"
)

const (
	fileFormat      = "%020d%s"
	logSuffix       = ".log"
	cleanedSuffix   = ".cleaned"
	truncatedSuffix = ".truncated"
	indexSuffix     = ".index"
)

// ErrEntryNotFound is returned when a segment search cannot find a specific
// entry.
var ErrEntryNotFound = errors.New("entry not found")

type Segment struct {
	writer     io.Writer
	reader     io.Reader
	log        *os.File
	Index      *Index
	BaseOffset int64
	nextOffset int64
	position   int64
	maxBytes   int64
	path       string
	suffix     string
	waiters    map[io.Reader]chan struct{}

	sync.RWMutex
}

func NewSegment(path string, baseOffset, maxBytes int64, args ...interface{}) (*Segment, error) {
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
	if lastEntry != nil {
		s.nextOffset = lastEntry.Offset + 1
	}
	return nil
}

func (s *Segment) IsFull() bool {
	s.RLock()
	defer s.RUnlock()
	return s.position >= s.maxBytes
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

func (s *Segment) WriteMessageSet(ms []byte, entries []Entry) error {
	if _, err := s.Write(ms, len(entries)); err != nil {
		return err
	}
	return s.Index.WriteEntries(entries)
}

// Write writes a byte slice to the log at the current position.
// It increments the offset as well as sets the position to the new tail.
func (s *Segment) Write(p []byte, numMsgs int) (n int, err error) {
	s.Lock()
	defer s.Unlock()
	n, err = s.writer.Write(p)
	if err != nil {
		return n, errors.Wrap(err, "log write failed")
	}
	s.nextOffset += int64(numMsgs)
	s.position += int64(n)
	s.notifyWaiters()
	return n, nil
}

func (s *Segment) ReadAt(p []byte, off int64) (n int, err error) {
	s.RLock()
	defer s.RUnlock()
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
	return s.Index.Close()
}

// Cleaner creates a cleaner segment for this segment.
func (s *Segment) Cleaner() (*Segment, error) {
	return NewSegment(s.path, s.BaseOffset, s.maxBytes, cleanedSuffix)
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
	return s.setupIndex()
}

// findEntry returns the first entry whose offset is greater than or equal to
// the given offset.
func (s *Segment) findEntry(offset int64) (e *Entry, err error) {
	s.RLock()
	defer s.RUnlock()
	e = &Entry{}
	n := int(s.Index.bytes / entryWidth)
	idx := sort.Search(n, func(i int) bool {
		if err := s.Index.ReadEntryAtFileOffset(e, int64(i*entryWidth)); err != nil {
			return true
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
	n := int(s.Index.bytes / entryWidth)
	idx := sort.Search(n, func(i int) bool {
		if err := s.Index.ReadEntryAtFileOffset(e, int64(i*entryWidth)); err != nil {
			return true
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
func (s *SegmentScanner) Scan() (ms MessageSet, err error) {
	entry, err := s.is.Scan()
	if err != nil {
		return nil, err
	}
	header := make(MessageSet, msgSetHeaderLen)
	_, err = s.s.ReadAt(header, entry.Position)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, header.Size())
	_, err = s.s.ReadAt(payload, entry.Position+msgSetHeaderLen)
	if err != nil {
		return nil, err
	}
	msgSet := append(header, payload...)
	return msgSet, nil
}

func (s *Segment) logPath() string {
	return filepath.Join(s.path, fmt.Sprintf(fileFormat, s.BaseOffset, logSuffix+s.suffix))
}

func (s *Segment) indexPath() string {
	return filepath.Join(s.path, fmt.Sprintf(fileFormat, s.BaseOffset, indexSuffix+s.suffix))
}
