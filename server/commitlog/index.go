package commitlog

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/tysonmote/gommap"
	"github.com/pkg/errors"

	proto "github.com/liftbridge-io/liftbridge/server/protocol"
)

var errIndexCorrupt = errors.New("corrupt index file")

const (
	offsetWidth    = 4
	timestampWidth = 8
	positionWidth  = 4
	sizeWidth      = 4
	entryWidth     = offsetWidth + timestampWidth + positionWidth + sizeWidth
)

type index struct {
	options
	mmap     gommap.MMap
	file     *os.File
	size     int64
	mu       sync.RWMutex
	position int64
	closed   bool
}

type entry struct {
	Offset      int64
	Timestamp   int64
	LeaderEpoch uint64
	Position    int64
	Size        int32
}

// relEntry is an Entry relative to the base fileOffset
type relEntry struct {
	Offset    int32
	Timestamp int64
	Position  int32
	Size      int32
}

func newRelEntry(e *entry, baseOffset int64) relEntry {
	return relEntry{
		Offset:    int32(e.Offset - baseOffset),
		Timestamp: e.Timestamp,
		Position:  int32(e.Position),
		Size:      e.Size,
	}
}

func (rel relEntry) fill(e *entry, baseOffset int64) {
	e.Offset = baseOffset + int64(rel.Offset)
	e.Timestamp = rel.Timestamp
	e.Position = int64(rel.Position)
	e.Size = rel.Size
}

type options struct {
	path       string
	bytes      int64
	baseOffset int64
}

func newIndex(opts options) (idx *index, err error) {
	if opts.bytes == 0 {
		opts.bytes = 10 * 1024 * 1024
	}
	if opts.path == "" {
		return nil, errors.New("path is empty")
	}
	idx = &index{
		options: opts,
	}
	idx.file, err = os.OpenFile(opts.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}
	fi, err := idx.file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat file failed")
	}
	// Pre-allocate the index if we just created it.
	if fi.Size() == 0 {
		if err := idx.file.Truncate(roundDown(opts.bytes, entryWidth)); err != nil {
			return nil, err
		}
	}
	// Get updated stats after resize.
	fi, err = idx.file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat file failed")
	}
	idx.position = fi.Size()
	idx.size = fi.Size()

	idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
	if err != nil {
		return nil, errors.Wrap(err, "mmap file failed")
	}
	return idx, nil
}

// Position returns the current position in the index to write to next. This
// value also represents the total length of the index.
func (idx *index) Position() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.position
}

func (idx *index) CountEntries() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.position / entryWidth
}

func (idx *index) writeEntries(entries []*entry) (err error) {
	b := new(bytes.Buffer)
	for _, entry := range entries {
		relEntry := newRelEntry(entry, idx.baseOffset)
		if err = binary.Write(b, proto.Encoding, relEntry); err != nil {
			return errors.Wrap(err, "binary write failed")
		}
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.closed {
		return ErrSegmentClosed
	}
	if err := idx.writeAt(b.Bytes(), idx.position); err != nil {
		return errors.Wrap(err, "index write failed")
	}
	idx.position += entryWidth * int64(len(entries))
	return nil
}

// ReadEntryAtFileOffset is used to read an index entry at the given
// byte offset of the index file. ReadEntryAtLogOffset is generally
// more useful for higher level use.
func (idx *index) ReadEntryAtFileOffset(e *entry, fileOffset int64) (err error) {
	p := make([]byte, entryWidth)
	if _, err = idx.ReadAt(p, fileOffset); err != nil {
		return err
	}
	b := bytes.NewReader(p)
	rel := &relEntry{}
	err = binary.Read(b, proto.Encoding, rel)
	if err != nil {
		return errors.Wrap(err, "binary read failed")
	}
	idx.mu.RLock()
	rel.fill(e, idx.baseOffset)
	idx.mu.RUnlock()
	return nil
}

// ReadEntryAtLogOffset is used to read an index entry at the given
// log offset of the index file.
func (idx *index) ReadEntryAtLogOffset(e *entry, logOffset int64) error {
	return idx.ReadEntryAtFileOffset(e, logOffset*entryWidth)
}

func (idx *index) ReadAt(p []byte, offset int64) (n int, err error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.closed {
		return 0, ErrSegmentClosed
	}
	if idx.position < offset+entryWidth {
		return 0, io.EOF
	}
	n = copy(p, idx.mmap[offset:offset+entryWidth])
	return n, nil
}

func (idx *index) writeAt(p []byte, offset int64) error {
	// Check if we need to expand the index file.
	if pSize := int64(len(p)); offset+pSize >= idx.size {
		// Expand the index file.
		newSize := roundDown(idx.size+idx.bytes, entryWidth)
		if newSize < offset+pSize {
			newSize = idx.size + pSize
		}
		err := idx.file.Truncate(newSize)
		if err != nil {
			panic(errors.Wrap(err, "failed to expand index file"))
		}
		idx.size = newSize

		// Re-mmap the index.
		oldMmap := idx.mmap
		idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
		if err != nil {
			panic(errors.Wrap(err, "failed to mmap expanded index file"))
		}
		// Unmap the old index.
		if err := oldMmap.UnsafeUnmap(); err != nil {
			return errors.Wrap(err, "failed to unmap memory mapped index file")
		}
	}

	copy(idx.mmap[offset:], p)
	return nil
}

func (idx *index) Sync() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.sync()
}

func (idx *index) sync() error {
	if idx.closed {
		return ErrSegmentClosed
	}
	if err := idx.file.Sync(); err != nil {
		return errors.Wrap(err, "file sync failed")
	}
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return errors.Wrap(err, "mmap sync failed")
	}
	return nil
}

func (idx *index) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.closed {
		return nil
	}
	if err := idx.sync(); err != nil {
		return err
	}
	if err := idx.shrink(); err != nil {
		return err
	}
	if err := idx.file.Close(); err != nil {
		return err
	}
	if err := idx.mmap.UnsafeUnmap(); err != nil {
		return err
	}
	idx.closed = true
	return nil
}

// Shrink truncates the memory-mapped index file to the size of its contents.
func (idx *index) Shrink() error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.shrink()
}

func (idx *index) shrink() error {
	return idx.file.Truncate(idx.position)
}

func (idx *index) Name() string {
	return idx.file.Name()
}

func (idx *index) InitializePosition() (*entry, error) {
	// Find the first empty entry.
	n := int(idx.size / entryWidth)
	entry := new(entry)
	i := sort.Search(n, func(i int) bool {
		if err := idx.ReadEntryAtFileOffset(entry, int64(i*entryWidth)); err != nil {
			panic(err)
		}
		return entry.Position == 0 && entry.Timestamp == 0 && entry.Size == 0
	})
	// Initialize the position.
	idx.mu.Lock()
	idx.position = int64(i * entryWidth)
	idx.mu.Unlock()

	if i == 0 {
		// Index is empty.
		return nil, nil
	}

	// Return the last entry in the index.
	i--
	if err := idx.ReadEntryAtFileOffset(entry, int64(i*entryWidth)); err != nil {
		return nil, err
	}
	// Do some sanity checks.
	if entry.Offset < idx.baseOffset {
		return nil, errIndexCorrupt
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.position%entryWidth != 0 {
		return nil, errIndexCorrupt
	}
	return entry, nil
}

type indexScanner struct {
	idx    *index
	entry  *entry
	offset int64
}

func newIndexScanner(idx *index) *indexScanner {
	return &indexScanner{idx: idx, entry: &entry{}}
}

func (s *indexScanner) Scan() (*entry, error) {
	err := s.idx.ReadEntryAtLogOffset(s.entry, s.offset)
	if err != nil {
		return nil, err
	}
	if s.entry.Offset == 0 && s.offset != 0 {
		return nil, io.EOF
	}
	s.offset++
	return s.entry, err
}
