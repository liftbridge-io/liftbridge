package commitlog

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/tysontate/gommap"

	"github.com/liftbridge-io/liftbridge/server/proto"
)

var (
	ErrIndexCorrupt = errors.New("corrupt index file")
)

const (
	offsetWidth    = 4
	timestampWidth = 8
	positionWidth  = 4
	sizeWidth      = 4
	entryWidth     = offsetWidth + timestampWidth + positionWidth + sizeWidth
)

type Index struct {
	options
	mmap     gommap.MMap
	file     *os.File
	size     int64
	mu       sync.RWMutex
	position int64
}

type Entry struct {
	Offset    int64
	Timestamp int64
	Position  int64
	Size      int32
}

// relEntry is an Entry relative to the base fileOffset
type relEntry struct {
	Offset    int32
	Timestamp int64
	Position  int32
	Size      int32
}

func newRelEntry(e *Entry, baseOffset int64) relEntry {
	return relEntry{
		Offset:    int32(e.Offset - baseOffset),
		Timestamp: e.Timestamp,
		Position:  int32(e.Position),
		Size:      e.Size,
	}
}

func (rel relEntry) fill(e *Entry, baseOffset int64) {
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

func NewIndex(opts options) (idx *Index, err error) {
	if opts.bytes == 0 {
		opts.bytes = 10 * 1024 * 1024
	}
	if opts.path == "" {
		return nil, errors.New("path is empty")
	}
	idx = &Index{
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
func (idx *Index) Position() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.position
}

func (idx *Index) WriteEntries(entries []*Entry) (err error) {
	b := new(bytes.Buffer)
	for _, entry := range entries {
		relEntry := newRelEntry(entry, idx.baseOffset)
		if err = binary.Write(b, proto.Encoding, relEntry); err != nil {
			return errors.Wrap(err, "binary write failed")
		}
	}
	idx.WriteAt(b.Bytes(), idx.position)
	idx.mu.Lock()
	idx.position += entryWidth * int64(len(entries))
	idx.mu.Unlock()
	return nil
}

// ReadEntryAtFileOffset is used to read an Index entry at the given
// byte offset of the Index file. ReadEntryAtLogOffset is generally
// more useful for higher level use.
func (idx *Index) ReadEntryAtFileOffset(e *Entry, fileOffset int64) (err error) {
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

// ReadEntryAtLogOffset is used to read an Index entry at the given
// log offset of the Index file.
func (idx *Index) ReadEntryAtLogOffset(e *Entry, logOffset int64) error {
	return idx.ReadEntryAtFileOffset(e, logOffset*entryWidth)
}

func (idx *Index) ReadAt(p []byte, offset int64) (n int, err error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.position < offset+entryWidth {
		return 0, io.EOF
	}
	n = copy(p, idx.mmap[offset:offset+entryWidth])
	return n, nil
}

func (idx *Index) WriteAt(p []byte, offset int64) (n int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Check if we need to expand the index file.
	if offset >= idx.size {
		// Expand the index file.
		newSize := roundDown(idx.size+idx.bytes, entryWidth)
		err := idx.file.Truncate(newSize)
		if err != nil {
			panic(errors.Wrap(err, "failed to expand index file"))
		}
		idx.size = newSize

		// Re-mmap the index.
		idx.mmap, err = gommap.Map(idx.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED)
		if err != nil {
			panic(errors.Wrap(err, "failed to mmap expanded index file"))
		}
	}

	return copy(idx.mmap[offset:], p)
}

func (idx *Index) Sync() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if err := idx.file.Sync(); err != nil {
		return errors.Wrap(err, "file sync failed")
	}
	if err := idx.mmap.Sync(gommap.MS_SYNC); err != nil {
		return errors.Wrap(err, "mmap sync failed")
	}
	return nil
}

func (idx *Index) Close() error {
	if err := idx.Sync(); err != nil {
		return err
	}
	if err := idx.Shrink(); err != nil {
		return err
	}
	return idx.file.Close()
}

// Shrink truncates the memory-mapped index file to the size of its contents.
func (idx *Index) Shrink() error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.file.Truncate(idx.position)
}

func (idx *Index) Name() string {
	return idx.file.Name()
}

func (idx *Index) TruncateEntries(number int) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if int64(number*entryWidth) > idx.position {
		return errors.New("bad truncate number")
	}
	idx.position = int64(number * entryWidth)
	return nil
}

func (idx *Index) InitializePosition() (*Entry, error) {
	// Find the first empty entry.
	n := int(idx.size / entryWidth)
	entry := new(Entry)
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
		return nil, ErrIndexCorrupt
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.position%entryWidth != 0 {
		return nil, ErrIndexCorrupt
	}
	return entry, nil
}

type IndexScanner struct {
	idx    *Index
	entry  *Entry
	offset int64
}

func NewIndexScanner(idx *Index) *IndexScanner {
	return &IndexScanner{idx: idx, entry: &Entry{}}
}

func (s *IndexScanner) Scan() (*Entry, error) {
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
