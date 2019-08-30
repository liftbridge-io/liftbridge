package commitlog

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	pkgErrors "github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/dustin/go-humanize/english"
	atomic_file "github.com/natefinch/atomic"

	"github.com/liftbridge-io/liftbridge/server/logger"
)

const (
	leaderEpochFileName = "leader-epoch-checkpoint"
	leaderEpochFileV0   = 0
)

// epochOffset contains the start offset for a given leader epoch.
type epochOffset struct {
	leaderEpoch uint64
	startOffset int64
}

type leaderEpochCache struct {
	epochOffsets   []*epochOffset
	mu             sync.RWMutex
	checkpointFile string
	name           string
	log            logger.Logger
}

func newLeaderEpochCacheNoFile(name string, log logger.Logger) *leaderEpochCache {
	return &leaderEpochCache{
		epochOffsets: []*epochOffset{},
		log:          log,
		name:         name,
	}
}

func newLeaderEpochCache(name, path string, log logger.Logger) (*leaderEpochCache, error) {
	var (
		file   = filepath.Join(path, leaderEpochFileName)
		epochs = []*epochOffset{}
	)
	if _, err := os.Stat(file); err == nil {
		// Load epoch offsets from file.
		f, err := os.Open(file)
		if err != nil {
			return nil, pkgErrors.Wrap(err, "failed to open leader epoch offsets file")
		}
		epochs, err = readLeaderEpochOffsets(f)
		if err != nil {
			return nil, pkgErrors.Wrap(err, "failed to read leader epoch offsets file")
		}
	}
	return &leaderEpochCache{
		epochOffsets:   epochs,
		checkpointFile: file,
		log:            log,
		name:           name,
	}, nil
}

// Assign the given leader epoch to the given offset. Once assigned, an epoch
// cannot be reassigned.
func (l *leaderEpochCache) Assign(epoch uint64, offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.assign(epoch, offset)
}

// LastOffsetForLeaderEpoch returns the start offset of the first leader epoch
// larger than the provided one or -1 if the current epoch equals the provided
// one.
func (l *leaderEpochCache) LastOffsetForLeaderEpoch(epoch uint64) int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	e := l.findEpoch(epoch + 1)
	if e == nil {
		return -1
	}
	return e.startOffset
}

// LastLeaderEpoch returns the latest leader epoch for the log.
func (l *leaderEpochCache) LastLeaderEpoch() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.latestEpoch()
}

// ClearLatest removes all leader epoch entries from the cache with start
// offsets greater than or equal to the given offset.
func (l *leaderEpochCache) ClearLatest(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset > l.latestOffset() {
		return nil
	}
	filtered := make([]*epochOffset, 0, len(l.epochOffsets))
	for _, epoch := range l.epochOffsets {
		if epoch.startOffset < offset {
			filtered = append(filtered, epoch)
		}
	}
	removed := len(l.epochOffsets) - len(filtered)
	l.epochOffsets = filtered
	err := l.flush()
	if err == nil {
		l.log.Debugf("Removed latest %s from leader epoch cache based on passed offset %d "+
			"leaving %d in epoch file for log %s",
			english.Plural(removed, "entry", ""), offset, len(l.epochOffsets), l.name)
	}
	return pkgErrors.Wrap(err, "failed to flush epoch offsets")
}

// ClearEarliest searches for the oldest leader epoch < offset, updates the
// saved epoch offset to the given offset, then removes any previous epoch
// entries.
func (l *leaderEpochCache) ClearEarliest(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.earliestOffset() >= offset {
		return nil
	}
	var (
		earliest = make([]*epochOffset, 0, len(l.epochOffsets))
		removed  = 0
	)
	for _, epoch := range l.epochOffsets {
		if epoch.startOffset < offset {
			earliest = append(earliest, epoch)
			removed++
		}
	}
	if len(earliest) == 0 {
		return nil
	}
	l.epochOffsets = l.epochOffsets[removed:]
	// If the offset is less than the earliest offset remaining, add
	// previous epoch back but with an updated offset.
	if offset < l.earliestOffset() || len(l.epochOffsets) == 0 {
		l.epochOffsets = append(l.epochOffsets, &epochOffset{
			leaderEpoch: earliest[len(earliest)-1].leaderEpoch,
			startOffset: offset,
		})
		removed--
	}
	err := l.flush()
	if err == nil {
		l.log.Debugf("Removed earliest %s from leader epoch cache based on passed offset %d "+
			"leaving %d in epoch file for log %s",
			english.Plural(removed, "entry", ""), offset, len(l.epochOffsets), l.name)
	}
	return pkgErrors.Wrap(err, "failed to flush epoch offsets")
}

// Rebase adds the leader epoch offsets from the given leaderEpochCache
// starting at the given offset.
func (l *leaderEpochCache) Rebase(from *leaderEpochCache, offset int64) error {
	from.mu.RLock()
	defer from.mu.RUnlock()

	i := sort.Search(len(from.epochOffsets), func(i int) bool {
		return from.epochOffsets[i].startOffset >= offset
	})

	l.mu.Lock()
	defer l.mu.Unlock()
	for _, epoch := range from.epochOffsets[i:] {
		if epoch.leaderEpoch > l.latestEpoch() {
			if err := l.assign(epoch.leaderEpoch, epoch.startOffset); err != nil {
				return err
			}
		}
	}

	return l.flush()
}

// Replace the contents of the cache using the provided one.
func (l *leaderEpochCache) Replace(from *leaderEpochCache) error {
	from.mu.RLock()
	defer from.mu.RUnlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.epochOffsets = from.epochOffsets
	return l.flush()
}

func (l *leaderEpochCache) earliestOffset() int64 {
	if len(l.epochOffsets) == 0 {
		return -1
	}
	return l.epochOffsets[0].startOffset
}

func (l *leaderEpochCache) latestEpoch() uint64 {
	if len(l.epochOffsets) == 0 {
		return 0
	}
	return l.epochOffsets[len(l.epochOffsets)-1].leaderEpoch
}

func (l *leaderEpochCache) latestOffset() int64 {
	if len(l.epochOffsets) == 0 {
		return -1
	}
	return l.epochOffsets[len(l.epochOffsets)-1].startOffset
}

func (l *leaderEpochCache) findEpoch(epoch uint64) *epochOffset {
	i := sort.Search(len(l.epochOffsets), func(i int) bool {
		return l.epochOffsets[i].leaderEpoch >= epoch
	})
	if i < len(l.epochOffsets) {
		return l.epochOffsets[i]
	}
	return nil
}

func (l *leaderEpochCache) assign(epoch uint64, offset int64) error {
	var (
		latestEpoch  = l.latestEpoch()
		latestOffset = l.latestOffset()
	)
	if epoch > latestEpoch && offset >= latestOffset {
		l.epochOffsets = append(l.epochOffsets, &epochOffset{
			leaderEpoch: epoch,
			startOffset: offset,
		})
		if err := l.flush(); err != nil {
			return pkgErrors.Wrap(err, "failed to flush epoch offsets")
		}
		l.log.Debugf("Updated log leader epoch. %s. Cache now contains %s.",
			l.epochChangeMsg(epoch, latestEpoch, offset, latestOffset),
			english.Plural(len(l.epochOffsets), "entry", ""))
	} else {
		l.warn(epoch, latestEpoch, offset, latestOffset)
	}
	return nil
}

// flush writes the cached epoch offsets to disk in the following format:
//
// v0:
// version
// num_entries
// leader_epoch start_offset
// leader_epoch start_offset
// ...
func (l *leaderEpochCache) flush() error {
	if l.checkpointFile == "" {
		return nil
	}
	b := new(bytes.Buffer)
	if _, err := b.WriteString(fmt.Sprintf("%d\n", leaderEpochFileV0)); err != nil {
		return err
	}
	if _, err := b.WriteString(fmt.Sprintf("%d\n", len(l.epochOffsets))); err != nil {
		return err
	}
	for _, epoch := range l.epochOffsets {
		if _, err := b.WriteString(fmt.Sprintf("%d %d\n", epoch.leaderEpoch, epoch.startOffset)); err != nil {
			return err
		}
	}
	return atomic_file.WriteFile(l.checkpointFile, b)
}

func (l *leaderEpochCache) warn(epoch, latestEpoch uint64, offset, latestOffset int64) {
	if epoch < l.latestEpoch() {
		l.log.Warnf("Received log leader epoch assignment for an epoch < latest epoch. "+
			"This implies messages have arrived out of order. %s",
			l.epochChangeMsg(epoch, latestEpoch, offset, latestOffset))
	} else if offset < l.latestOffset() {
		l.log.Warnf("Received log leader epoch assignment for an offset < latest offset "+
			"for the most recently stored leader epoch. This implies messages have arrived out of order. %s",
			l.epochChangeMsg(epoch, latestEpoch, offset, latestOffset))
	}
}

func (l *leaderEpochCache) epochChangeMsg(newEpoch, lastEpoch uint64, newOffset, lastOffset int64) string {
	return fmt.Sprintf("New: {epoch:%d, offset:%d}, Previous: {epoch:%d, offset:%d} for log %s",
		newEpoch, newOffset, lastEpoch, lastOffset, l.name)
}

// readLeaderEpochOffsets reads the contents of the leader epoch checkpoint
// file, which is of the following form:
//
// v0:
// version
// num_entries
// leader_epoch start_offset
// leader_epoch start_offset
// ...
func readLeaderEpochOffsets(file io.Reader) ([]*epochOffset, error) {
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)
	if !scanner.Scan() {
		return nil, errors.New("missing version")
	}
	version, err := strconv.Atoi(scanner.Text())
	if err != nil {
		return nil, pkgErrors.Wrap(err, "invalid file version value")
	}
	if version > leaderEpochFileV0 {
		return nil, fmt.Errorf("unknown version: %d", version)
	}
	if !scanner.Scan() {
		return nil, errors.New("missing number of entries")
	}
	numEntries, err := strconv.Atoi(scanner.Text())
	if err != nil {
		return nil, pkgErrors.Wrap(err, "invalid entries count value")
	}

	var (
		epochs       = make(map[uint64]int64)
		epochOffsets = []*epochOffset{}
	)

	for scanner.Scan() {
		leaderEpoch, err := strconv.ParseInt(scanner.Text(), 10, 64)
		if err != nil {
			return nil, pkgErrors.Wrap(err, "invalid leader epoch value")
		}
		if !scanner.Scan() {
			return nil, errors.New("missing start offset for epoch")
		}
		startOffset, err := strconv.ParseInt(scanner.Text(), 10, 64)
		if err != nil {
			return nil, pkgErrors.Wrap(err, "invalid epoch start offset value")
		}

		if _, ok := epochs[uint64(leaderEpoch)]; ok {
			// Duplicate entry.
			return nil, fmt.Errorf("duplicate leader epoch %d", leaderEpoch)
		}
		epochs[uint64(leaderEpoch)] = startOffset
		epochOffsets = append(epochOffsets, &epochOffset{
			leaderEpoch: uint64(leaderEpoch),
			startOffset: startOffset,
		})
	}

	if numEntries != len(epochOffsets) {
		return nil, fmt.Errorf("expected %s, got %d",
			english.Plural(numEntries, "entry", ""), len(epochOffsets))
	}

	return epochOffsets, nil
}
