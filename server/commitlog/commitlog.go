// Package commitlog provides an implementation for a file-backed write-ahead
// log.
package commitlog

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	atomic_file "github.com/natefinch/atomic"
	"github.com/pkg/errors"

	"github.com/liftbridge-io/liftbridge/server/logger"
	"github.com/liftbridge-io/liftbridge/server/proto"
)

var ErrSegmentNotFound = errors.New("segment not found")

const (
	logFileSuffix               = ".log"
	indexFileSuffix             = ".index"
	hwFileName                  = "replication-offset-checkpoint"
	defaultMaxSegmentBytes      = 1073741824
	defaultHWCheckpointInterval = 5 * time.Second
	defaultCleanerInterval      = 5 * time.Minute
)

// CommitLog implements the server.CommitLog interface, which is a durable
// write-ahead log.
type CommitLog struct {
	Options
	deleteCleaner    *DeleteCleaner
	compactCleaner   *CompactCleaner
	name             string
	mu               sync.RWMutex
	hw               int64
	closed           chan struct{}
	segments         []*Segment
	vActiveSegment   *Segment
	hwWaiters        map[contextReader]chan struct{}
	leaderEpochCache *leaderEpochCache
}

// Options contains settings for configuring a CommitLog.
type Options struct {
	Name                 string        // CommitLog name
	Path                 string        // Path to log directory
	MaxSegmentBytes      int64         // Max bytes a Segment can contain before creating a new one
	MaxLogBytes          int64         // Retention by bytes
	MaxLogMessages       int64         // Retention by messages
	MaxLogAge            time.Duration // Retention by age
	Compact              bool          // Run compaction on log clean
	CompactMaxGoroutines int           // Max number of goroutines to use in a log compaction
	CleanerInterval      time.Duration // Frequency to enforce retention policy
	HWCheckpointInterval time.Duration // Frequency to checkpoint HW to disk
	LogRollTime          time.Duration // Max time before a new log segment is rolled out.
	Logger               logger.Logger
}

// New creates a new CommitLog and starts a background goroutine which
// periodically checkpoints the high watermark to disk.
func New(opts Options) (*CommitLog, error) {
	if opts.Path == "" {
		return nil, errors.New("path is empty")
	}

	if opts.Logger == nil {
		opts.Logger = logger.NewLogger(0)
		opts.Logger.SetWriter(ioutil.Discard)
	}

	if opts.MaxSegmentBytes == 0 {
		opts.MaxSegmentBytes = defaultMaxSegmentBytes
	}
	if opts.HWCheckpointInterval == 0 {
		opts.HWCheckpointInterval = defaultHWCheckpointInterval
	}
	if opts.CleanerInterval == 0 {
		opts.CleanerInterval = defaultCleanerInterval
	}

	cleanerOpts := DeleteCleanerOptions{
		Name:   opts.Path,
		Logger: opts.Logger,
	}
	cleanerOpts.Retention.Bytes = opts.MaxLogBytes
	cleanerOpts.Retention.Messages = opts.MaxLogMessages
	cleanerOpts.Retention.Age = opts.MaxLogAge
	cleaner := NewDeleteCleaner(cleanerOpts)

	compactCleanerOpts := CompactCleanerOptions{
		Name:          opts.Name,
		Logger:        opts.Logger,
		MaxGoroutines: opts.CompactMaxGoroutines,
	}
	compactCleaner := NewCompactCleaner(compactCleanerOpts)

	path, _ := filepath.Abs(opts.Path)
	epochCache, err := newLeaderEpochCache(opts.Name, path, opts.Logger)
	if err != nil {
		return nil, err
	}

	l := &CommitLog{
		Options:          opts,
		name:             filepath.Base(path),
		deleteCleaner:    cleaner,
		compactCleaner:   compactCleaner,
		hw:               -1,
		closed:           make(chan struct{}),
		hwWaiters:        make(map[contextReader]chan struct{}),
		leaderEpochCache: epochCache,
	}

	if err := l.init(); err != nil {
		return nil, err
	}

	if err := l.open(); err != nil {
		return nil, err
	}

	// After an unclean shutdown, the leader epoch checkpoint file could be
	// ahead of the log (as the log is flushed asynchronously by default). To
	// account for this, remove all entries from the leader epoch checkpoint
	// file where the offset is greater than the log end offset.
	if err := l.leaderEpochCache.ClearLatest(l.activeSegment().NextOffset()); err != nil {
		return nil, err
	}

	// The earliest leader epoch may not be flushed during a hard failure.
	// Recover it here.
	if err := l.leaderEpochCache.ClearEarliest(l.OldestOffset()); err != nil {
		return nil, err
	}

	go l.checkpointHWLoop()
	go l.cleanerLoop()

	return l, nil
}

func (l *CommitLog) init() error {
	err := os.MkdirAll(l.Path, 0755)
	if err != nil {
		return errors.Wrap(err, "mkdir failed")
	}
	return nil
}

func (l *CommitLog) open() error {
	files, err := ioutil.ReadDir(l.Path)
	if err != nil {
		return errors.Wrap(err, "read dir failed")
	}
	for _, file := range files {
		// If this file is an index file, make sure it has a corresponding .log
		// file.
		if strings.HasSuffix(file.Name(), indexFileSuffix) {
			_, err := os.Stat(filepath.Join(
				l.Path, strings.Replace(file.Name(), indexFileSuffix, logFileSuffix, 1)))
			if os.IsNotExist(err) {
				if err := os.Remove(filepath.Join(l.Path, file.Name())); err != nil {
					return err
				}
			} else if err != nil {
				return errors.Wrap(err, "stat file failed")
			}
		} else if strings.HasSuffix(file.Name(), logFileSuffix) {
			offsetStr := strings.TrimSuffix(file.Name(), logFileSuffix)
			baseOffset, err := strconv.Atoi(offsetStr)
			if err != nil {
				return err
			}
			segment, err := NewSegment(l.Path, int64(baseOffset), l.MaxSegmentBytes, false, "")
			if err != nil {
				return err
			}
			l.segments = append(l.segments, segment)
		} else if file.Name() == hwFileName {
			// Recover high watermark.
			b, err := ioutil.ReadFile(filepath.Join(l.Path, file.Name()))
			if err != nil {
				return errors.Wrap(err, "read high watermark file failed")
			}
			hw, err := strconv.ParseInt(string(b), 10, 64)
			if err != nil {
				return errors.Wrap(err, "parse high watermark file failed")
			}
			l.hw = hw
		}
	}
	if len(l.segments) == 0 {
		segment, err := NewSegment(l.Path, 0, l.MaxSegmentBytes, true, "")
		if err != nil {
			return err
		}
		l.segments = append(l.segments, segment)
	}
	activeSegment := l.segments[len(l.segments)-1]
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&l.vActiveSegment)),
		unsafe.Pointer(activeSegment))
	return nil
}

// Append writes the given batch of messages to the log and returns their
// corresponding offsets in the log.
func (l *CommitLog) Append(msgs []*proto.Message) ([]int64, error) {
	if _, err := l.checkAndPerformSplit(); err != nil {
		return nil, err
	}
	var (
		segment          = l.activeSegment()
		basePosition     = segment.Position()
		baseOffset       = segment.NextOffset()
		ms, entries, err = NewMessageSetFromProto(baseOffset, basePosition, msgs)
	)
	if err != nil {
		return nil, err
	}
	return l.append(segment, ms, entries)
}

// AppendMessageSet writes the given message set data to the log and returns
// the corresponding offsets in the log.
func (l *CommitLog) AppendMessageSet(ms []byte) ([]int64, error) {
	if _, err := l.checkAndPerformSplit(); err != nil {
		return nil, err
	}
	var (
		segment      = l.activeSegment()
		basePosition = segment.Position()
		entries      = EntriesForMessageSet(basePosition, ms)
	)
	return l.append(segment, ms, entries)
}

func (l *CommitLog) append(segment *Segment, ms []byte, entries []*Entry) ([]int64, error) {
	if err := segment.WriteMessageSet(ms, entries); err != nil {
		return nil, err
	}
	var (
		lastLeaderEpoch = l.leaderEpochCache.LastLeaderEpoch()
		offsets         = make([]int64, len(entries))
	)
	for i, entry := range entries {
		// Check if message is in a new leader epoch.
		if entry.LeaderEpoch > lastLeaderEpoch {
			// If it is, we need to assign the epoch offset.
			if err := l.leaderEpochCache.Assign(entry.LeaderEpoch, entry.Offset); err != nil {
				return nil, err
			}
			lastLeaderEpoch = entry.LeaderEpoch
		}
		offsets[i] = entry.Offset
	}
	return offsets, nil
}

// NewestOffset returns the offset of the last message in the log or -1 if
// empty.
func (l *CommitLog) NewestOffset() int64 {
	return l.activeSegment().NextOffset() - 1
}

// OldestOffset returns the offset of the first message in the log or -1 if
// empty.
func (l *CommitLog) OldestOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].FirstOffset()
}

// OffsetForTimestamp returns the earliest offset whose timestamp is greater
// than or equal to the given timestamp.
func (l *CommitLog) OffsetForTimestamp(timestamp int64) (int64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Find the first segment whose base timestamp is greater than the given
	// timestamp.
	idx, err := findSegmentIndexByTimestamp(l.segments, timestamp)
	if err != nil {
		return 0, errors.Wrap(err, "failed to find log segment for timestamp")
	}
	// Search the previous segment for the first entry whose timestamp is
	// greater than or equal to the given timestamp. If this is the first
	// segment, just search it.
	var seg *Segment
	if idx == 0 {
		seg = l.segments[0]
	} else {
		seg = l.segments[idx-1]
	}
	entry, err := seg.findEntryByTimestamp(timestamp)
	if err == nil {
		return entry.Offset, nil
	}
	if err != ErrEntryNotFound && err != io.EOF {
		return 0, errors.Wrap(err, "failed to find log entry for timestamp")
	}
	// This indicates there are no entries in the segment whose timestamp
	// is greater than or equal to the target timestamp. In this case, search
	// the next segment if there is one. If there isn't, the timestamp is
	// beyond the end of the log so return the next offset.
	if idx < len(l.segments) {
		seg = l.segments[idx]
		entry, err := seg.findEntryByTimestamp(timestamp)
		if err != nil {
			return 0, errors.Wrap(err, "failed to find log entry for timestamp")
		}
		return entry.Offset, nil
	}
	return l.segments[len(l.segments)-1].NextOffset(), nil
}

// SetHighWatermark sets the high watermark on the log. All messages up to and
// including the high watermark are considered committed.
func (l *CommitLog) SetHighWatermark(hw int64) {
	l.mu.Lock()
	if hw > l.hw {
		l.hw = hw
		l.notifyHWWaiters()
	}
	l.mu.Unlock()
	// TODO: should we flush the HW to disk here?
}

// OverrideHighWatermark sets the high watermark on the log using the given
// value, even if the value is less than the current HW. This is used for unit
// testing purposes.
func (l *CommitLog) OverrideHighWatermark(hw int64) {
	l.mu.Lock()
	l.hw = hw
	l.notifyHWWaiters()
	l.mu.Unlock()
}

func (l *CommitLog) notifyHWWaiters() {
	for r, ch := range l.hwWaiters {
		close(ch)
		delete(l.hwWaiters, r)
	}
}

func (l *CommitLog) waitForHW(r contextReader, hw int64) <-chan struct{} {
	wait := make(chan struct{})
	l.mu.Lock()
	// Check if HW has changed.
	if l.hw != hw {
		close(wait)
	} else {
		l.hwWaiters[r] = wait
	}
	l.mu.Unlock()
	return wait
}

func (l *CommitLog) removeHWWaiter(r contextReader) {
	l.mu.Lock()
	delete(l.hwWaiters, r)
	l.mu.Unlock()
}

// HighWatermark returns the high watermark for the log.
func (l *CommitLog) HighWatermark() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.hw
}

// NewLeaderEpoch indicates the log is entering a new leader epoch.
func (l *CommitLog) NewLeaderEpoch(epoch uint64) error {
	return l.leaderEpochCache.Assign(epoch, l.NewestOffset())
}

// LastOffsetForLeaderEpoch returns the start offset of the first leader epoch
// larger than the provided one or the log end offset if the current epoch
// equals the provided one.
func (l *CommitLog) LastOffsetForLeaderEpoch(epoch uint64) int64 {
	offset := l.leaderEpochCache.LastOffsetForLeaderEpoch(epoch)
	if offset == -1 {
		offset = l.activeSegment().NextOffset() - 1
	}
	return offset
}

// LastLeaderEpoch returns the latest leader epoch for the log.
func (l *CommitLog) LastLeaderEpoch() uint64 {
	return l.leaderEpochCache.LastLeaderEpoch()
}

func (l *CommitLog) activeSegment() *Segment {
	return (*Segment)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&l.vActiveSegment))))
}

// Close closes each log segment file and stops the background goroutine
// checkpointing the high watermark to disk.
func (l *CommitLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.checkpointHW(); err != nil {
		return err
	}
	close(l.closed)
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Delete closes the log and removes all data associated with it from the
// filesystem.
func (l *CommitLog) Delete() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Path)
}

// Truncate removes all messages from the log starting at the given offset.
func (l *CommitLog) Truncate(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	segment, idx := findSegment(l.segments, offset)
	if segment == nil {
		// Nothing to truncate.
		return nil
	}

	// Delete all following segments.
	deleted := 0
	for i := idx + 1; i < len(l.segments); i++ {
		if err := l.segments[i].Delete(); err != nil {
			return err
		}
		deleted++
	}

	var replace bool

	// Delete the segment if its base offset is the target offset, provided
	// it's not the first segment.
	if segment.BaseOffset == offset {
		if idx == 0 {
			replace = true
		} else {
			if err := segment.Delete(); err != nil {
				return err
			}
			deleted++
		}
	} else {
		replace = true
	}

	// Retain all preceding segments.
	segments := make([]*Segment, len(l.segments)-deleted)
	for i := 0; i < idx; i++ {
		segments[i] = l.segments[i]
	}

	// Replace segment containing offset with truncated segment.
	if replace {
		var (
			ss              = NewSegmentScanner(segment)
			newSegment, err = segment.Truncated()
		)
		if err != nil {
			return err
		}
		for ms, entry, err := ss.Scan(); err == nil; ms, entry, err = ss.Scan() {
			if ms.Offset() < offset {
				if err := newSegment.WriteMessageSet(ms, []*Entry{entry}); err != nil {
					return err
				}
			} else {
				break
			}
		}
		if err = newSegment.Replace(segment); err != nil {
			return err
		}
		segments[idx] = newSegment
	}
	activeSegment := segments[len(segments)-1]
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&l.vActiveSegment)),
		unsafe.Pointer(activeSegment))
	l.segments = segments
	return l.leaderEpochCache.ClearLatest(offset)
}

func (l *CommitLog) Segments() []*Segment {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments
}

// NotifyLEO registers and returns a channel which is closed when messages past
// the given log end offset are added to the log. If the given offset is no
// longer the log end offset, the channel is closed immediately. Waiter is an
// opaque value that uniquely identifies the entity waiting for data.
func (l *CommitLog) NotifyLEO(waiter interface{}, leo int64) <-chan struct{} {
	return l.activeSegment().WaitForLEO(waiter, leo)
}

// checkAndPerformSplit determines if a new log segment should be rolled out
// either because the active segment is full or LogRollTime has passed since
// the first message was written to it. It then performs the split if eligible,
// returning any error resulting from the split. The returned bool indicates if
// a split was performed.
func (l *CommitLog) checkAndPerformSplit() (bool, error) {
	// Do this in a loop because segment splitting may fail due to a competing
	// thread performing the split at the same time. If this happens, we just
	// retry the check on the new active segment.
	for {
		activeSegment := l.activeSegment()
		if !activeSegment.CheckSplit(l.LogRollTime) {
			return false, nil
		}
		if err := l.split(activeSegment); err != nil {
			// ErrSegmentExists indicates another thread has already performed
			// the segment split, so reload the new active segment and check
			// again.
			if err == ErrSegmentExists {
				continue
			}
			return false, err
		}
		activeSegment.Seal()
		return true, nil
	}
}

func (l *CommitLog) split(oldActiveSegment *Segment) error {
	offset := l.NewestOffset() + 1
	l.Logger.Debugf("Appending new log segment for %s with base offset %d", l.Path, offset)
	segment, err := NewSegment(l.Path, offset, l.MaxSegmentBytes, true, "")
	if err != nil {
		return err
	}
	// Do a CAS on the active segment to ensure no other threads have replaced
	// it already. If this fails, it means another thread has already replaced
	// it, so delete the new segment and return ErrSegmentExists.
	if !atomic.CompareAndSwapPointer(
		(*unsafe.Pointer)(unsafe.Pointer(&l.vActiveSegment)),
		unsafe.Pointer(oldActiveSegment), unsafe.Pointer(segment)) {
		segment.Delete()
		return ErrSegmentExists
	}
	l.mu.Lock()
	segments := append(l.segments, segment)
	l.segments = segments
	l.mu.Unlock()
	return nil
}

func (l *CommitLog) cleanerLoop() {
	ticker := time.NewTicker(l.CleanerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-l.closed:
			return
		}

		// Check to see if the active segment should be split.
		split, err := l.checkAndPerformSplit()
		if err != nil {
			l.Logger.Errorf("Failed to split log %s: %v", l.Path, err)
			continue
		}

		// If we rolled a new segment, we don't need to run the cleaner since
		// it already ran.
		if split {
			continue
		}

		if err := l.Clean(); err != nil {
			l.Logger.Errorf("Failed to clean log %s: %v", l.Path, err)
		}
	}
}

// Clean applies retention and compaction rules against the log, if applicable.
func (l *CommitLog) Clean() error {
	l.mu.RLock()
	oldSegments := l.segments
	l.mu.RUnlock()
	cleaned, epochCache, err := l.clean(oldSegments)
	if err != nil {
		return err
	}
	l.mu.Lock()
	newSegments := l.segments
	if len(newSegments) > len(oldSegments) {
		// New segments were added while cleaning. Rebase the new segments onto
		// the cleaned ones.
		rebase := newSegments[len(oldSegments):]
		cleaned = l.rebaseSegments(rebase, cleaned, epochCache)
	}
	l.segments = cleaned
	// Update the leader epoch offset cache to account for deleted segments. If
	// compaction ran, we need to regenerate the cache using the one returned
	// from compaction.
	if epochCache != nil {
		err = l.leaderEpochCache.Replace(epochCache)
	} else {
		err = l.leaderEpochCache.ClearEarliest(l.segments[0].BaseOffset)
	}
	l.mu.Unlock()
	return err
}

// rebaseSegments adds the segments in from to the end of the slice of segments
// in to and adds any leader epoch offsets to the given leaderEpochCache.
func (l *CommitLog) rebaseSegments(from, to []*Segment, epochCache *leaderEpochCache) []*Segment {
	for _, seg := range from {
		to = append(to, seg)
	}
	// Rebase any leader epoch offsets also. We don't check the error returned
	// here because Rebase can't return an error since epochCache is not
	// file-backed.
	epochCache.Rebase(l.leaderEpochCache, from[0].BaseOffset)
	return to
}

// clean returns the cleaned segments and, if compaction ran, a
// *leaderEpochCache maintaining the start offset for each new leader epoch. If
// compaction did not run, the leaderEpochCache will be nil.
func (l *CommitLog) clean(segments []*Segment) ([]*Segment, *leaderEpochCache, error) {
	cleaned, err := l.deleteCleaner.Clean(segments)
	if err != nil {
		return nil, nil, err
	}
	var epochCache *leaderEpochCache
	if l.Compact {
		cleaned, epochCache, err = l.compactCleaner.Compact(l.HighWatermark(), cleaned)
		if err != nil {
			return nil, nil, err
		}
	}
	return cleaned, epochCache, nil
}

func (l *CommitLog) checkpointHWLoop() {
	ticker := time.NewTicker(l.HWCheckpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-l.closed:
			return
		}
		l.mu.RLock()
		if err := l.checkpointHW(); err != nil {
			panic(errors.Wrap(err, "failed to checkpoint high watermark"))
		}
		l.mu.RUnlock()
	}
}

func (l *CommitLog) checkpointHW() error {
	var (
		hw   = l.hw
		r    = strings.NewReader(strconv.FormatInt(hw, 10))
		file = filepath.Join(l.Path, hwFileName)
	)
	return atomic_file.WriteFile(file, r)
}
