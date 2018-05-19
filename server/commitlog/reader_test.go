package commitlog_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/tylertreat/liftbridge/server/commitlog"
)

var segmentSizeTests = []struct {
	name        string
	segmentSize int64
}{
	{"6", 6},
	{"60", 60},
	{"600", 600},
	{"6000", 6000},
}

func TestReaderStartOffset(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			l := setupWithOptions(t, commitlog.Options{
				Path:            path,
				MaxSegmentBytes: test.segmentSize,
				MaxLogBytes:     -1,
			})
			defer cleanup(t)

			numMsgs := 10
			msgs := make([]commitlog.MessageSet, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = commitlog.NewMessageSet(
					uint64(i), commitlog.NewMessage([]byte(strconv.Itoa(i))),
				)
			}
			for _, ms := range msgs {
				_, err := l.Append(ms)
				require.NoError(t, err)
			}
			idx := 4
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := l.NewReaderUncommitted(ctx, int64(idx))
			require.NoError(t, err)

			p := make([]byte, msgs[idx].Size())
			_, err = r.Read(p)
			require.NoError(t, err)
			act := commitlog.MessageSet(p)
			require.Equal(t, int64(idx), act.Offset())
		})
	}
}
