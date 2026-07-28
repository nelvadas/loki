package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/iter"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/storage/config"
)

func TestNewStreamFirstSampleBatchIterator(t *testing.T) {
	periodConfig := config.PeriodConfig{From: config.DayTime{Time: 0}, Schema: "v11", RowShards: 16}
	schemaConfig := config.SchemaConfig{Configs: []config.PeriodConfig{periodConfig}}
	chunkfmt, headfmt, err := periodConfig.ChunkFormat()
	require.NoError(t, err)

	newEx := func() log.SampleExtractor {
		ex, err := log.NewLineSampleExtractor(log.CountExtractor, nil, nil, false, false)
		require.NoError(t, err)
		return ex
	}
	matchers := newMatchers(`{foo=~".+"}`)
	start, end := time.Unix(0, 0), time.Unix(0, 100*int64(time.Millisecond))

	// streamFirst builds the stream-first iterator over chunks with the given batching and fetch.
	streamFirst := func(ctx context.Context, chunks []*LazyChunk, batchSize, maxConcurrent int, fetch chunkFetchFunc) (iter.SampleIterator, error) {
		return newStreamFirstSampleBatchIterator(ctx, schemaConfig, NilMetrics, chunks, batchSize, matchers, start, end, nil, maxConcurrent, fetch, newEx())
	}

	// drainTimestamps drains it and returns each sample's timestamp, asserting a clean close.
	drainTimestamps := func(t *testing.T, it iter.SampleIterator) []int64 {
		var got []int64
		for it.Next() {
			got = append(got, it.At().Timestamp)
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		return got
	}

	// millis turns millisecond values into the nanosecond timestamps the iterator returns.
	millisToNanos := func(vals ...int64) []int64 {
		out := make([]int64, len(vals))
		for i, v := range vals {
			out[i] = v * time.Millisecond.Nanoseconds()
		}
		return out
	}

	t.Run("matches the timestamp-first iterator's deduplicated result, in stream-first order", func(t *testing.T) {
		// Three streams interleaved, plus a duplicate chunk for one stream to exercise dedup.
		buildChunks := func() []*LazyChunk {
			return []*LazyChunk{
				newLazyChunk(chunkfmt, headfmt, mkStream("b", 1, 2, 3)),
				newLazyChunk(chunkfmt, headfmt, mkStream("a", 1, 2, 3)),
				newLazyChunk(chunkfmt, headfmt, mkStream("c", 1, 2, 3)),
				newLazyChunk(chunkfmt, headfmt, mkStream("a", 1, 2, 3)), // duplicate of stream "a"
			}
		}
		type entry struct {
			hash   uint64
			labels string
			ts     int64
			value  float64
		}
		drain := func(it iter.SampleIterator) []entry {
			var out []entry
			for it.Next() {
				sm := it.At()
				out = append(out, entry{it.StreamHash(), it.Labels(), sm.Timestamp, sm.Value})
			}
			require.NoError(t, it.Err())
			require.NoError(t, it.Close())
			return out
		}

		timestampFirstIterator, err := newTimestampFirstSampleBatchIterator(context.Background(), schemaConfig, NilMetrics, buildChunks(), 10, matchers, start, end, nil, newEx())
		require.NoError(t, err)
		streamFirstIterator, err := streamFirst(context.Background(), buildChunks(), 10, 0, fetchLazyChunks)
		require.NoError(t, err)

		timestampFirstEntries := drain(timestampFirstIterator)
		streamFirstEntries := drain(streamFirstIterator)

		// Same deduplicated data, regardless of order. The streamHash intentionally differs between
		// the two paths — the timestamp path exposes the extractor's reduced hash, the stream path
		// exposes the raw fingerprint the cross-source merge aligns on — so compare only
		// (labels, ts, value).
		type point struct {
			labels string
			ts     int64
			value  float64
		}
		points := func(es []entry) []point {
			out := make([]point, len(es))
			for i, e := range es {
				out[i] = point{e.labels, e.ts, e.value}
			}
			return out
		}
		require.ElementsMatch(t, points(timestampFirstEntries), points(streamFirstEntries))
		require.NotEmpty(t, streamFirstEntries)

		// Stream-first ordering: streamHash is non-decreasing; within one streamHash, ts ascending.
		for i := 1; i < len(streamFirstEntries); i++ {
			if streamFirstEntries[i].hash == streamFirstEntries[i-1].hash {
				require.LessOrEqualf(t, streamFirstEntries[i-1].ts, streamFirstEntries[i].ts, "ts not ascending within stream at %d", i)
			} else {
				require.Lessf(t, streamFirstEntries[i-1].hash, streamFirstEntries[i].hash, "streamHash not ascending at %d", i)
			}
		}
	})

	t.Run("reads non-overlapping chunks across multiple batches in order", func(t *testing.T) {
		chunks := []*LazyChunk{
			newLazyChunk(chunkfmt, headfmt, mkStream("a", 1, 2, 3)),
			newLazyChunk(chunkfmt, headfmt, mkStream("a", 4, 5, 6)),
			newLazyChunk(chunkfmt, headfmt, mkStream("a", 7, 8, 9)),
		}
		// batchSize 2 splits the 3 chunks across multiple prefetch batches.
		it, err := streamFirst(context.Background(), chunks, 2, 2, fetchLazyChunks)
		require.NoError(t, err)
		require.Equal(t, millisToNanos(1, 2, 3, 4, 5, 6, 7, 8, 9), drainTimestamps(t, it))
	})

	t.Run("merges and deduplicates time-overlapping chunks across multiple batches", func(t *testing.T) {
		// [1,2,3], [3,4,5] (overlaps at ts 3 — a duplicate line) and a full duplicate of [1,2,3]; the
		// deduplicated stream is ts 1..5. batchSize 2 resolves the overlap across a batch boundary.
		chunks := []*LazyChunk{
			newLazyChunk(chunkfmt, headfmt, mkStream("a", 1, 2, 3)),
			newLazyChunk(chunkfmt, headfmt, mkStream("a", 3, 4, 5)),
			newLazyChunk(chunkfmt, headfmt, mkStream("a", 1, 2, 3)),
		}
		it, err := streamFirst(context.Background(), chunks, 2, 2, fetchLazyChunks)
		require.NoError(t, err)
		require.Equal(t, millisToNanos(1, 2, 3, 4, 5), drainTimestamps(t, it))
	})

	t.Run("reads all chunks in a single batch", func(t *testing.T) {
		chunks := []*LazyChunk{
			newLazyChunk(chunkfmt, headfmt, mkStream("a", 1, 2, 3)),
			newLazyChunk(chunkfmt, headfmt, mkStream("a", 4, 5, 6)),
		}
		// batchSize exceeds the chunk count → a single prefetch batch.
		it, err := streamFirst(context.Background(), chunks, 10, 1, fetchLazyChunks)
		require.NoError(t, err)
		require.Equal(t, millisToNanos(1, 2, 3, 4, 5, 6), drainTimestamps(t, it))
	})

	t.Run("returns the context error when canceled while waiting for the preloader", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		fetching := make(chan struct{})
		fetch := func(ctx context.Context, _ config.SchemaConfig, _ []*LazyChunk) error {
			close(fetching) // a single chunk means fetch runs exactly once
			<-ctx.Done()    // block until the query is canceled
			return ctx.Err()
		}

		chunks := []*LazyChunk{newLazyChunk(chunkfmt, headfmt, mkStream("a", 1, 2, 3))}
		it, err := streamFirst(ctx, chunks, 2, 1, fetch)
		require.NoError(t, err)

		go func() {
			<-fetching
			cancel()
		}()

		require.False(t, it.Next())
		require.ErrorIs(t, it.Err(), context.Canceled)
		require.NoError(t, it.Close())
	})

	t.Run("surfaces a preloader fetch error and stops iteration", func(t *testing.T) {
		mockErr := errors.New("fetch failed")
		fetch := func(context.Context, config.SchemaConfig, []*LazyChunk) error { return mockErr }

		chunks := []*LazyChunk{newLazyChunk(chunkfmt, headfmt, mkStream("a", 1, 2, 3))}
		it, err := streamFirst(context.Background(), chunks, 2, 1, fetch)
		require.NoError(t, err)

		require.False(t, it.Next())
		require.ErrorIs(t, it.Err(), mockErr)
		require.NoError(t, it.Close())
	})
}

// streamFirstPrefetchTestFixture builds a small multi-stream chunk set plus the schema/matchers/extractor
// needed to iterate it, for the consumer-level prefetch tests below.
type streamFirstPrefetchTestFixture struct {
	schema     config.SchemaConfig
	chunks     []*LazyChunk
	matchers   []*labels.Matcher
	start, end time.Time
	newEx      func() log.SampleExtractor
}

func newStreamFirstPrefetchTestFixture(t *testing.T) streamFirstPrefetchTestFixture {
	t.Helper()

	periodConfig := config.PeriodConfig{From: config.DayTime{Time: 0}, Schema: "v11", RowShards: 16}
	schemaConfig := config.SchemaConfig{Configs: []config.PeriodConfig{periodConfig}}
	chunkfmt, headfmt, err := periodConfig.ChunkFormat()
	require.NoError(t, err)

	// Multiple streams, each with several chunks, so batches split streams and span boundaries.
	var chunks []*LazyChunk
	for _, foo := range []string{"a", "b", "c"} {
		for c := 0; c < 3; c++ {
			base := int64(c*10 + 1)
			chunks = append(chunks, newLazyChunk(chunkfmt, headfmt, mkStream(foo, base, base+1, base+2)))
		}
	}

	return streamFirstPrefetchTestFixture{
		schema:   schemaConfig,
		chunks:   chunks,
		matchers: newMatchers(`{foo=~".+"}`),
		start:    time.Unix(0, 0),
		end:      time.Unix(0, 100*int64(time.Millisecond)),
		newEx: func() log.SampleExtractor {
			ex, err := log.NewLineSampleExtractor(log.CountExtractor, nil, nil, false, false)
			require.NoError(t, err)
			return ex
		},
	}
}

// TestLazyStreamFirstSampleIterator verifies the consumer only decodes a stream after the preloader
// has fetched its chunks, and frees each stream's compressed Data once the stream is consumed.
func TestLazyStreamFirstSampleIterator(t *testing.T) {
	fx := newStreamFirstPrefetchTestFixture(t)

	// Mark every chunk unfetched; the injected fetch is the only thing that may validate them.
	// If the consumer decoded a stream before it was fetched, buildHeapIterator would skip its
	// (invalid) chunks and the stream would yield nothing.
	for _, c := range fx.chunks {
		c.IsValid = false
	}

	var fetchedChunks int
	fetch := func(_ context.Context, _ config.SchemaConfig, chunks []*LazyChunk) error {
		for _, c := range chunks {
			require.NotNil(t, c.Chunk.Data, "fetch must run before Data is released")
			c.IsValid = true // simulate fetchLazyChunks validating the chunk
			fetchedChunks++
		}
		return nil
	}

	// batchSize 2 forces several batches so streams split across batch boundaries.
	it, err := newStreamFirstSampleBatchIterator(
		context.Background(), fx.schema, NilMetrics, fx.chunks, 2,
		fx.matchers, fx.start, fx.end, nil, 0, fetch, fx.newEx())
	require.NoError(t, err)

	var n int
	for it.Next() {
		_ = it.At()
		n++
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())

	require.Positive(t, n, "expected samples; got none (chunks likely decoded before fetch)")
	require.Equal(t, len(fx.chunks), fetchedChunks, "every chunk should be fetched exactly once")

	// After a full drain, every stream's compressed Data is released.
	for _, c := range fx.chunks {
		require.Nil(t, c.Chunk.Data, "chunk Data should be released after its stream is consumed")
	}
}

// mkStream builds a single-series logproto.Stream labelled {foo="<fooVal>"} with one line per
// timestamp (in milliseconds); each line is distinct per timestamp.
func mkStream(fooVal string, tss ...int64) logproto.Stream {
	st := logproto.Stream{Labels: fmt.Sprintf(`{foo="%s"}`, fooVal)}
	for _, ts := range tss {
		st.Entries = append(st.Entries, logproto.Entry{
			Timestamp: time.Unix(0, ts*int64(time.Millisecond)),
			Line:      fmt.Sprintf("line-%d", ts),
		})
	}
	return st
}
