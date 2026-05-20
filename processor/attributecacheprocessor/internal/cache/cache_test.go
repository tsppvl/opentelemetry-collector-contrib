// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/attributecacheprocessor/internal/datasource"
)

// stubSource is a DataSource whose Load behavior is driven by a function so
// individual tests can vary the response per call.
type stubSource struct {
	mu        sync.Mutex
	calls     int
	loadFn    func(call int) ([]string, []map[string]string, error)
	closed    atomic.Bool
	closeErr  error
}

func (s *stubSource) Load(_ context.Context) ([]string, []map[string]string, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.mu.Unlock()
	return s.loadFn(n)
}

func (s *stubSource) Close(_ context.Context) error {
	s.closed.Store(true)
	return s.closeErr
}

func (s *stubSource) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestCacheStartShutdown(t *testing.T) {
	ds := datasource.NewInline(&datasource.InlineConfig{
		Rows: []map[string]string{
			{"name": "alice", "city": "berlin"},
			{"name": "bob", "city": "prague"},
		},
	})
	c := New(ds, 0, zaptest.NewLogger(t), RefreshHooks{})

	// Before Start: no table.
	assert.Nil(t, c.Current())

	require.NoError(t, c.Start(context.Background()))

	tbl := c.Current()
	require.NotNil(t, tbl)
	assert.Equal(t, []string{"city", "name"}, tbl.Columns)
	require.Len(t, tbl.Rows, 2)
	assert.Equal(t, "alice", tbl.Rows[0]["name"])

	require.NoError(t, c.Shutdown(context.Background()))

	// Shutdown must be idempotent (close-of-closed-channel guard).
	require.NoError(t, c.Shutdown(context.Background()))
}

func TestCacheRefresh(t *testing.T) {
	src := &stubSource{
		loadFn: func(call int) ([]string, []map[string]string, error) {
			if call == 1 {
				return []string{"k"}, []map[string]string{{"k": "v1"}}, nil
			}
			return []string{"k"}, []map[string]string{{"k": "v2"}}, nil
		},
	}
	c := New(src, 10*time.Millisecond, zaptest.NewLogger(t), RefreshHooks{})
	require.NoError(t, c.Start(context.Background()))
	t.Cleanup(func() { _ = c.Shutdown(context.Background()) })

	// Initial snapshot from synchronous load.
	require.Equal(t, "v1", c.Current().Rows[0]["k"])

	// Wait for refresh to swap in the second snapshot.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.Current().Rows[0]["k"] == "v2" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	assert.Equal(t, "v2", c.Current().Rows[0]["k"], "background refresh must atomically swap the snapshot")
	assert.GreaterOrEqual(t, src.callCount(), 2)
}

func TestCacheRefreshError(t *testing.T) {
	src := &stubSource{
		loadFn: func(call int) ([]string, []map[string]string, error) {
			if call == 1 {
				return []string{"k"}, []map[string]string{{"k": "good"}}, nil
			}
			return nil, nil, errors.New("boom")
		},
	}
	logger := zaptest.NewLogger(t)
	c := New(src, 5*time.Millisecond, logger, RefreshHooks{})
	require.NoError(t, c.Start(context.Background()))
	t.Cleanup(func() { _ = c.Shutdown(context.Background()) })

	// Wait for at least one refresh attempt to occur.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if src.callCount() >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	require.GreaterOrEqual(t, src.callCount(), 2, "expected at least one refresh after the initial load")

	// Snapshot must remain the previous good one.
	tbl := c.Current()
	require.NotNil(t, tbl)
	require.Len(t, tbl.Rows, 1)
	assert.Equal(t, "good", tbl.Rows[0]["k"])
}

func TestCacheIntervalZero(t *testing.T) {
	src := &stubSource{
		loadFn: func(int) ([]string, []map[string]string, error) {
			return []string{"k"}, []map[string]string{{"k": "v"}}, nil
		},
	}
	c := New(src, 0, zaptest.NewLogger(t), RefreshHooks{})
	require.NoError(t, c.Start(context.Background()))

	// No background goroutine means the call count never grows beyond 1.
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 1, src.callCount(), "interval=0 must not start a refresh goroutine")

	require.NoError(t, c.Shutdown(context.Background()))
	assert.True(t, src.closed.Load(), "Shutdown must close the data source")
}

func TestCacheStartLoadError(t *testing.T) {
	src := &stubSource{
		loadFn: func(int) ([]string, []map[string]string, error) {
			return nil, nil, errors.New("init failure")
		},
	}
	c := New(src, 10*time.Millisecond, zap.NewNop(), RefreshHooks{})

	err := c.Start(context.Background())
	require.Error(t, err)
	assert.Nil(t, c.Current(), "failed initial load must leave Current() nil")

	// Even after a failed start, Shutdown must not panic.
	require.NoError(t, c.Shutdown(context.Background()))
}

// ---------------------------------------------------------------------------
// Additional cache tests
// ---------------------------------------------------------------------------

func TestCacheRefreshHooks_onSuccess(t *testing.T) {
	// OnSuccess must be called after the initial Start() load with the correct
	// row count, and again after each successful background refresh.
	var (
		mu         sync.Mutex
		successCts []int64
	)
	hooks := RefreshHooks{
		OnSuccess: func(_ context.Context, rowCount int64) {
			mu.Lock()
			defer mu.Unlock()
			successCts = append(successCts, rowCount)
		},
	}

	src := &stubSource{
		loadFn: func(call int) ([]string, []map[string]string, error) {
			rows := []map[string]string{{"k": "v"}}
			if call > 1 {
				rows = append(rows, map[string]string{"k": "v2"})
			}
			return []string{"k"}, rows, nil
		},
	}

	c := New(src, 10*time.Millisecond, zaptest.NewLogger(t), hooks)
	require.NoError(t, c.Start(context.Background()))
	t.Cleanup(func() { _ = c.Shutdown(context.Background()) })

	// Initial load must have fired OnSuccess with count=1.
	mu.Lock()
	firstCount := successCts[0]
	mu.Unlock()
	assert.Equal(t, int64(1), firstCount, "OnSuccess must be called with initial row count")

	// Wait for at least one background refresh (call 2 → 2 rows).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(successCts)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	total := len(successCts)
	secondCount := successCts[1]
	mu.Unlock()
	assert.GreaterOrEqual(t, total, 2, "OnSuccess must be called again after refresh")
	assert.Equal(t, int64(2), secondCount, "OnSuccess must report updated row count after refresh")
}

func TestCacheRefreshHooks_onError(t *testing.T) {
	// OnError must be called when a background refresh fails.
	var (
		mu       sync.Mutex
		errCalls int
	)
	hooks := RefreshHooks{
		OnError: func(_ context.Context) {
			mu.Lock()
			defer mu.Unlock()
			errCalls++
		},
	}

	src := &stubSource{
		loadFn: func(call int) ([]string, []map[string]string, error) {
			if call == 1 {
				return []string{"k"}, []map[string]string{{"k": "good"}}, nil
			}
			return nil, nil, errors.New("refresh boom")
		},
	}

	c := New(src, 5*time.Millisecond, zaptest.NewLogger(t), hooks)
	require.NoError(t, c.Start(context.Background()))
	t.Cleanup(func() { _ = c.Shutdown(context.Background()) })

	// Wait for at least one error hook invocation.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := errCalls
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	total := errCalls
	mu.Unlock()
	assert.GreaterOrEqual(t, total, 1, "OnError must be invoked on refresh failure")
}

func TestCacheConcurrentGet(t *testing.T) {
	// Concurrent calls to Current() must be data-race free.
	// Run with: go test -race ./internal/cache/...
	src := &stubSource{
		loadFn: func(int) ([]string, []map[string]string, error) {
			return []string{"k"}, []map[string]string{{"k": "v"}}, nil
		},
	}
	c := New(src, 0, zaptest.NewLogger(t), RefreshHooks{})
	require.NoError(t, c.Start(context.Background()))
	t.Cleanup(func() { _ = c.Shutdown(context.Background()) })

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			tbl := c.Current()
			if tbl == nil {
				return
			}
			// Just read from the snapshot — validates no data race.
			_ = tbl.Rows[0]["k"]
		}()
	}
	wg.Wait()
}

func TestCacheConcurrentGetDuringRefresh(t *testing.T) {
	// Simultaneous reads and background refreshes must not race.
	// Run with -race to catch any data races.
	var call atomic.Int32
	src := &stubSource{
		loadFn: func(int) ([]string, []map[string]string, error) {
			n := call.Add(1)
			return []string{"k"}, []map[string]string{{"k": "v" + string(rune('0'+n%10))}}, nil
		},
	}
	c := New(src, 5*time.Millisecond, zaptest.NewLogger(t), RefreshHooks{})
	require.NoError(t, c.Start(context.Background()))
	t.Cleanup(func() { _ = c.Shutdown(context.Background()) })

	const goroutines = 20
	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					_ = c.Current()
				}
			}
		}()
	}
	time.Sleep(100 * time.Millisecond)
	close(done)
	wg.Wait()
}

func TestCacheNilSource(t *testing.T) {
	// New with nil source must not panic; Start must return an error.
	c := New(nil, 0, zap.NewNop(), RefreshHooks{})
	err := c.Start(context.Background())
	require.Error(t, err, "nil data source must cause Start to return an error")
	require.NoError(t, c.Shutdown(context.Background()))
}

func TestCacheShutdownIdempotent(t *testing.T) {
	src := &stubSource{
		loadFn: func(int) ([]string, []map[string]string, error) {
			return []string{"k"}, []map[string]string{{"k": "v"}}, nil
		},
	}
	c := New(src, 0, zap.NewNop(), RefreshHooks{})
	require.NoError(t, c.Start(context.Background()))
	// Multiple shutdowns must not panic.
	require.NoError(t, c.Shutdown(context.Background()))
	require.NoError(t, c.Shutdown(context.Background()))
	require.NoError(t, c.Shutdown(context.Background()))
}
