package fastq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mustFinish runs fn and fails the test (from the test goroutine) if it
// doesn't return within d. Used so a deadlocked Stop() can't hang the suite.
func mustFinish(t *testing.T, d time.Duration, desc string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
		return
	case <-time.After(d):
	}
	t.Fatalf("timeout after %s: %s did not return", d, desc)
}

// ---------------------------------------------------------------------------
// 1. TestPushVariants — basic correctness for each entry point.
// ---------------------------------------------------------------------------

func TestPushVariants(t *testing.T) {
	ctx := context.Background()

	t.Run("PushAndWait/result", func(t *testing.T) {
		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			return x * 2, nil
		}, 2)
		defer q.Stop()

		got, err := q.PushAndWait(ctx, 21)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != 42 {
			t.Fatalf("got %d, want 42", got)
		}
	})

	t.Run("PushAndWait/handler_error", func(t *testing.T) {
		boom := errors.New("boom")
		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			return 0, boom
		}, 2)
		defer q.Stop()

		_, err := q.PushAndWait(ctx, 1)
		if !errors.Is(err, boom) {
			t.Fatalf("got err=%v, want %v", err, boom)
		}
	})

	t.Run("PushWithCallback/result", func(t *testing.T) {
		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			return x + 100, nil
		}, 2)
		defer q.Stop()

		var (
			gotResult int
			gotErr    error
			calls     int32
		)
		done := make(chan struct{})
		err := q.PushWithCallback(ctx, 5, func(_ context.Context, r int, e error) {
			atomic.AddInt32(&calls, 1)
			gotResult = r
			gotErr = e
			close(done)
		})
		if err != nil {
			t.Fatalf("PushWithCallback returned %v", err)
		}
		mustFinish(t, time.Second, "callback fires", func() { <-done })

		if gotResult != 105 || gotErr != nil {
			t.Fatalf("callback got (%d, %v), want (105, nil)", gotResult, gotErr)
		}
		if n := atomic.LoadInt32(&calls); n != 1 {
			t.Fatalf("callback fired %d times, want 1", n)
		}
	})

	t.Run("PushWithCallback/handler_error", func(t *testing.T) {
		boom := errors.New("boom")
		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			return 0, boom
		}, 2)
		defer q.Stop()

		var gotErr error
		done := make(chan struct{})
		_ = q.PushWithCallback(ctx, 1, func(_ context.Context, _ int, e error) {
			gotErr = e
			close(done)
		})
		mustFinish(t, time.Second, "callback fires", func() { <-done })

		if !errors.Is(gotErr, boom) {
			t.Fatalf("callback err=%v, want %v", gotErr, boom)
		}
	})

	t.Run("PushWithCallback/nil_callback", func(t *testing.T) {
		var ran int32
		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			atomic.AddInt32(&ran, 1)
			return x, nil
		}, 2)
		defer q.Stop()

		if err := q.PushWithCallback(ctx, 7, nil); err != nil {
			t.Fatalf("PushWithCallback nil cb returned %v", err)
		}
		mustFinish(t, time.Second, "WaitEmpty", q.WaitEmpty)

		if atomic.LoadInt32(&ran) != 1 {
			t.Fatalf("handler ran %d times, want 1", ran)
		}
	})

	t.Run("PushWithoutResult/runs", func(t *testing.T) {
		var ran int32
		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			atomic.AddInt32(&ran, 1)
			return x, nil
		}, 2)
		defer q.Stop()

		const n = 10
		for i := 0; i < n; i++ {
			if err := q.PushWithoutResult(ctx, i); err != nil {
				t.Fatalf("PushWithoutResult(%d) = %v", i, err)
			}
		}
		mustFinish(t, 2*time.Second, "WaitEmpty", q.WaitEmpty)

		if got := atomic.LoadInt32(&ran); got != n {
			t.Fatalf("handler ran %d times, want %d", got, n)
		}
	})
}

// ---------------------------------------------------------------------------
// 2. TestConcurrencyCap — workers actually bound parallelism.
// ---------------------------------------------------------------------------

func TestConcurrencyCap(t *testing.T) {
	ctx := context.Background()

	// peakTrackingHandler returns a handler that records the maximum number of
	// concurrent invocations into *peak.
	peakTrackingHandler := func(inflight, peak *atomic.Int32) func(context.Context, int) (int, error) {
		return func(_ context.Context, x int) (int, error) {
			cur := inflight.Add(1)
			defer inflight.Add(-1)
			for p := peak.Load(); cur > p; p = peak.Load() {
				if peak.CompareAndSwap(p, cur) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond) // give other tasks a chance to overlap
			return x, nil
		}
	}

	t.Run("peak_le_workers", func(t *testing.T) {
		const (
			workers = 4
			total   = 50
		)
		var inflight, peak atomic.Int32
		q := NewFastQueue(ctx, peakTrackingHandler(&inflight, &peak), workers)
		defer q.Stop()

		var wg sync.WaitGroup
		wg.Add(total)
		for i := 0; i < total; i++ {
			go func(i int) {
				defer wg.Done()
				_, _ = q.PushAndWait(ctx, i)
			}(i)
		}
		mustFinish(t, 5*time.Second, "all PushAndWait return", wg.Wait)

		if got := peak.Load(); got > workers {
			t.Fatalf("peak concurrent handlers = %d, exceeded cap %d", got, workers)
		}
		if got := peak.Load(); got < workers {
			// not strictly required by the API, but with 50 tasks vs 4 workers
			// we expect to actually saturate; otherwise the test wasn't really
			// exercising the cap.
			t.Logf("note: peak only reached %d (cap=%d); test may not have saturated", got, workers)
		}
	})

	t.Run("single_worker_serializes", func(t *testing.T) {
		var inflight, peak atomic.Int32
		q := NewFastQueue(ctx, peakTrackingHandler(&inflight, &peak), 1)
		defer q.Stop()

		var wg sync.WaitGroup
		wg.Add(20)
		for i := 0; i < 20; i++ {
			go func(i int) {
				defer wg.Done()
				_, _ = q.PushAndWait(ctx, i)
			}(i)
		}
		mustFinish(t, 5*time.Second, "all PushAndWait return", wg.Wait)

		if got := peak.Load(); got != 1 {
			t.Fatalf("peak concurrent handlers = %d, want 1 with single worker", got)
		}
	})
}

// ---------------------------------------------------------------------------
// 3. TestFIFOOrder — async pushes preserve enqueue order.
// ---------------------------------------------------------------------------
//
// Workers=1 so any reordering is the queue's fault, not "two workers raced
// to record observation". The handler appends payloads under a mutex; we
// then assert the recorded sequence equals [0..N).

func TestFIFOOrder(t *testing.T) {
	ctx := context.Background()
	const n = 50

	makeRecorder := func() (handler func(context.Context, int) (int, error), seen *[]int, mu *sync.Mutex) {
		mu = &sync.Mutex{}
		s := make([]int, 0, n)
		seen = &s
		handler = func(_ context.Context, x int) (int, error) {
			mu.Lock()
			*seen = append(*seen, x)
			mu.Unlock()
			return x, nil
		}
		return
	}

	assertSequential := func(t *testing.T, seen []int) {
		t.Helper()
		if len(seen) != n {
			t.Fatalf("recorded %d items, want %d", len(seen), n)
		}
		for i, v := range seen {
			if v != i {
				t.Fatalf("position %d = %d, want %d (full sequence: %v)", i, v, i, seen)
			}
		}
	}

	t.Run("PushWithCallback/order", func(t *testing.T) {
		handler, seen, mu := makeRecorder()
		q := NewFastQueue(ctx, handler, 1)
		defer q.Stop()

		for i := 0; i < n; i++ {
			if err := q.PushWithCallback(ctx, i, nil); err != nil {
				t.Fatalf("PushWithCallback(%d) = %v", i, err)
			}
		}
		mustFinish(t, 5*time.Second, "WaitEmpty", q.WaitEmpty)

		mu.Lock()
		defer mu.Unlock()
		assertSequential(t, *seen)
	})

	t.Run("PushWithoutResult/order", func(t *testing.T) {
		handler, seen, mu := makeRecorder()
		q := NewFastQueue(ctx, handler, 1)
		defer q.Stop()

		for i := 0; i < n; i++ {
			if err := q.PushWithoutResult(ctx, i); err != nil {
				t.Fatalf("PushWithoutResult(%d) = %v", i, err)
			}
		}
		mustFinish(t, 5*time.Second, "WaitEmpty", q.WaitEmpty)

		mu.Lock()
		defer mu.Unlock()
		assertSequential(t, *seen)
	})

	t.Run("mixed/order", func(t *testing.T) {
		handler, seen, mu := makeRecorder()
		q := NewFastQueue(ctx, handler, 1)
		defer q.Stop()

		for i := 0; i < n; i++ {
			var err error
			if i%2 == 0 {
				err = q.PushWithCallback(ctx, i, nil)
			} else {
				err = q.PushWithoutResult(ctx, i)
			}
			if err != nil {
				t.Fatalf("push %d = %v", i, err)
			}
		}
		mustFinish(t, 5*time.Second, "WaitEmpty", q.WaitEmpty)

		mu.Lock()
		defer mu.Unlock()
		assertSequential(t, *seen)
	})
}

// occupyWorkers fills every worker slot with a handler blocked on `release`.
// Returns once every worker is mid-handler. Caller must close(release).
func occupyWorkers(t *testing.T, workers int, release <-chan struct{}) *FastQueue[int, int] {
	t.Helper()
	started := make(chan struct{}, workers)
	q := NewFastQueue(context.Background(), func(_ context.Context, x int) (int, error) {
		started <- struct{}{}
		<-release
		return x, nil
	}, workers)
	for i := 0; i < workers; i++ {
		_ = q.PushWithoutResult(context.Background(), i)
	}
	for i := 0; i < workers; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("worker %d failed to start", i)
		}
	}
	return q
}

// ---------------------------------------------------------------------------
// 4. TestContext — per-task ctx and queue ctx propagation.
// ---------------------------------------------------------------------------

type ctxKey string

func TestContext(t *testing.T) {
	const key ctxKey = "k"

	t.Run("inherits_queue_ctx", func(t *testing.T) {
		queueCtx := context.WithValue(context.Background(), key, "queue")

		got := make(chan any, 1)
		q := NewFastQueue(queueCtx, func(ctx context.Context, x int) (int, error) {
			got <- ctx.Value(key)
			return x, nil
		}, 1)
		defer q.Stop()

		// nil per-task ctx → handler should see queueCtx.
		_, _ = q.PushAndWait(nil, 1) //nolint:staticcheck // intentional nil

		select {
		case v := <-got:
			if v != "queue" {
				t.Fatalf("handler ctx value = %v, want %q", v, "queue")
			}
		case <-time.After(time.Second):
			t.Fatal("handler never ran")
		}
	})

	t.Run("per_task_ctx_overrides", func(t *testing.T) {
		queueCtx := context.WithValue(context.Background(), key, "queue")
		taskCtx := context.WithValue(context.Background(), key, "task")

		got := make(chan any, 1)
		q := NewFastQueue(queueCtx, func(ctx context.Context, x int) (int, error) {
			got <- ctx.Value(key)
			return x, nil
		}, 1)
		defer q.Stop()

		_, _ = q.PushAndWait(taskCtx, 1)

		select {
		case v := <-got:
			if v != "task" {
				t.Fatalf("handler ctx value = %v, want %q", v, "task")
			}
		case <-time.After(time.Second):
			t.Fatal("handler never ran")
		}
	})

	t.Run("per_task_ctx_in_callback", func(t *testing.T) {
		taskCtx := context.WithValue(context.Background(), key, "cb")

		q := NewFastQueue(context.Background(), func(_ context.Context, x int) (int, error) {
			return x, nil
		}, 1)
		defer q.Stop()

		got := make(chan any, 1)
		_ = q.PushWithCallback(taskCtx, 1, func(ctx context.Context, _ int, _ error) {
			got <- ctx.Value(key)
		})

		select {
		case v := <-got:
			if v != "cb" {
				t.Fatalf("callback ctx value = %v, want %q", v, "cb")
			}
		case <-time.After(time.Second):
			t.Fatal("callback never fired")
		}
	})

	t.Run("worker_ctx_cancelled_by_stop", func(t *testing.T) {
		started := make(chan struct{})
		exited := make(chan error, 1)
		q := NewFastQueue(context.Background(), func(ctx context.Context, x int) (int, error) {
			close(started)
			<-ctx.Done()
			exited <- ctx.Err()
			return x, ctx.Err()
		}, 1)

		_ = q.PushWithoutResult(context.Background(), 1)
		<-started

		mustFinish(t, 2*time.Second, "Stop interrupts handler", q.Stop)

		select {
		case err := <-exited:
			if err == nil {
				t.Fatal("handler observed ctx.Err()==nil after Stop")
			}
		case <-time.After(time.Second):
			t.Fatal("handler never observed ctx cancellation")
		}
	})
}

// ---------------------------------------------------------------------------
// 5. TestPauseResume
// ---------------------------------------------------------------------------

func TestPauseResume(t *testing.T) {
	ctx := context.Background()

	t.Run("pause_blocks_new_starts", func(t *testing.T) {
		const (
			workers = 2
			total   = 6
		)
		release := make(chan struct{})
		started := make(chan struct{}, total) // big enough so handlers never block here
		var ran int32

		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			started <- struct{}{}
			<-release
			atomic.AddInt32(&ran, 1)
			return x, nil
		}, workers)
		defer q.Stop()

		// Saturate the workers, then pause. Workers are inside the handler so
		// the pause check at the top of their loop only fires after they
		// finish the current task.
		for i := 0; i < total; i++ {
			_ = q.PushWithoutResult(ctx, i)
		}
		for i := 0; i < workers; i++ {
			<-started
		}

		q.Pause()
		close(release) // let the in-flight pair finish

		// Wait long enough for any wrongly-unpaused worker to pick up the next
		// task. With 2 workers paused, only the initial 2 should ever run.
		time.Sleep(50 * time.Millisecond)
		if got := atomic.LoadInt32(&ran); got != workers {
			t.Fatalf("handlers completed during pause: got %d, want %d", got, workers)
		}

		q.Resume()
		mustFinish(t, 2*time.Second, "WaitEmpty after resume", q.WaitEmpty)

		if got := atomic.LoadInt32(&ran); got != total {
			t.Fatalf("after resume: ran=%d, want %d", got, total)
		}
	})

	t.Run("pause_with_busy_workers_does_not_deadlock", func(t *testing.T) {
		release := make(chan struct{})
		q := occupyWorkers(t, 2, release)
		// Defers run LIFO: close(release) first → handlers exit → Stop can drain.
		defer q.Stop()
		defer close(release)

		mustFinish(t, 2*time.Second, "Pause with busy workers", q.Pause)
	})

	t.Run("resume_with_busy_workers_does_not_deadlock", func(t *testing.T) {
		release := make(chan struct{})
		q := occupyWorkers(t, 2, release)
		defer q.Stop()
		defer close(release)

		q.Pause()
		mustFinish(t, 2*time.Second, "Resume with busy workers", q.Resume)
	})

	t.Run("multiple_cycles", func(t *testing.T) {
		var ran int32
		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			atomic.AddInt32(&ran, 1)
			return x, nil
		}, 2)
		defer q.Stop()

		const total = 20
		for i := 0; i < total; i++ {
			_ = q.PushWithoutResult(ctx, i)
		}

		q.Pause()
		q.Resume()
		q.Pause()
		q.Resume()

		mustFinish(t, 2*time.Second, "WaitEmpty after cycles", q.WaitEmpty)
		if got := atomic.LoadInt32(&ran); got != total {
			t.Fatalf("ran=%d after cycles, want %d", got, total)
		}
	})
}

// ---------------------------------------------------------------------------
// 6. TestStop
// ---------------------------------------------------------------------------

func TestStop(t *testing.T) {
	ctx := context.Background()
	identity := func(_ context.Context, x int) (int, error) { return x, nil }

	t.Run("idle", func(t *testing.T) {
		q := NewFastQueue(ctx, identity, 2)
		mustFinish(t, 2*time.Second, "Stop on idle queue", q.Stop)
	})

	t.Run("idempotent", func(t *testing.T) {
		q := NewFastQueue(ctx, identity, 2)
		mustFinish(t, 2*time.Second, "first Stop", q.Stop)
		mustFinish(t, 2*time.Second, "second Stop", q.Stop)
	})

	t.Run("concurrent_callers", func(t *testing.T) {
		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			time.Sleep(10 * time.Millisecond)
			return x, nil
		}, 4)

		for i := 0; i < 10; i++ {
			_ = q.PushWithoutResult(ctx, i)
		}

		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				q.Stop()
			}()
		}
		mustFinish(t, 5*time.Second, "all Stop callers return", wg.Wait)
	})

	t.Run("rejects_further_push", func(t *testing.T) {
		q := NewFastQueue(ctx, identity, 2)
		mustFinish(t, 2*time.Second, "Stop", q.Stop)

		if _, err := q.PushAndWait(ctx, 1); !errors.Is(err, ErrQueueStopped) {
			t.Errorf("PushAndWait after Stop: got %v, want ErrQueueStopped", err)
		}
		if err := q.PushWithCallback(ctx, 1, nil); !errors.Is(err, ErrQueueStopped) {
			t.Errorf("PushWithCallback after Stop: got %v, want ErrQueueStopped", err)
		}
		if err := q.PushWithoutResult(ctx, 1); !errors.Is(err, ErrQueueStopped) {
			t.Errorf("PushWithoutResult after Stop: got %v, want ErrQueueStopped", err)
		}
	})

	t.Run("interrupts_running_workers", func(t *testing.T) {
		started := make(chan struct{}, 1)
		q := NewFastQueue(ctx, func(taskCtx context.Context, x int) (int, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-taskCtx.Done()
			return 0, taskCtx.Err()
		}, 2)

		_ = q.PushWithoutResult(ctx, 1)
		<-started // ensure at least one worker is mid-handler

		mustFinish(t, 2*time.Second, "Stop with running workers", q.Stop)
	})

	t.Run("cancels_queued_tasks_pushandwait", func(t *testing.T) {
		release := make(chan struct{})
		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			<-release
			return x, nil
		}, 2)

		const n = 10
		var pushWg sync.WaitGroup
		errs := make([]error, n)
		for i := 0; i < n; i++ {
			i := i
			pushWg.Add(1)
			go func() {
				defer pushWg.Done()
				_, errs[i] = q.PushAndWait(ctx, i)
			}()
		}

		// Let all pushers attach before stopping.
		time.Sleep(50 * time.Millisecond)

		// Unblock running handlers so workerWaitGroup can drain.
		close(release)

		mustFinish(t, 5*time.Second, "Stop with queued tasks", q.Stop)
		mustFinish(t, 5*time.Second, "all pushers return", pushWg.Wait)

		ok, cancelled := 0, 0
		for _, err := range errs {
			switch {
			case err == nil:
				ok++
			case errors.Is(err, ErrTaskCancelled):
				cancelled++
			default:
				t.Errorf("unexpected push error: %v", err)
			}
		}
		if ok+cancelled != n {
			t.Fatalf("lost pushes: ok=%d cancelled=%d total=%d", ok, cancelled, n)
		}
	})

	t.Run("cancels_queued_tasks_pushwithcallback", func(t *testing.T) {
		release := make(chan struct{})
		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			<-release
			return x, nil
		}, 2)

		const n = 10
		var cbWg sync.WaitGroup
		errs := make([]error, n)
		for i := 0; i < n; i++ {
			i := i
			cbWg.Add(1)
			_ = q.PushWithCallback(ctx, i, func(_ context.Context, _ int, e error) {
				errs[i] = e
				cbWg.Done()
			})
		}

		time.Sleep(50 * time.Millisecond)
		close(release)

		mustFinish(t, 5*time.Second, "Stop with queued callbacks", q.Stop)
		mustFinish(t, 5*time.Second, "all callbacks fire", cbWg.Wait)

		ok, cancelled := 0, 0
		for _, err := range errs {
			switch {
			case err == nil:
				ok++
			case errors.Is(err, ErrTaskCancelled):
				cancelled++
			default:
				t.Errorf("unexpected callback error: %v", err)
			}
		}
		if ok+cancelled != n {
			t.Fatalf("lost callbacks: ok=%d cancelled=%d total=%d", ok, cancelled, n)
		}
	})

	t.Run("paused_queue", func(t *testing.T) {
		q := NewFastQueue(ctx, identity, 2)
		q.Pause()
		mustFinish(t, 3*time.Second, "Stop on paused queue", q.Stop)
	})

	t.Run("does_not_strand_concurrent_push", func(t *testing.T) {
		const (
			trials  = 20
			pushers = 100
			workers = 4
		)

		for trial := 0; trial < trials; trial++ {
			q := NewFastQueue(ctx, identity, workers)

			var pushWg sync.WaitGroup
			for i := 0; i < pushers; i++ {
				pushWg.Add(1)
				go func() {
					defer pushWg.Done()
					_, _ = q.PushAndWait(ctx, 1)
				}()
			}

			mustFinish(t, 3*time.Second,
				fmt.Sprintf("trial %d: pushers return", trial), func() {
					q.Stop()
					pushWg.Wait()
				})

			if p := q.Pending(); p != 0 {
				t.Fatalf("trial %d: %d task(s) stranded after Stop", trial, p)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// 7. TestClear
// ---------------------------------------------------------------------------

func TestClear(t *testing.T) {
	ctx := context.Background()

	t.Run("drops_queued_tasks", func(t *testing.T) {
		const workers = 2
		release := make(chan struct{})
		q := occupyWorkers(t, workers, release)
		defer q.Stop()
		defer close(release)

		// Workers are busy, so the next `workers` PushAndWait sends fit in the
		// input buffer (cap == numberOfWorkers). Spawn exactly that many so
		// nobody blocks at the send — Clear can then drain them all.
		const buffered = workers
		errs := make([]error, buffered)
		var wg sync.WaitGroup
		for i := 0; i < buffered; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[i] = q.PushAndWait(ctx, 100+i)
			}()
		}
		// Give the buffered sends time to land in q.input.
		time.Sleep(50 * time.Millisecond)

		failedBefore := q.Failed()
		q.Clear()
		mustFinish(t, 2*time.Second, "cleared pushers return", wg.Wait)

		for i, err := range errs {
			if !errors.Is(err, ErrTaskCancelled) {
				t.Errorf("buffered push %d: got %v, want ErrTaskCancelled", i, err)
			}
		}
		if delta := q.Failed() - failedBefore; delta != buffered {
			t.Errorf("Failed delta after Clear = %d, want %d", delta, buffered)
		}
	})

	t.Run("does_not_affect_running", func(t *testing.T) {
		release := make(chan struct{})
		var ran int32
		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			<-release
			atomic.AddInt32(&ran, 1)
			return x, nil
		}, 1)
		defer q.Stop()

		done := make(chan error, 1)
		go func() {
			_, err := q.PushAndWait(ctx, 1)
			done <- err
		}()
		// Wait for the handler to actually start so Clear can't clip it from
		// the buffer. PushAndWait returns once the worker grabs the task; we
		// approximate that with a small sleep — Pending drops to 1 only after
		// the worker has picked it up.
		time.Sleep(50 * time.Millisecond)

		q.Clear()
		close(release) // let the running handler finish

		select {
		case err := <-done:
			if err != nil {
				t.Errorf("running task returned err=%v after Clear", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("running task never returned")
		}
		if atomic.LoadInt32(&ran) != 1 {
			t.Fatalf("running handler did not complete: ran=%d", ran)
		}
	})

	t.Run("noop_when_stopped", func(t *testing.T) {
		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			return x, nil
		}, 2)
		mustFinish(t, 2*time.Second, "Stop", q.Stop)

		// Must not panic, must return promptly.
		mustFinish(t, time.Second, "Clear after Stop", q.Clear)
	})
}

// ---------------------------------------------------------------------------
// 8. TestWaitEmpty
// ---------------------------------------------------------------------------

func TestWaitEmpty(t *testing.T) {
	ctx := context.Background()

	t.Run("returns_immediately_when_idle", func(t *testing.T) {
		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			return x, nil
		}, 2)
		defer q.Stop()

		mustFinish(t, time.Second, "WaitEmpty on idle queue", q.WaitEmpty)
	})

	t.Run("mix_of_variants", func(t *testing.T) {
		var ran int32
		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			atomic.AddInt32(&ran, 1)
			return x, nil
		}, 4)
		defer q.Stop()

		const each = 5
		// PushAndWait — sync, so wrap each in a goroutine.
		var pawWg sync.WaitGroup
		pawWg.Add(each)
		for i := 0; i < each; i++ {
			go func(i int) {
				defer pawWg.Done()
				_, _ = q.PushAndWait(ctx, i)
			}(i)
		}
		// PushWithCallback — async.
		for i := 0; i < each; i++ {
			_ = q.PushWithCallback(ctx, i, nil)
		}
		// PushWithoutResult — async.
		for i := 0; i < each; i++ {
			_ = q.PushWithoutResult(ctx, i)
		}

		mustFinish(t, 5*time.Second, "WaitEmpty over mixed pushes", q.WaitEmpty)
		pawWg.Wait()

		if got := atomic.LoadInt32(&ran); got != 3*each {
			t.Fatalf("ran=%d after WaitEmpty, want %d", got, 3*each)
		}
	})
}

// ---------------------------------------------------------------------------
// 9. TestCountersAndStatus
// ---------------------------------------------------------------------------

func TestCountersAndStatus(t *testing.T) {
	ctx := context.Background()
	identity := func(_ context.Context, x int) (int, error) { return x, nil }

	t.Run("pending_counts_queued_plus_inflight", func(t *testing.T) {
		const workers = 2
		release := make(chan struct{})
		q := occupyWorkers(t, workers, release)
		defer q.Stop()
		defer close(release)

		// occupyWorkers already pushed `workers` tasks (in-flight).
		const extra = 3
		for i := 0; i < extra; i++ {
			_ = q.PushWithoutResult(ctx, 100+i)
		}
		// Give async sends a moment to fully register.
		time.Sleep(30 * time.Millisecond)

		want := workers + extra
		if got := q.Pending(); got != want {
			t.Errorf("Pending() = %d, want %d", got, want)
		}
		if got := q.Status().Pending; got != want {
			t.Errorf("Status().Pending = %d, want %d", got, want)
		}
	})

	// Note: WaitEmpty releases as soon as wgQueue.Done() fires inside the
	// worker, but the atomic counter increments (Completed/Failed) happen
	// *after* that Done(). So WaitEmpty + immediate Failed()/Completed()
	// reads are racy. We call Stop() before reading counters: Stop waits on
	// workerWaitGroup, which only fires after the worker re-enters its for
	// loop (i.e. after the atomics have settled).

	t.Run("completed_increments_on_success_and_error", func(t *testing.T) {
		boom := errors.New("boom")
		q := NewFastQueue(ctx, func(_ context.Context, x int) (int, error) {
			if x%2 == 1 {
				return 0, boom
			}
			return x, nil
		}, 2)

		const n = 10
		for i := 0; i < n; i++ {
			_ = q.PushWithoutResult(ctx, i)
		}
		mustFinish(t, 2*time.Second, "WaitEmpty", q.WaitEmpty)
		mustFinish(t, 2*time.Second, "Stop (settle counters)", q.Stop)

		if got := q.Completed(); got != n {
			t.Errorf("Completed() = %d, want %d", got, n)
		}
		if got := q.Failed(); got != n/2 {
			t.Errorf("Failed() = %d, want %d", got, n/2)
		}
	})

	t.Run("failed_counts_handler_error", func(t *testing.T) {
		boom := errors.New("boom")
		q := NewFastQueue(ctx, func(_ context.Context, _ int) (int, error) {
			return 0, boom
		}, 2)

		const n = 7
		for i := 0; i < n; i++ {
			_ = q.PushWithoutResult(ctx, i)
		}
		mustFinish(t, 2*time.Second, "WaitEmpty", q.WaitEmpty)
		mustFinish(t, 2*time.Second, "Stop (settle counters)", q.Stop)

		if got := q.Failed(); got != n {
			t.Errorf("Failed() = %d, want %d", got, n)
		}
	})

	t.Run("failed_counts_cancellation", func(t *testing.T) {
		// Stop's drain races with workers picking up buffered tasks: the
		// worker's select sees both `workerContext.Done()` and `<-q.input`
		// ready and picks randomly. We can't predict the cancelled-vs-
		// completed split, but every push must terminate and Failed must
		// equal the number of ErrTaskCancelled outcomes.
		const workers = 2
		release := make(chan struct{})
		q := occupyWorkers(t, workers, release)

		const extra = 2
		errs := make([]error, extra)
		var wg sync.WaitGroup
		for i := 0; i < extra; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[i] = q.PushAndWait(ctx, 100+i)
			}()
		}
		time.Sleep(50 * time.Millisecond)

		go func() {
			time.Sleep(20 * time.Millisecond)
			close(release)
		}()
		mustFinish(t, 5*time.Second, "Stop with buffered tasks", q.Stop)
		mustFinish(t, 5*time.Second, "buffered pushers return", wg.Wait)

		cancelled := 0
		for i, err := range errs {
			switch {
			case err == nil:
				// worker raced the drain and processed it
			case errors.Is(err, ErrTaskCancelled):
				cancelled++
			default:
				t.Errorf("buffered push %d: unexpected err %v", i, err)
			}
		}
		if got := q.Failed(); got != cancelled {
			t.Errorf("Failed() = %d, want %d (one per ErrTaskCancelled)", got, cancelled)
		}
	})

	t.Run("status_ready_when_idle", func(t *testing.T) {
		q := NewFastQueue(ctx, identity, 2)
		defer q.Stop()
		if got := q.Status().Status; got != Ready {
			t.Errorf("idle queue: Status = %q, want %q", got, Ready)
		}
	})

	t.Run("status_stopped", func(t *testing.T) {
		q := NewFastQueue(ctx, identity, 2)
		mustFinish(t, 2*time.Second, "Stop", q.Stop)
		if got := q.Status().Status; got != Stopped {
			t.Errorf("stopped queue: Status = %q, want %q", got, Stopped)
		}
	})

	t.Run("status_paused", func(t *testing.T) {
		q := NewFastQueue(ctx, identity, 2)
		defer q.Stop()
		q.Pause()
		if got := q.Status().Status; got != Paused {
			t.Errorf("paused queue: Status = %q, want %q", got, Paused)
		}
	})

	t.Run("status_running_with_inflight", func(t *testing.T) {
		release := make(chan struct{})
		q := occupyWorkers(t, 2, release)
		defer q.Stop()
		defer close(release)

		if got := q.Status().Status; got != Running {
			t.Errorf("queue with active tasks: Status = %q, want %q", got, Running)
		}
	})
}
