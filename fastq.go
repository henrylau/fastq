// Package fastq provides a small, generic worker queue with bounded
// concurrency, FIFO admission for async pushes, pause/resume, and graceful
// shutdown.
//
// A FastQueue runs a fixed pool of worker goroutines that consume tasks
// produced by PushAndWait, PushWithCallback, or PushWithoutResult. Each
// worker invokes the user-supplied handler and reports the outcome via the
// pushing API or by updating the queue's pending/completed/failed counters.
package fastq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// Callback is invoked by PushWithCallback once the worker finishes a task.
// err is either the handler's error or ErrTaskCancelled if the task was
// cancelled before execution by Stop.
type Callback[R any] func(ctx context.Context, result R, error error)

// TaskInput is the internal envelope sent from a Push* call to a worker.
// Result is nil for fire-and-forget pushes; Context, when non-nil, is used
// as the parent context for the handler's per-task context.
type TaskInput[T any, R any] struct {
	Payload T
	Result  chan Result[R]
	Context context.Context
}

// Result is the value a worker delivers back to a synchronous or
// callback-based pusher.
type Result[R any] struct {
	Result R
	Error  error
}

// QueueStats is a snapshot of the queue's counters and lifecycle status,
// returned by FastQueue.Status.
type QueueStats struct {
	Pending   int
	Completed int
	Failed    int
	Status    QueueStatus
}

// QueueStatus is the lifecycle state reported by QueueStats.Status.
type QueueStatus string

// Lifecycle states reported by QueueStats.Status.
//
//	Running  - workers are processing tasks (pending > 0)
//	Paused   - Pause has been called and Resume has not
//	Ready    - the queue is up but has no pending work
//	Stopped  - Stop has been called; no further tasks will run
const (
	Running QueueStatus = "Running"
	Paused  QueueStatus = "Paused"
	Ready   QueueStatus = "Ready"
	Stopped QueueStatus = "Stopped"
)

// FastQueue is a typed, bounded worker pool. T is the task payload type
// and R is the handler's result type. Construct with NewFastQueue and
// shut down with Stop. All exported methods are safe to call from
// multiple goroutines.
type FastQueue[T any, R any] struct {
	// task handler
	handler func(ctx context.Context, payload T) (R, error)

	input           chan TaskInput[T, R]
	pushWaitGroup   sync.WaitGroup
	stopped         bool
	stoppedChan     chan struct{}
	numberOfWorkers int
	mu              *sync.Mutex
	ctx             context.Context
	workerWaitGroup sync.WaitGroup
	workerContext   context.Context
	cancel          context.CancelFunc

	// wait group for queue completed
	wgQueue  sync.WaitGroup
	sendTail chan struct{}

	// atomic counters
	pending   int32
	completed int32
	failed    int32

	// pause control
	paused     bool
	pausedCond *sync.Cond
}

// ErrTaskCancelled is returned (or delivered to a callback) when a task
// was admitted to the queue but never executed because the queue was
// stopped or the input buffer was drained by Clear.
var ErrTaskCancelled = errors.New("task cancelled")

// ErrQueueStopped is returned by any Push* call made after Stop has begun.
// The task is not admitted and no callback fires.
var ErrQueueStopped = errors.New("queue stopped")

// NewFastQueue creates a FastQueue with numberOfWorkers worker goroutines
// and starts them. ctx is the default parent context handlers receive when
// a Push* call passes a nil context. handler is invoked once per task; its
// returned (R, error) is delivered to the pushing API.
//
// Call Stop to release worker goroutines.
func NewFastQueue[T any, R any](ctx context.Context, handler func(ctx context.Context, payload T) (R, error), numberOfWorkers int) *FastQueue[T, R] {
	workerContext, cancel := context.WithCancel(context.Background())

	q := &FastQueue[T, R]{
		handler:         handler,
		input:           make(chan TaskInput[T, R], numberOfWorkers), // Buffer for input channel
		numberOfWorkers: numberOfWorkers,
		ctx:             ctx,
		stopped:         false,
		stoppedChan:     make(chan struct{}),
		cancel:          cancel,
		workerContext:   workerContext,
		mu:              &sync.Mutex{},
	}

	q.pausedCond = sync.NewCond(q.mu)

	// start the queue
	q.start()

	return q
}

func (q *FastQueue[T, R]) start() error {
	// Start multiple worker goroutines
	for i := 0; i < q.numberOfWorkers; i++ {
		q.workerWaitGroup.Add(1)
		go q.worker(i)
	}

	return nil
}

func (q *FastQueue[T, R]) isStopped() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	stopped := q.stopped
	return stopped
}

// NumberOfWorkers returns the worker pool size set at construction.
func (q *FastQueue[T, R]) NumberOfWorkers() int {
	return q.numberOfWorkers
}

func (q *FastQueue[T, R]) worker(workerID int) {
	defer q.workerWaitGroup.Done()

	for {
		q.mu.Lock()
		for q.paused {
			q.pausedCond.Wait()
		}
		q.mu.Unlock()

		select {
		case <-q.workerContext.Done():
			// fmt.Printf("worker %d stopping due to context cancellation\n", workerID)
			return
		case taskInput, ok := <-q.input:
			if !ok {
				// fmt.Printf("worker %d stopping due to input channel closure\n", workerID)
				return
			}

			ctx := q.ctx
			if taskInput.Context != nil {
				ctx = taskInput.Context
			}

			taskCtx, taskCancel := context.WithCancel(ctx)
			stop := context.AfterFunc(q.workerContext, taskCancel)

			// execute the task
			result, err := q.handler(taskCtx, taskInput.Payload)

			// remove the cancel function
			stop()
			taskCancel()

			q.wgQueue.Done()
			atomic.AddInt32(&q.pending, -1)
			atomic.AddInt32(&q.completed, 1)
			if err != nil {
				atomic.AddInt32(&q.failed, 1)
			}

			// If result channel is not nil, send result to it
			if taskInput.Result != nil {
				taskInput.Result <- Result[R]{Result: result, Error: err}
			}
		}
	}
}

// PushAndWait submits payload and blocks until a worker returns its
// result. If ctx is non-nil, the handler receives a context derived from
// it (also cancelled when the queue stops).
//
// Returns ErrQueueStopped if Stop has already begun and the task is not
// admitted, or ErrTaskCancelled if the task was admitted but cancelled
// by Stop or Clear before execution. Otherwise returns the handler's
// (R, error).
func (q *FastQueue[T, R]) PushAndWait(ctx context.Context, payload T) (R, error) {
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return *new(R), ErrQueueStopped
	}

	q.pushWaitGroup.Add(1)
	q.wgQueue.Add(1)
	atomic.AddInt32(&q.pending, 1)
	q.mu.Unlock()

	defer q.pushWaitGroup.Done()

	resultChan := make(chan Result[R], 1)

	// Past the admission check — the caller saw the queue as Running, so if
	// Stop races in and wins the send select, surface that as a task-level
	// cancellation rather than ErrQueueStopped.
	select {
	case q.input <- TaskInput[T, R]{Payload: payload, Result: resultChan, Context: ctx}:
	case <-q.stoppedChan:
		q.wgQueue.Done()
		atomic.AddInt32(&q.pending, -1)
		atomic.AddInt32(&q.failed, 1)
		return *new(R), ErrTaskCancelled
	}

	// Task is committed. A worker delivers the real result, or Stop's drain
	// (running concurrently with pushWaitGroup.Wait) delivers ErrTaskCancelled.
	result := <-resultChan
	return result.Result, result.Error
}

// PushWithCallback submits payload and returns immediately. callback is
// invoked from a separate goroutine after the worker finishes — with the
// handler's result, or with ErrTaskCancelled if Stop cancels the task
// before execution.
//
// Tasks submitted via PushWithCallback are enqueued in the order their
// PushWithCallback calls returned; the callbacks themselves fire in
// worker-completion order.
//
// Returns ErrQueueStopped if Stop has already begun.
func (q *FastQueue[T, R]) PushWithCallback(ctx context.Context, payload T, callback Callback[R]) error {
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return ErrQueueStopped
	}

	q.pushWaitGroup.Add(1)
	q.wgQueue.Add(1)
	atomic.AddInt32(&q.pending, 1)

	prev := q.sendTail
	myPushTask := make(chan struct{})
	q.sendTail = myPushTask

	q.mu.Unlock()

	resultChan := make(chan Result[R], 1)

	go func() {
		defer q.pushWaitGroup.Done()

		// wait previous enqueue operation completed
		if prev != nil {
			<-prev
		}

		var stopped bool
		func() {
			defer close(myPushTask)
			select {
			case q.input <- TaskInput[T, R]{Payload: payload, Result: resultChan, Context: ctx}:
			case <-q.stoppedChan:
				stopped = true
			}
		}()

		if stopped {
			q.wgQueue.Done()
			atomic.AddInt32(&q.pending, -1)
			atomic.AddInt32(&q.failed, 1)
			if callback != nil {
				callback(ctx, *new(R), ErrTaskCancelled)
			}
			return
		}

		if callback != nil {
			result := <-resultChan
			callback(ctx, result.Result, result.Error)
		}
	}()

	return nil
}

// PushWithoutResult submits payload fire-and-forget and returns
// immediately. The handler's return value is discarded; an error increments
// the Failed counter. Use WaitEmpty to wait for all queued tasks to drain.
//
// Tasks submitted via PushWithoutResult are enqueued in the order their
// PushWithoutResult calls returned.
//
// Returns ErrQueueStopped if Stop has already begun.
func (q *FastQueue[T, R]) PushWithoutResult(ctx context.Context, payload T) error {
	// q.keepOrder <- struct{}{}
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return ErrQueueStopped
	}

	q.pushWaitGroup.Add(1)
	q.wgQueue.Add(1)
	atomic.AddInt32(&q.pending, 1)

	// keep the push operation in order
	prev := q.sendTail
	myPushTask := make(chan struct{})
	q.sendTail = myPushTask

	q.mu.Unlock()

	go func() {
		defer q.pushWaitGroup.Done()
		defer close(myPushTask)

		// wait previous enqueue operation completed
		if prev != nil {
			<-prev
		}

		select {
		case q.input <- TaskInput[T, R]{Payload: payload, Result: nil, Context: ctx}:
		case <-q.stoppedChan:
			// send error means worker is stopped and task is not executed need to decrement pending and failed
			q.wgQueue.Done()
			atomic.AddInt32(&q.pending, -1)
			atomic.AddInt32(&q.failed, 1)
		}
	}()

	return nil
}

// WaitEmpty blocks until every admitted task has been processed
// (completed, failed, or cancelled). It does not prevent further pushes;
// pair it with no concurrent pushers, or with Stop, for a deterministic
// drain.
func (q *FastQueue[T, R]) WaitEmpty() {
	q.wgQueue.Wait()
}

// Pending returns the number of tasks admitted but not yet finished
// (queued plus in-flight).
func (q *FastQueue[T, R]) Pending() int {
	return int(atomic.LoadInt32(&q.pending))
}

// Completed returns the cumulative number of handlers that have returned,
// including those that returned an error.
func (q *FastQueue[T, R]) Completed() int {
	return int(atomic.LoadInt32(&q.completed))
}

// Failed returns the cumulative number of tasks that ended with an error
// — either the handler returned a non-nil error or the task was cancelled
// (ErrTaskCancelled).
func (q *FastQueue[T, R]) Failed() int {
	return int(atomic.LoadInt32(&q.failed))
}

// Pause halts new task pickup. Workers currently running a handler finish
// it; they then block until Resume or Stop. Pause is idempotent and safe
// to call concurrently with pushes.
func (q *FastQueue[T, R]) Pause() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.paused = true
}

// Resume wakes paused workers so they can pick up new tasks. It is a
// no-op if the queue is not paused.
func (q *FastQueue[T, R]) Resume() {
	q.mu.Lock()
	q.paused = false
	q.pausedCond.Broadcast()
	q.mu.Unlock()
}

// Stop performs a graceful shutdown:
//
//  1. Subsequent Push* calls return ErrQueueStopped.
//  2. Worker contexts are cancelled so handlers that respect ctx exit
//     early.
//  3. Tasks still in the buffer are drained and surfaced as
//     ErrTaskCancelled to their result channels or callbacks.
//  4. Stop returns once all workers and pending pushes have settled.
//
// Stop is safe to call multiple times and from any goroutine.
func (q *FastQueue[T, R]) Stop() {
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}

	// reset the queue
	q.stopped = true
	close(q.stoppedChan)

	// stop the workers
	q.cancel()

	// unblock paused workers
	if q.paused {
		q.paused = false
		q.pausedCond.Broadcast()
	}

	q.mu.Unlock()

	// Workers must exit before draining so they don't race the drain for tasks.
	q.workerWaitGroup.Wait()

	// Drain concurrently with pushWaitGroup.Wait. A Push whose send won the
	// race against stoppedChan has a task committed to q.input and is blocked
	// on <-resultChan; only this drain can unblock it by delivering
	// ErrTaskCancelled. Once all Pushes have finished, do a final non-blocking
	// sweep to clear anything that arrived in the interim.
	pushDone := make(chan struct{})
	go func() {
		q.pushWaitGroup.Wait()
		close(pushDone)
	}()

	cancelTask := func(taskInput TaskInput[T, R]) {
		q.wgQueue.Done()
		atomic.AddInt32(&q.pending, -1)
		atomic.AddInt32(&q.failed, 1)
		if taskInput.Result != nil {
			taskInput.Result <- Result[R]{Error: ErrTaskCancelled}
		}
	}

	for {
		select {
		case taskInput := <-q.input:
			cancelTask(taskInput)
		case <-pushDone:
			for {
				select {
				case taskInput := <-q.input:
					cancelTask(taskInput)
				default:
					return
				}
			}
		}
	}
}

// Status returns a snapshot of the queue's counters and lifecycle state.
// The snapshot is taken under the queue's mutex so the four fields are
// mutually consistent.
func (q *FastQueue[T, R]) Status() QueueStats {
	q.mu.Lock()
	defer q.mu.Unlock()

	status := QueueStats{
		Pending:   int(atomic.LoadInt32(&q.pending)),
		Completed: int(atomic.LoadInt32(&q.completed)),
		Failed:    int(atomic.LoadInt32(&q.failed)),
		Status:    Running,
	}

	if q.stopped {
		status.Status = Stopped
	} else if q.paused {
		status.Status = Paused
	} else if status.Pending == 0 {
		status.Status = Ready
	}
	return status
}

// Clear discards every task currently sitting in the input buffer,
// delivering ErrTaskCancelled to any waiting PushAndWait callers and to
// PushWithCallback callbacks. Tasks already executing on a worker run to
// completion. Clear is a no-op once Stop has begun.
func (q *FastQueue[T, R]) Clear() {
	if q.isStopped() {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	// Drain all pending tasks from input channel
	for {
		select {
		case taskInput, ok := <-q.input:
			if !ok {
				return
			}
			// Decrement WaitGroup for this cancelled task
			q.wgQueue.Done()
			atomic.AddInt32(&q.pending, -1)
			atomic.AddInt32(&q.failed, 1)

			// Send error to result channel if present
			if taskInput.Result != nil {
				taskInput.Result <- Result[R]{Error: ErrTaskCancelled}
			}
		default:
			// No more pending tasks in channel
			return
		}
	}
}

// Queue is the type-erased lifecycle interface implemented by any
// *FastQueue[T, R]. It exposes only methods that do not depend on the
// task or result types, so callers that just need to inspect or control
// a queue can hold a Queue value without specifying its generics.
type Queue interface {
	// Pending returns the number of tasks admitted but not yet finished.
	Pending() int
	// Completed returns the cumulative count of finished tasks.
	Completed() int
	// Failed returns the cumulative count of tasks that ended with an error.
	Failed() int
	// Status returns a snapshot of counters and lifecycle state.
	Status() QueueStats

	// Pause halts new task pickup; in-flight handlers run to completion.
	Pause()
	// Resume wakes paused workers.
	Resume()
	// Clear discards queued (but not in-flight) tasks.
	Clear()
	// Stop performs a graceful shutdown.
	Stop()

	// WaitEmpty blocks until every admitted task has finished.
	WaitEmpty()
}

var _ Queue = &FastQueue[any, any]{}
