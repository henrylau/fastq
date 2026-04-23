package fastq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

type Callback[R any] func(ctx context.Context, result R, error error)

type TaskInput[T any, R any] struct {
	Payload  T
	Result   chan Result[R]
	Callback Callback[R]
	Context  context.Context
}

type Result[R any] struct {
	Result R
	Error  error
}

var ErrTaskCancelled = errors.New("task cancelled")

type QueueStats struct {
	Pending   int
	Completed int
	Failed    int
	Status    QueueStatus
}

type QueueStatus string

var ErrQueueStopped = errors.New("queue is stopped")

const (
	Running QueueStatus = "Running"
	Paused  QueueStatus = "Paused"
	Ready   QueueStatus = "Ready"
	Stopped QueueStatus = "Stopped"
)

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
	wgQueue sync.WaitGroup

	// atomic counters
	pending   int32
	completed int32
	failed    int32

	// pause control
	paused     bool
	pausedCond *sync.Cond
}

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

func (q *FastQueue[T, R]) Push(ctx context.Context, payload T, callback Callback[R]) (R, error) {
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
	case q.input <- TaskInput[T, R]{Payload: payload, Result: resultChan, Callback: callback, Context: ctx}:
	case <-q.stoppedChan:
		q.wgQueue.Done()
		atomic.AddInt32(&q.pending, -1)
		atomic.AddInt32(&q.failed, 1)
		return *new(R), ErrTaskCancelled
	}

	// Task is committed. A worker delivers the real result, or Stop's drain
	// (running concurrently with pushWaitGroup.Wait) delivers ErrTaskCancelled.
	result := <-resultChan

	if callback != nil {
		callbackContext := ctx
		if callbackContext == nil {
			callbackContext = q.ctx
		}
		go callback(callbackContext, result.Result, result.Error)
	}

	return result.Result, result.Error
}

func (q *FastQueue[T, R]) PushWithoutResult(ctx context.Context, payload T) error {
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return ErrQueueStopped
	}

	q.pushWaitGroup.Add(1)
	q.wgQueue.Add(1)
	atomic.AddInt32(&q.pending, 1)
	q.mu.Unlock()

	defer q.pushWaitGroup.Done()

	select {
	case q.input <- TaskInput[T, R]{Payload: payload, Result: nil, Callback: nil, Context: ctx}:
		return nil
	case <-q.stoppedChan:
		// send error means worker is stopped and task is not executed need to decrement pending and failed
		q.wgQueue.Done()
		atomic.AddInt32(&q.pending, -1)
		return ErrQueueStopped
	}
}

func (q *FastQueue[T, R]) Drained() {
	q.wgQueue.Wait()
}

func (q *FastQueue[T, R]) Pending() int {
	return int(atomic.LoadInt32(&q.pending))
}

func (q *FastQueue[T, R]) Pause() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.paused = true
}

func (q *FastQueue[T, R]) Resume() {
	q.mu.Lock()
	q.paused = false
	q.pausedCond.Broadcast()
	q.mu.Unlock()
}

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

func (q *FastQueue[T, R]) Status() QueueStats {
	q.mu.Lock()
	defer q.mu.Unlock()

	pending := int(atomic.LoadInt32(&q.completed))

	status := Running
	if q.stopped {
		status = Stopped
	} else if q.paused {
		status = Paused
	} else if pending == 0 {
		status = Ready
	}

	return QueueStats{
		Pending:   pending,
		Completed: int(atomic.LoadInt32(&q.completed)),
		Failed:    int(atomic.LoadInt32(&q.failed)),
		Status:    status,
	}
}

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

/**
 * Queue is a interface for the FastQueue
 **/
type Queue interface {
	Pending() int
	NumberOfWorkers() int
	Drained()

	Pause()
	Resume()
	Clear()
	Stop()

	Status() QueueStats
}

var _ Queue = &FastQueue[any, any]{}
