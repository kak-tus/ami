// Package retrier for retry any operations by timer.
package retrier

import (
	"math/rand"
	"sync"
	"time"
)

var defaultRetryPolicy = []time.Duration{time.Second * 5}

// Retrier type represents a retrier instance.
type Retrier struct {
	config   Config
	idSeq    int
	attempts map[int]int
	stop     chan struct{}
	stopped  bool
	m        *sync.Mutex
}

// Config reperesents configuration parameters of retrier.
type Config struct {
	MaxAttempts int
	RetryPolicy []time.Duration
}

func init() {
	now := time.Now()
	rand.Seed(now.Unix())
}

// New method creates new retrier.
func New(config Config) *Retrier {
	if config.RetryPolicy == nil {
		config.RetryPolicy = defaultRetryPolicy
	}

	return &Retrier{
		config:   config,
		idSeq:    1,
		attempts: make(map[int]int),
		stop:     make(chan struct{}),
		m:        &sync.Mutex{},
	}
}

// Do function executes an operation and retry it if operation was failed.
func (r *Retrier) Do(f func() *Error) error {
	r.m.Lock()
	id := r.idSeq
	r.idSeq++
	r.m.Unlock()

	for {
		err := f()

		r.m.Lock()
		r.attempts[id]++
		attempts := r.attempts[id]
		r.m.Unlock()

		if err == nil {
			r.m.Lock()
			delete(r.attempts, id)
			r.m.Unlock()

			return nil
		}

		if err.IsFatal() ||
			(r.config.MaxAttempts > 0 &&
				attempts >= r.config.MaxAttempts) ||
			r.stopped {

			r.m.Lock()
			delete(r.attempts, id)
			r.m.Unlock()

			return err.err
		}

		interval := r.getNextInterval(attempts)
		t := time.NewTimer(interval)

		select {
		case <-r.stop:
			t.Stop()
			return err
		case <-t.C:
		}
	}
}

// Stop method stops all retries.
func (r *Retrier) Stop() {
	r.m.Lock()
	defer r.m.Unlock()

	if r.stopped {
		return
	}

	close(r.stop)
	r.stopped = true
}

func (r *Retrier) getNextInterval(attemptNum int) time.Duration {
	i := attemptNum - 1
	maxIndex := len(r.config.RetryPolicy) - 1

	if i > maxIndex {
		i = maxIndex
	}

	interval := int64(r.config.RetryPolicy[i])

	return time.Duration(interval/2 + rand.Int63n(interval))
}
