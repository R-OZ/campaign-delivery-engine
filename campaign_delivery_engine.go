// Campaign Delivery: a small, retry-aware notification dispatcher.
// Run: go run campaign_delivery_engine.go
// Only the standard library is used.
package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Sender must honor the key across retries to prevent duplicate delivery.
// Without provider-side idempotency, an ambiguous timeout cannot be retried safely.
type Sender interface {
	Send(ctx context.Context, key, recipient, body string) error
}

type Message struct {
	ID, Recipient, Body string
}

type task struct {
	message Message
	attempt int
	due     time.Time
}

type result struct {
	task task
	err  error
}

type Report struct {
	Sent, Failed int
	Attempts     int
}

// Dispatch sends messages with a fixed worker limit and a shared rate limit.
// Retries are scheduled centrally, so workers never sleep through backoff.
func Dispatch(ctx context.Context, sender Sender, messages []Message, workers int, gap time.Duration, maxAttempts int) (Report, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	if workers < 1 || gap <= 0 || maxAttempts < 1 {
		return Report{}, errors.New("workers, gap and maxAttempts must be positive")
	}

	// Stable IDs are required because they become provider idempotency keys.
	seen := make(map[string]bool, len(messages))
	ready := make([]task, 0, len(messages))
	for _, m := range messages {
		if m.ID == "" || seen[m.ID] {
			return Report{}, fmt.Errorf("missing or duplicate message ID: %q", m.ID)
		}
		seen[m.ID] = true
		ready = append(ready, task{message: m})
	}

	jobs := make(chan task)
	results := make(chan result, workers)
	limiter := time.NewTicker(gap) // one shared rate limit, not one per worker
	defer limiter.Stop()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				select {
				case <-ctx.Done():
					return
				case <-limiter.C:
				}
				err := sender.Send(ctx, job.message.ID, job.message.Recipient, job.message.Body)
				select {
				case results <- result{task: job, err: err}:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	defer func() { close(jobs); wg.Wait() }()

	var report Report
	var delayed []task
	remaining := len(messages)

	for remaining > 0 {
		// Move retries whose backoff has elapsed into the ready queue.
		now := time.Now()
		for i := 0; i < len(delayed); {
			if !delayed[i].due.After(now) {
				ready = append(ready, delayed[i])
				delayed = append(delayed[:i], delayed[i+1:]...)
			} else {
				i++
			}
		}

		// A timer is needed only when all ready work has been dispatched.
		var wake <-chan time.Time
		if len(delayed) > 0 {
			sort.Slice(delayed, func(i, j int) bool { return delayed[i].due.Before(delayed[j].due) })
			wake = time.After(time.Until(delayed[0].due))
		}

		var send chan task
		var next task
		if len(ready) > 0 {
			send, next = jobs, ready[0]
		}

		select {
		case <-ctx.Done():
			return report, ctx.Err()
		case send <- next:
			ready = ready[1:]
		case outcome := <-results:
			// Account for a completed attempt before honoring cancellation.
			// Otherwise a successful send could be missing from the report.
			report.Attempts++
			if outcome.err == nil {
				report.Sent++
				remaining--
			} else if outcome.task.attempt+1 >= maxAttempts {
				report.Failed++
				remaining--
			} else {
				// Exponential backoff: 100ms, 200ms, 400ms, ...
				// Cap it to keep shifts predictable.
				n := outcome.task.attempt
				if n > 5 {
					n = 5
				}
				outcome.task.attempt++
				outcome.task.due = time.Now().Add(100 * time.Millisecond * time.Duration(1<<n))
				delayed = append(delayed, outcome.task)
			}
			if err := ctx.Err(); err != nil {
				return report, err
			}
		case <-wake:
		}
	}
	return report, nil
}

// DemoSender models two different failures: a definite rejection and a lost
// acknowledgement after acceptance. The latter must not produce duplicates.
type DemoSender struct {
	mu         sync.Mutex
	attempts   map[string]int
	deliveries map[string]int
}

func (s *DemoSender) Send(ctx context.Context, key, recipient, body string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts[key]++
	if s.deliveries[key] > 0 {
		fmt.Printf("Deduplicated %-7s on retry\n", key)
		return nil // provider recognizes a previously accepted idempotency key
	}
	if key == "order-4" && s.attempts[key] == 1 {
		return errors.New("provider temporarily unavailable")
	}
	s.deliveries[key]++
	fmt.Printf("Delivered %-7s to %s\n", key, recipient)
	if key == "order-2" && s.attempts[key] == 1 {
		return errors.New("acknowledgement lost after acceptance")
	}
	return nil
}

func main() {
	messages := []Message{
		{ID: "order-1", Recipient: "alex@example.com", Body: "Your order shipped!"},
		{ID: "order-2", Recipient: "sam@example.com", Body: "Your order shipped!"},
		{ID: "order-3", Recipient: "lee@example.com", Body: "Your order shipped!"},
		{ID: "order-4", Recipient: "kim@example.com", Body: "Your order shipped!"},
	}
	provider := &DemoSender{attempts: map[string]int{}, deliveries: map[string]int{}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	report, err := Dispatch(ctx, provider, messages, 2, 30*time.Millisecond, 3)
	fmt.Printf("\nReport: %+v, error: %v\n", report, err)
	fmt.Printf("Actual deliveries: %v (order-2 deliveries: %d)\n", provider.deliveries, provider.deliveries["order-2"])
}
