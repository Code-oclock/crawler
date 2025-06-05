package workerpool

import (
	"context"
	"sync"
)

// Accumulator is a function type used to aggregate values of type T into a result of type R.
// It must be thread-safe, as multiple goroutines will access the accumulator function concurrently.
// Each worker will produce intermediate results, which are combined with an initial or
// accumulated value.
type Accumulator[T, R any] func(current T, accum R) R

// Transformer is a function type used to transform an element of type T to another type R.
// The function is invoked concurrently by multiple workers, and therefore must be thread-safe
// to ensure data integrity when accessed across multiple goroutines.
// Each worker independently applies the transformer to its own subset of data, and although
// no shared state is expected, the transformer must handle any internal state in a thread-safe
// manner if present.
type Transformer[T, R any] func(current T) R

// Searcher is a function type for exploring data in a hierarchical manner.
// Each call to Searcher takes a parent element of type T and returns a slice of T representing
// its child elements. Since multiple goroutines may call Searcher concurrently, it must be
// thread-safe to ensure consistent results during recursive  exploration.
//
// Important considerations:
//  1. Searcher should be designed to avoid race conditions, particularly if it captures external
//     variables in closures.
//  2. The calling function must handle any state or values in closures, ensuring that
//     captured variables remain consistent throughout recursive or hierarchical search paths.
type Searcher[T any] func(parent T) []T

// Pool is the primary interface for managing worker pools, with support for three main
// operations: Transform, Accumulate, and List. Each operation takes an input channel, applies
// a transformation, accumulation, or list expansion, and returns the respective output.
type Pool[T, R any] interface {
	// Transform applies a transformer function to each item received from the input channel,
	// with results sent to the output channel. Transform operates concurrently, utilizing the
	// specified number of workers. The number of workers must be explicitly defined in the
	// configuration for this function to handle expected workloads effectively.
	// Since multiple workers may call the transformer function concurrently, it must be
	// thread-safe to prevent race conditions or unexpected results when handling shared or
	// internal state. Each worker independently applies the transformer function to its own
	// data subset.
	Transform(ctx context.Context, workers int, input <-chan T, transformer Transformer[T, R]) <-chan R

	// Accumulate applies an accumulator function to the items received from the input channel,
	// with results accumulated and sent to the output channel. The accumulator function must
	// be thread-safe, as multiple workers concurrently update the accumulated result.
	// The output channel will contain intermediate accumulated results as R
	Accumulate(ctx context.Context, workers int, input <-chan T, accumulator Accumulator[T, R]) <-chan R

	// List expands elements based on a searcher function, starting
	// from the given element. The searcher function finds child elements for each parent,
	// allowing exploration in a tree-like structure.
	// The number of workers should be configured based on the workload, ensuring each worker
	// independently processes assigned elements.
	List(ctx context.Context, workers int, start T, searcher Searcher[T])
}

type poolImpl[T, R any] struct{}

func New[T, R any]() *poolImpl[T, R] {
	return &poolImpl[T, R]{}
}

// Accumulate applies the accumulator function to combine values from the input channel.
// The result is sent to the output channel, with multiple workers processing concurrently.
func (p *poolImpl[T, R]) Accumulate(
	ctx context.Context,
	workers int,
	input <-chan T,
	accumulator Accumulator[T, R],
) <-chan R {
	result := make(chan R)

	var wg sync.WaitGroup

	// Start worker goroutines.
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			var accumulate R
			defer func() {
				defer wg.Done()
				select {
				case <-ctx.Done(): // Handle context cancellation.
					return
				case result <- accumulate:
				}
			}()
			for {
				select {
				case <-ctx.Done(): // Handle context cancellation.
					return
				case t, ok := <-input: // Read data from the input channel.
					if !ok {
						return
					}

					select {
					case <-ctx.Done(): // Handle context cancellation.
						return
					default:
						accumulate = accumulator(t, accumulate) // Update accumulated result.
					}
				}
			}
		}()
	}

	go func() {
		defer close(result) // close channel
		wg.Wait()           // Ensure all workers complete before closing the channel.
	}()

	return result
}

// createWorkers spawns multiple workers to process elements using the searcher function,
// populating the next level of elements for tree traversal.
func createWorkers[T any](
	ctx context.Context,
	searcher Searcher[T],
	currentLevel []T,
	workers int,
	nextLevel *[]T,
) {
	var (
		mu sync.Mutex     // Mutex to safely update shared slice.
		wg sync.WaitGroup // Channel for distributing work to workers.
	)
	queue := make(chan T) // Channel for distributing work to workers.

	// Start worker goroutines.
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for parent := range queue {
				select {
				case <-ctx.Done(): // Handle context cancellation.
					return
				default:
					children := searcher(parent) // Get child elements.
					mu.Lock()
					*nextLevel = append(*nextLevel, children...) // Safely update next level.
					mu.Unlock()
				}
			}
		}()
	}
	for _, parent := range currentLevel {
		select {
		case <-ctx.Done(): // Handle context cancellation.
			close(queue) // close channel
			wg.Wait()
			return
		case queue <- parent:
		}
	}
	close(queue) // Close the queue when done.
	wg.Wait()    // Wait for all workers to finish.
}

// List performs hierarchical exploration using the searcher function,
// starting from a root element and iterating through levels.
func (p *poolImpl[T, R]) List(
	ctx context.Context,
	workers int,
	start T,
	searcher Searcher[T],
) {
	currentLevel := []T{start}
	for {
		var nextLevel []T
		createWorkers(ctx, searcher, currentLevel, workers, &nextLevel) // Process current level.
		currentLevel = nextLevel
		if len(currentLevel) == 0 { // Stop if no more elements.
			break
		}
	}
}

// Transform applies the transformer function to elements from the input channel,
// sending transformed results to the output channel. Multiple workers run concurrently.
func (p *poolImpl[T, R]) Transform(
	ctx context.Context,
	workers int,
	input <-chan T,
	transformer Transformer[T, R],
) <-chan R {
	result := make(chan R)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done(): // Handle context cancellation.
					return
				case t, ok := <-input: // Read data from the input channel.
					if !ok {
						return
					}

					select {
					case <-ctx.Done(): // Handle context cancellation.
						return
					case result <- transformer(t): // Apply transformation and send result.
					}
				}
			}
		}()
	}

	go func() {
		defer close(result) // close channel
		wg.Wait()           // Wait for all workers to finish before closing the channel.
	}()

	return result
}
