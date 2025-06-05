package crawler

import (
	"context"
	"crawler/internal/fs"
	"crawler/internal/workerpool"
	"encoding/json"
	"fmt"
	"log"
	"sync"
)

// Configuration holds the configuration for the crawler, specifying the number of workers for
// file searching, processing, and accumulating tasks. The values for SearchWorkers, FileWorkers,
// and AccumulatorWorkers are critical to efficient performance and must be defined in
// every configuration.
type Configuration struct {
	SearchWorkers      int // Number of workers responsible for searching files.
	FileWorkers        int // Number of workers for processing individual files.
	AccumulatorWorkers int // Number of workers for accumulating results.
}

// Combiner is a function type that defines how to combine two values of type R into a single
// result. Combiner is not required to be thread-safe
//
// Combiner can either:
//   - Modify one of its input arguments to include the result of the other and return it,
//     or
//   - Create a new combined result based on the inputs and return it.
//
// It is assumed that type R has a neutral element (forming a monoid)
type Combiner[R any] func(current R, accum R) R

// Crawler represents a concurrent crawler implementing a map-reduce model with multiple workers
// to manage file processing, transformation, and accumulation tasks. The crawler is designed to
// handle large sets of files efficiently, assuming that all files can fit into memory
// simultaneously.
type Crawler[T, R any] interface {
	// Collect performs the full crawling operation, coordinating with the file system
	// and worker pool to process files and accumulate results. The result type R is assumed
	// to be a monoid, meaning there exists a neutral element for combination, and that
	// R supports an associative combiner operation.
	// The result of this collection process, after all reductions, is returned as type R.
	//
	// Important requirements:
	// 1. Number of workers in the Configuration is mandatory for managing workload efficiently.
	// 2. FileSystem and Accumulator must be thread-safe.
	// 3. Combiner does not need to be thread-safe.
	// 4. If an accumulator or combiner function modifies one of its arguments,
	//    it should return that modified value rather than creating a new one,
	//    or alternatively, it can create and return a new combined result.
	// 5. Context cancellation is respected across workers.
	// 6. Type T is derived by json-deserializing the file contents, and any issues in deserialization
	//    must be handled within the worker.
	// 7. The combiner function will wait for all workers to complete, ensuring no goroutine leaks
	//    occur during the process.
	Collect(
		ctx context.Context,
		fileSystem fs.FileSystem,
		root string,
		conf Configuration,
		accumulator workerpool.Accumulator[T, R],
		combiner Combiner[R],
	) (R, error)
}

type crawlerImpl[T, R any] struct{}

func New[T, R any]() *crawlerImpl[T, R] {
	return &crawlerImpl[T, R]{}
}

func recoverWithError(errFiles *error) {
	// recoverWithError handles panic and stores it as an error in errFiles.
	if r := recover(); r != nil {
		if err, ok := r.(error); ok {
			*errFiles = err
		} else {
			*errFiles = fmt.Errorf("panic: %v", r)
		}
	}
}

func createSearcher(errFiles *error, fileSystem fs.FileSystem, ctx context.Context, files chan<- string) func(root string) []string {
	// createSearcher returns a function that recursively searches for files and directories.
	searcher := func(root string) []string {
		var children []string

		defer recoverWithError(errFiles) // Capture and handle panics.

		entries, err := fileSystem.ReadDir(root) // Read directory contents.
		if err != nil {
			*errFiles = err
			return nil
		}
		for _, entry := range entries {
			fullPath := fileSystem.Join(root, entry.Name())
			if entry.IsDir() {
				children = append(children, fullPath) // Add directories to be processed.
			} else {
				select {
				case <-ctx.Done(): // Handle context cancellation.
					return nil
				case files <- fullPath: // Send file paths to the channel.
				}
			}
		}
		return children
	}
	return searcher
}

func createOptimusPrime[T any](errFiles *error, fileSystem fs.FileSystem) func(path string) T {
	// createOptimusPrime returns a function that processes a file and decodes its content.
	optimusPrime := func(path string) T { // looks like a transformer
		defer recoverWithError(errFiles)   // Capture and handle panics.
		data, err := fileSystem.Open(path) // Open the file.
		if err != nil {
			*errFiles = err
			var z T
			return z
		}

		var result T
		decoder := json.NewDecoder(data) // Decode JSON content from the file.
		if err := decoder.Decode(&result); err != nil {
			*errFiles = err
			return result
		}

		defer func() T { // Ensure the file is closed.
			if err := data.Close(); err != nil {
				log.Println("Error closing file:", err)
			}
			return result
		}()
		return result
	}
	return optimusPrime
}

func (c *crawlerImpl[T, R]) Collect(
	ctx context.Context,
	fileSystem fs.FileSystem,
	root string,
	conf Configuration,
	accumulator workerpool.Accumulator[T, R],
	combiner Combiner[R],
) (R, error) {
	pool := workerpool.New[string, T]() // Initialize a worker pool for file processing.
	files := make(chan string)
	var errFiles error

	// Transform files into decoded results using OptimusPrime.
	transformed := pool.Transform(ctx, conf.FileWorkers, files, createOptimusPrime[T](&errFiles, fileSystem))
	newPool := workerpool.New[T, R]() // Initialize a worker pool for result accumulation.
	accumulated := newPool.Accumulate(ctx, conf.AccumulatorWorkers, transformed, accumulator)

	var finalResult R
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done(): // Handle context cancellation.
				return
			case accumulatedResult, ok := <-accumulated:
				if !ok {
					return
				}
				finalResult = combiner(finalResult, accumulatedResult) // Combine results.
			}
		}
	}()

	// Start the searcher to list files/directories.
	pool.List(ctx, conf.SearchWorkers, root, createSearcher(&errFiles, fileSystem, ctx, files))
	close(files) // Close the files channel after searching is complete.
	wg.Wait()    // Wait for all goroutines to finish.

	select {
	case <-ctx.Done(): // Return result or context error if cancelled.
		return finalResult, ctx.Err()
	default:
		return finalResult, errFiles // Return the final result and any errors encountered.
	}
}
