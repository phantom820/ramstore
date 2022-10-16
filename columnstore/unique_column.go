package columnstore

import (
	"os"
	"sync"
	"time"

	"github.com/phantom820/collections/sets/hashset"
	"github.com/phantom820/collections/types"
	"github.com/phantom820/streams"
)

// UniqueColumn a single column in which each row is unique i.e no duplicate rows according to Equals.
type UniqueColumn[T types.Hashable[T]] struct {
	sync.RWMutex
	name           string
	maxConcurrency int
	data           *hashset.HashSet[T]
	writer         *fileWriter[T]
}

// NewUniqueColumn creates a new UniqueColumn with the given name.
func NewUniqueColumn[T types.Hashable[T]](name string, maxConcurrency int) *UniqueColumn[T] {
	return &UniqueColumn[T]{name: name, maxConcurrency: maxConcurrency, data: hashset.New[T]()}
}

// All returns all rows in the column.
func (c *Column[T]) All() []T {
	c.RLock()
	defer c.RUnlock()
	return c.data.Collect()
}

// Insert inserts a row into the column.
func (c *UniqueColumn[T]) Insert(x T) bool {
	c.Lock()
	defer c.Unlock()
	return c.data.Add(x)
}

// Persist periodically dumps the rows of the column to the specified outputFile.
func (c *UniqueColumn[T]) Persist(outputFilename string, period time.Duration) error {
	file, err := os.Create(outputFilename)
	if err != nil {
		return err
	}
	writer := fileWriter[T]{period: period, stopped: false, shutdownChannel: make(chan string)}
	c.writer = &writer
	go c.writer.writeToFile(&c.RWMutex, file, c.name, c.data.Collect)
	return nil
}

// Close terminates background persistence.
func (c *UniqueColumn[T]) StopPersistence() {
	if c.writer == nil {
		return
	}
	c.writer.shutdown()
	c.writer = nil
	return
}

// Delete removes the first occurence of a row with the given value from the column.
func (c *UniqueColumn[T]) Delete(x T) bool {
	c.Lock()
	defer c.Unlock()
	return c.data.Remove(x)
}

// DeleteWhere removes all rows that satisfy the given predicate.
func (c *UniqueColumn[T]) DeleteWhere(f func(x T) bool) bool {
	c.Lock()
	defer c.Unlock()
	newData := streams.FromSlice(c.data.Collect, computeConcurrency(c.data.Len(), c.maxConcurrency)).Filter(func(x T) bool {
		return !f(x)
	}).Collect()
	c.data = hashset.New(newData...)
	return true
}

// FindWhere returns all rows that satisfy the given predicate.
func (c *UniqueColumn[T]) FindWhere(f func(x T) bool) []T {
	c.RLock()
	defer c.RUnlock()
	return streams.FromSlice(c.data.Collect, computeConcurrency(c.data.Len(), c.maxConcurrency)).Filter(f).Collect()
}

// Len returns the number of rows in the column.
func (c *UniqueColumn[T]) Len() int {
	c.RLock()
	defer c.RUnlock()
	return c.data.Len()
}

// Stream returns a stream of the rows in the column.
func (c *UniqueColumn[T]) Stream() streams.Stream[T] {
	return streams.FromSlice(func() []T {
		c.RLock()
		defer c.RUnlock()
		return c.data.Collect()
	}, computeConcurrency(c.data.Len(), c.maxConcurrency))
}

// UpdateWhere updates all rows that satisfy the predicate.
func (c *UniqueColumn[T]) UpdateWhere(f func(x T) bool, g func(y T) T) bool {
	c.Lock()
	defer c.Unlock()
	newData := streams.FromSlice(c.data.Collect, computeConcurrency(c.data.Len(), c.maxConcurrency)).Map(func(x T) T {
		if f(x) {
			return g(x)
		}
		return x
	}).Collect()
	c.data = hashset.New(newData...)
	return true
}
