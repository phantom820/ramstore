// package columnstore provides a column centric in memory datastore for CRUD operations. Data is modelled as a single column with rows representing individual data points.
package columnstore

import (
	"os"
	"sync"
	"time"

	"github.com/phantom820/collections/lists/vector"
	"github.com/phantom820/collections/types"
	"github.com/phantom820/streams"
)

// Column a single column that allows duplicate rows according to Equals.
type Column[T types.Equitable[T]] struct {
	sync.RWMutex
	name           string
	maxConcurrency int
	data           *vector.Vector[T]
	writer         *fileWriter[T]
}

// NewColumn creates a new column with the given name.
func NewColumn[T types.Equitable[T]](name string, maxConcurrency int) *Column[T] {
	column := &Column[T]{name: name, maxConcurrency: maxConcurrency, data: vector.New[T]()}
	return column
}

// All returns all rows in the column.
func (c *UniqueColumn[T]) All() []T {
	c.RLock()
	defer c.RUnlock()
	return c.data.Collect()
}

// Persist periodically dumps the rows of the column to the specified outputFile.
func (c *Column[T]) Persist(outputFilename string, period time.Duration) error {
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
func (c *Column[T]) StopPersistence() {
	if c.writer == nil {
		return
	}
	c.writer.shutdown()
	c.writer = nil
	return
}

// Insert inserts a row into the column.
func (c *Column[T]) Insert(x T) bool {
	c.Lock()
	defer c.Unlock()
	return c.data.Add(x)
}

// Delete removes the first occurence of a row with the given value from the column.
func (c *Column[T]) Delete(x T) bool {
	c.Lock()
	defer c.Unlock()
	return c.data.Remove(x)
}

// DeleteWhere removes all rows that satisfy the given predicate.
func (c *Column[T]) DeleteWhere(f func(x T) bool) {
	c.Lock()
	defer c.Unlock()
	newData := streams.FromSlice(c.data.Collect, computeConcurrency(c.data.Len(), c.maxConcurrency)).Filter(func(x T) bool {
		return !f(x)
	}).Collect()
	c.data = vector.New(newData...)
}

// FindWhere returns all rows that satisfy the given predicate.
func (c *Column[T]) FindWhere(f func(x T) bool) []T {
	c.RLock()
	defer c.RUnlock()
	return streams.FromSlice(c.data.Collect, computeConcurrency(c.Len(), c.maxConcurrency)).Filter(f).Collect()
}

// Len returns the number of rows in the column.
func (c *Column[T]) Len() int {
	c.RLock()
	defer c.RUnlock()
	return c.data.Len()
}

// Stream returns a stream of the rows in the column.
func (c *Column[T]) Stream() streams.Stream[T] {
	return streams.FromSlice(func() []T {
		c.RLock()
		defer c.RUnlock()
		return c.data.Collect()
	}, computeConcurrency(c.Len(), c.maxConcurrency))
}

// UpdateWhere updates all rows that satisfy the predicate.
func (c *Column[T]) UpdateWhere(f func(x T) bool, g func(y T) T) {
	c.Lock()
	defer c.Unlock()
	newData := streams.FromSlice(c.data.Collect, computeConcurrency(c.data.Len(), c.maxConcurrency)).Map(func(x T) T {
		if f(x) {
			return g(x)
		}
		return x
	}).Collect()
	c.data = vector.New(newData...)
}
