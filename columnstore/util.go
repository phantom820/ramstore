package columnstore

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// computeConcurrency return the concurrency to be used for processing a column.
func computeConcurrency(columnSize int, maxConcurrency int) int {
	if columnSize < 200 {
		return 1
	} else if columnSize <= 1000 && maxConcurrency > 1 {
		return 2
	}
	return maxConcurrency
}

// fileWriter for writing data to file in the background.
type fileWriter[T any] struct {
	period          time.Duration
	stopped         bool
	shutdownChannel chan string
}

// writeToFile writes the data from the given callback to the given file. This should be called as a go routine so its done periodically in background.
func (f *fileWriter[T]) writeToFile(mux *sync.RWMutex, file *os.File, header string, data func() []T) {
	mux.RLock()
	defer mux.RUnlock()
	for {
		select {
		case <-f.shutdownChannel:
			f.shutdownChannel <- "Down"
			return
		case <-time.After(f.period):
			_ = file.Truncate(0)
			_, _ = file.Seek(0, 0)
			break
		}
		fmt.Fprintf(file, "%v, Timestamp\n", header)
		for _, val := range data() {
			fmt.Fprintf(file, "%v, %v\n", val, time.Now())
		}

	}
}

// shutdown shuts down the background file writing.
func (f *fileWriter[T]) shutdown() {
	f.stopped = true
	f.shutdownChannel <- "Down"
	<-f.shutdownChannel
	close(f.shutdownChannel)
}
