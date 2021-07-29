package gdrive

import (
	"sync"

	. "github.com/dgraph-io/ristretto"
)

var (
	cache *Cache
	once  sync.Once
)

const (
	numCounters = 1e7     // number of keys to track frequency of (10M).
	maxCost     = 1 << 30 // maximum cost of cache (1GB).
	bufferItems = 64      // number of keys per Get buffer.
	cost        = 1
	expireTime  = 100
)

func makeCache() (err error) {
	once.Do(func() {
		config := &Config{
			NumCounters: numCounters,
			MaxCost:     maxCost,
			BufferItems: bufferItems,
		}

		cache, err = NewCache(config)

		if err != nil {
			return
		}
	})
	return nil
}
