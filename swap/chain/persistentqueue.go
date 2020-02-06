package chain

import (
	"context"
	"encoding"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethersphere/swarm/state"
)

// PersistentQueue represents a queue stored in a state store
type PersistentQueue struct {
	lock    sync.Mutex
	store   state.Store
	prefix  string
	trigger chan struct{}
	nonce   uint64
}

// NewPersistentQueue creates a structure to interact with a queue with the given prefix
func NewPersistentQueue(store state.Store, prefix string) *PersistentQueue {
	return &PersistentQueue{
		store:   store,
		prefix:  prefix,
		trigger: make(chan struct{}, 1),
		nonce:   0,
	}
}

// Queue puts the necessary database operations for the queueing into the supplied batch
// it returns the generated key and a trigger function which must be called if the batch was successfully written
// this only returns an error if the encoding fails which is an unrecoverable error
func (pq *PersistentQueue) Queue(b *state.StoreBatch, v interface{}) (key string, trigger func(), err error) {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	pq.nonce++
	key = time.Now().String() + "_" + strconv.FormatUint(pq.nonce, 10)
	if err = b.Put(pq.prefix+key, v); err != nil {
		return "", nil, err
	}

	return key, func() {
		select {
		case pq.trigger <- struct{}{}:
		default:
		}
	}, nil
}

// Peek looks at the next item in the queue
// the error returned is either an decode or an io error
func (pq *PersistentQueue) Peek(i interface{}) (key string, exists bool, err error) {
	err = pq.store.Iterate(pq.prefix, func(k, data []byte) (bool, error) {
		key = string(k)
		unmarshaler, ok := i.(encoding.BinaryUnmarshaler)
		if !ok {
			return true, json.Unmarshal(data, i)
		}
		return true, unmarshaler.UnmarshalBinary(data)
	})
	if err != nil {
		return "", false, err
	}
	if key == "" {
		return "", false, nil
	}
	return strings.TrimPrefix(key, pq.prefix), true, nil
}

// Next looks at the next item in the queue and blocks until an item is available if there is none
// the error returned is either an decode error, an io error or a cancelled context
func (pq *PersistentQueue) Next(ctx context.Context, i interface{}) (key string, err error) {
	key, exists, err := pq.Peek(i)
	if err != nil {
		return "", err
	}
	if exists {
		return key, nil
	}

	for {
		select {
		case <-pq.trigger:
			key, exists, err = pq.Peek(i)
			if err != nil {
				return "", err
			}
			if exists {
				return key, nil
			}
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

// Delete adds the batch operation to delete the queue element with the given key
func (pq *PersistentQueue) Delete(b *state.StoreBatch, key string) {
	b.Delete(pq.prefix + key)
}
