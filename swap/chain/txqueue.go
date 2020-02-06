package chain

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethersphere/swarm/state"
)

// TxQueue is a TxScheduler which sends transactions in sequence
// A new transaction is only sent after the previous one confirmed
type TxQueue struct {
	lock    sync.Mutex         // lock for the entire queue
	ctx     context.Context    // context used for all network requests to ensure the queue can be stopped at any point
	cancel  context.CancelFunc // function to cancel the above context
	wg      sync.WaitGroup     // used to ensure that all background go routines have finished before Stop returns
	running bool               // bool indicating that the queue is running. used to ensure it does not run multiple times simultaneously

	store              state.Store                                              // state store to use as the backend
	prefix             string                                                   // all keys in the state store are prefixed with this
	requestQueue       *PersistentQueue                                         // queue for all future requests
	handlers           map[TransactionRequestTypeID]*TransactionRequestHandlers // map from request type ids to their registered handlers
	notificationQueues map[TransactionRequestTypeID]*PersistentQueue            // map from request type ids to the notification queue of that handler

	backend    Backend           // ethereum backend to use
	privateKey *ecdsa.PrivateKey // private key used to sign transactions
}

// TransactionRequestMetadata is the metadata the queue saves for every request
type TransactionRequestMetadata struct {
	RequestTypeID TransactionRequestTypeID // the type id of this request
	State         TransactionRequestState  // the state this request is in
	Hash          common.Hash              // the hash of the associated transaction (if already sent)
}

// NotificationQueueItem is the metadata the queue saves for every pending notification
type NotificationQueueItem struct {
	NotificationType string // the type of the notification
	RequestID        uint64 // the request this notification is for
}

// NewTxQueue creates a new TxQueue
func NewTxQueue(store state.Store, prefix string, backend Backend, privateKey *ecdsa.PrivateKey) *TxQueue {
	txq := &TxQueue{
		store:              store,
		prefix:             prefix,
		handlers:           make(map[TransactionRequestTypeID]*TransactionRequestHandlers),
		notificationQueues: make(map[TransactionRequestTypeID]*PersistentQueue),
		backend:            backend,
		privateKey:         privateKey,
		requestQueue:       NewPersistentQueue(store, prefix+"_requestQueue_"),
	}
	txq.ctx, txq.cancel = context.WithCancel(context.Background())
	return txq
}

// requestKey returns the database key for the TransactionRequestMetadata data
func (txq *TxQueue) requestKey(id uint64) string {
	return txq.prefix + "_requests_" + strconv.FormatUint(id, 10)
}

// requestDataKey returns the database key for the custom TransactionRequest
func (txq *TxQueue) requestDataKey(id uint64) string {
	return txq.requestKey(id) + "_data"
}

// activeRequestKey returns the database key used for the currently active request
func (txq *TxQueue) activeRequestKey() string {
	return txq.prefix + "_active"
}

// requestDataKey returns the database key for the custom TransactionRequest
func (txq *TxQueue) notificationKey(key string) string {
	return txq.prefix + "_notification_" + key
}

// SetHandlers registers the handlers for the given handler
// This starts the delivery of notifications for this handler
func (txq *TxQueue) SetHandlers(requestTypeID TransactionRequestTypeID, handlers *TransactionRequestHandlers) {
	txq.lock.Lock()
	defer txq.lock.Unlock()

	if txq.handlers[requestTypeID] != nil {
		log.Error("handlers for %v.%v already set", requestTypeID.Handler, requestTypeID.RequestType)
		return
	}
	txq.handlers[requestTypeID] = handlers

	// go routine processing the notification queue for this handler
	txq.wg.Add(1)
	go func() {
		defer txq.wg.Done()
		notifyQ := txq.getNotificationQueue(requestTypeID)

		for {
			var item NotificationQueueItem
			// get the next notification item
			key, err := notifyQ.Next(txq.ctx, &item)
			if err != nil {
				return
			}

			// load and decode the notification
			var notification interface{}
			switch item.NotificationType {
			case "TransactionReceiptNotification":
				notification = &TransactionReceiptNotification{}
			case "TransactionStateChangedNotification":
				notification = &TransactionStateChangedNotification{}
			}

			err = txq.store.Get(txq.notificationKey(key), notification)
			if err != nil {
				return
			}

			switch item.NotificationType {
			case "TransactionReceiptNotification":
				if handlers.NotifyReceipt != nil {
					err = handlers.NotifyReceipt(item.RequestID, notification.(*TransactionReceiptNotification))
				}
			case "TransactionStateChangedNotification":
				if handlers.NotifyStateChanged != nil {
					err = handlers.NotifyStateChanged(item.RequestID, notification.(*TransactionStateChangedNotification))
				}
			}
			if err != nil {
				select {
				case <-txq.ctx.Done():
					return
				case <-time.After(10 * time.Second):
					continue
				}
			}

			// once the notification was handled delete it from the queue
			batch := new(state.StoreBatch)
			notifyQ.Delete(batch, key)
			err = txq.store.WriteBatch(batch)
			if err != nil {
				return
			}
		}
	}()
}

// ScheduleRequest adds a new request to be processed
// The request is assigned an id which is returned
func (txq *TxQueue) ScheduleRequest(requestTypeID TransactionRequestTypeID, request interface{}) (id uint64, err error) {
	txq.lock.Lock()
	defer txq.lock.Unlock()

	// get the last id
	idKey := txq.prefix + "_request_id"
	err = txq.store.Get(idKey, &id)
	if err != nil && err != state.ErrNotFound {
		return 0, err
	}
	// ids start at 1
	id++

	// in a single batch we
	// * store the request data
	// * store the request metadata
	// * add it to the queue
	batch := new(state.StoreBatch)
	batch.Put(idKey, id)
	if err := batch.Put(txq.requestDataKey(id), request); err != nil {
		return 0, err
	}

	err = batch.Put(txq.requestKey(id), &TransactionRequestMetadata{
		RequestTypeID: requestTypeID,
		State:         TransactionQueued,
	})
	if err != nil {
		return 0, err
	}

	_, triggerQueue, err := txq.requestQueue.Queue(batch, id)
	if err != nil {
		return 0, err
	}

	// persist to disk
	err = txq.store.WriteBatch(batch)
	if err != nil {
		return 0, err
	}

	triggerQueue()

	return id, nil
}

// GetRequest load the serialized transaction request from disk and tries to decode it
func (txq *TxQueue) GetRequest(id uint64, request interface{}) error {
	return txq.store.Get(txq.requestDataKey(id), &request)
}

// Start starts processing transactions if it is not already doing so
func (txq *TxQueue) Start() {
	txq.lock.Lock()
	defer txq.lock.Unlock()

	if txq.running {
		return
	}

	txq.running = true
	txq.wg.Add(1)
	go func() {
		err := txq.loop()
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("transaction queue terminated with an error", "error", err)
		}
		txq.wg.Done()
	}()
}

// Stop stops processing transactions if it is running
// It will block until processing has terminated
func (txq *TxQueue) Stop() {
	txq.lock.Lock()

	if !txq.running {
		return
	}

	txq.running = false
	txq.cancel()

	txq.lock.Unlock()
	// wait until all routines have finished
	txq.wg.Wait()
}

// getNotificationQueue gets the notification queue for a handler
// it initializes the struct if it does not yet exist
// the TxQueue lock must not be held
func (txq *TxQueue) getNotificationQueue(requestTypeID TransactionRequestTypeID) *PersistentQueue {
	txq.lock.Lock()
	defer txq.lock.Unlock()
	queue, ok := txq.notificationQueues[requestTypeID]
	if !ok {
		queue = NewPersistentQueue(txq.store, txq.prefix+"_notify_"+requestTypeID.Handler+"_"+requestTypeID.RequestType)
		txq.notificationQueues[requestTypeID] = queue
	}
	return queue
}

// updateRequestStatus is a helper function to change the state of a transaction request while also emitting a notification
// in one write batch it
// * adds a TransactionStateChangedNotification notification to the notification queue
// * stores the corresponding notification
// * updates the state of requestMetadata and persists it
// it returns the trigger function which must be called once the batch was written
// this only returns an error if the encoding fails which is an unrecoverable error
func (txq *TxQueue) updateRequestStatus(batch *state.StoreBatch, id uint64, requestMetadata *TransactionRequestMetadata, newState TransactionRequestState) (finish func(), err error) {
	nQueue := txq.getNotificationQueue(requestMetadata.RequestTypeID)
	key, triggerQueue, err := nQueue.Queue(batch, &NotificationQueueItem{
		RequestID:        id,
		NotificationType: "TransactionStateChangedNotification",
	})
	if err != nil {
		return nil, fmt.Errorf("could not serialize notification queue item (this is a bug): %v", err)
	}

	err = batch.Put(txq.notificationKey(key), &TransactionStateChangedNotification{
		OldState: requestMetadata.State,
		NewState: newState,
	})
	if err != nil {
		return nil, fmt.Errorf("could not serialize notification (this is a bug): %v", err)
	}
	requestMetadata.State = newState
	batch.Put(txq.requestKey(id), requestMetadata)

	return triggerQueue, nil
}

// loop is the main transaction processing function of the TxQueue
// first it checks if there already is an active request. If so it processes this first
// then it will take request from the queue in a loop and execute those
func (txq *TxQueue) loop() error {
	// get the stored active request key
	// if nothing is stored id will remain 0 (which is invalid as ids start with 1)
	var id uint64
	err := txq.store.Get(txq.activeRequestKey(), &id)
	if err != state.ErrNotFound {
		return err
	}

	// if there is a non-zero id there is an active request
	if id != 0 {
		log.Info("Continuing to monitor previous transaction")
		// load the request metadata
		var requestMetadata TransactionRequestMetadata
		err = txq.store.Get(txq.requestKey(id), &requestMetadata)
		if err != nil {
			return err
		}

		switch requestMetadata.State {
		// if the transaction is still in the Queued state we cannot know for sure where the process terminated
		// with a very high likelihood the transaction was not yet sent, but we cannot be sure of that
		// the transaction is marked as TransactionStatusUnknown and removed as the active transaction
		// in that rare case nonce issue might arise in subsequent requests
		case TransactionQueued:
			batch := new(state.StoreBatch)
			finish, err := txq.updateRequestStatus(batch, id, &requestMetadata, TransactionStatusUnknown)
			// this only returns an error if the encoding fails which is an unrecoverable error
			if err != nil {
				return err
			}
			batch.Delete(txq.activeRequestKey())
			if err := txq.store.WriteBatch(batch); err != nil {
				return err
			}
			finish()
		// if the transaction is in the pending state this means we were previously waiting for the transaction
		case TransactionPending:
			// this only returns an error if the encoding fails which is an unrecoverable error
			if err := txq.waitForActiveTransaction(id, &requestMetadata); err != nil {
				return err
			}
		default:
			// this indicates a client bug
			log.Error("found active transaction in unexpected state")
			if err := txq.store.Delete(txq.activeRequestKey()); err != nil {
				return err
			}
		}
	}
l:
	for txq.running {
		// terminate the loop if the context was cancelled
		select {
		case <-txq.ctx.Done():
			break l
		default:
		}

		// get the id of the next request in the queue
		var id uint64
		key, err := txq.requestQueue.Next(txq.ctx, &id)
		if err != nil {
			return err
		}

		// load the request metadata
		var requestMetadata TransactionRequestMetadata
		err = txq.store.Get(txq.requestKey(id), &requestMetadata)
		if err != nil {
			return err
		}

		txq.lock.Lock()
		handlers := txq.handlers[requestMetadata.RequestTypeID]
		txq.lock.Unlock()
		if handlers == nil {
			// if there is no handler for this handler available we mark the request as cancelled and remove it from the queue
			batch := new(state.StoreBatch)
			finish, err := txq.updateRequestStatus(batch, id, &requestMetadata, TransactionCancelled)
			// this only returns an error if the encoding fails which is an unrecoverable error
			if err != nil {
				return err
			}

			txq.requestQueue.Delete(batch, key)
			err = txq.store.WriteBatch(batch)
			if err != nil {
				return err
			}

			finish()
			continue l
		}

		// if the request was successfully decoded it is removed from the queue and set as the active request
		batch := new(state.StoreBatch)
		err = batch.Put(txq.activeRequestKey(), id)
		// this only returns an error if the encoding fails which is an unrecoverable error
		if err != nil {
			return fmt.Errorf("could not put id write into batch (this is a bug): %v", err)
		}

		txq.requestQueue.Delete(batch, key)
		if err = txq.store.WriteBatch(batch); err != nil {
			return err
		}

		// finally we call the handler to send the actual transaction
		opts := bind.NewKeyedTransactor(txq.privateKey)
		hash, err := handlers.Send(id, txq.backend, opts)
		if err != nil {
			// even if SendTransactionRequest returns an error there are still certain rare edge cases where the transaction might still be sent so we mark it as status unknown
			batch := new(state.StoreBatch)
			finish, err := txq.updateRequestStatus(batch, id, &requestMetadata, TransactionStatusUnknown)
			if err != nil {
				// this only returns an error if the encoding fails which is an unrecoverable error
				return fmt.Errorf("failed to write transaction request status to store: %v", err)
			}

			if err = txq.store.WriteBatch(batch); err != nil {
				return err
			}
			finish()
			continue l
		}

		// if we have a hash we mark the transaction as pending
		batch = new(state.StoreBatch)
		requestMetadata.Hash = hash
		finish, err := txq.updateRequestStatus(batch, id, &requestMetadata, TransactionPending)
		if err != nil {
			// this only returns an error if the encoding fails which is an unrecoverable error
			return fmt.Errorf("failed to write transaction request status to store: %v", err)
		}

		if err = txq.store.WriteBatch(batch); err != nil {
			return err
		}

		finish()

		err = txq.waitForActiveTransaction(id, &requestMetadata)
		if err != nil {
			// this only returns an error if the encoding fails which is an unrecoverable error
			return fmt.Errorf("error while waiting for transaction: %v", err)
		}
	}
	return nil
}

// waitForActiveTransaction waits for requestMetadata to be mined and resets the active transaction  afterwards
// the transaction will also be considered mine once the notification was queued successfully
// this only returns an error if the encoding fails which is an unrecoverable error
func (txq *TxQueue) waitForActiveTransaction(id uint64, requestMetadata *TransactionRequestMetadata) error {
	ctx, cancel := context.WithTimeout(txq.ctx, 20*time.Minute)
	defer cancel()

	// an error here means the context was cancelled
	receipt, err := WaitMined(ctx, txq.backend, requestMetadata.Hash)
	if err != nil {
		// if the main context of the TxQueue was cancelled we log and return
		if txq.ctx.Err() != nil {
			log.Warn("terminating transaction queue while waiting for a transaction")
			return nil
		}

		// if the timeout context expired we mark the transaction status as unknown
		// future versions of the queue (with local nonce-tracking) should keep note of that and reuse the nonce for the next request
		log.Error("transaction timeout reached")
		batch := new(state.StoreBatch)
		finish, err := txq.updateRequestStatus(batch, id, requestMetadata, TransactionStatusUnknown)
		if err != nil {
			return err
		}

		if err = txq.store.WriteBatch(batch); err != nil {
			return err
		}

		finish()
		return nil
	}

	// if the transaction is mined we need to
	// * update the request state and emit the corresponding notification
	// * emit a TransactionReceiptNotification
	// * reset the active request
	notifyQueue := txq.getNotificationQueue(requestMetadata.RequestTypeID)
	batch := new(state.StoreBatch)
	triggerRequestQueue, err := txq.updateRequestStatus(batch, id, requestMetadata, TransactionConfirmed)
	if err != nil {
		// unrecoverable decode error
		return err
	}

	key, triggerNotifyQueue, err := notifyQueue.Queue(batch, &NotificationQueueItem{
		RequestID:        id,
		NotificationType: "TransactionReceiptNotification",
	})
	if err != nil {
		// unrecoverable decode error
		return err
	}

	batch.Put(txq.notificationKey(key), &TransactionReceiptNotification{
		Receipt: *receipt,
	})

	err = batch.Put(txq.activeRequestKey(), nil)
	if err != nil {
		return err
	}

	if err = txq.store.WriteBatch(batch); err != nil {
		return err
	}

	triggerRequestQueue()
	triggerNotifyQueue()
	return nil
}
