package chain

import (
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// TxScheduler represents a central sender for all transactions from a single ethereum account
// its purpose is to ensure there are no nonce issues and that transaction initiators are notified of the result
// notifications are guaranteed to happen even across restarts and disconnects from the ethereum backend
type TxScheduler interface {
	// SetHandlers registers the handlers for the given requestTypeID
	// This starts the delivery of notifications for this requestTypeID
	SetHandlers(requestTypeID TransactionRequestTypeID, handlers *TransactionRequestHandlers)
	// ScheduleRequest adds a new request to be processed
	// The request is assigned an id which is returned
	ScheduleRequest(requestTypeID TransactionRequestTypeID, request interface{}) (id uint64, err error)
	// GetRequest load the serialized transaction request from disk and tries to decode it
	GetRequest(id uint64, request interface{}) error
	// Start starts processing transactions if it is not already doing so
	Start()
	// Stop stops processing transactions if it is running
	// It will block until processing has terminated
	Stop()
}

// TransactionRequestTypeID is a combination of a handler and a request type
// All requests with a given TransactionRequestTypeID are handled the same
type TransactionRequestTypeID struct {
	Handler     string
	RequestType string
}

// TransactionRequestHandlers holds all the callbacks for a given TransactionRequestTypeID
// Any of the functions may be nil
type TransactionRequestHandlers struct {
	// Send should send the transaction using the backend and opts provided
	// opts may be modified, however From, Nonce and Signer must be left untouched
	// if the transaction is sent through other means opts.Nonce must be respected (if set to nil, the "pending" nonce must be used)
	Send func(id uint64, backend Backend, opts *bind.TransactOpts) (common.Hash, error)
	// Notify functions are called by the transaction queue when a notification for a transaction occurs.
	// If the handler returns an error the notification will be resent in the future (including across restarts)
	// NotifyReceipt is called the first time a receipt is observed for a transaction
	NotifyReceipt func(id uint64, notification *TransactionReceiptNotification) error
	// NotifyStateChanged is called every time the transaction status changes
	NotifyStateChanged func(id uint64, notification *TransactionStateChangedNotification) error
}

// TransactionRequestState is the type used to indicate which state the transaction is in
type TransactionRequestState uint8

// TransactionReceiptNotification is the notification emitted when the receipt is available
type TransactionReceiptNotification struct {
	Receipt types.Receipt // the receipt of the included transaction
}

// TransactionStateChangedNotification is the notification emitted when the state of the request changes
// Note: by the time the handler processes the notification, the state might have already changed again
type TransactionStateChangedNotification struct {
	OldState TransactionRequestState // the state prior to the change
	NewState TransactionRequestState // the state after the change
}

const (
	// TransactionQueued is the initial state for all requests that enter the queue
	TransactionQueued TransactionRequestState = 0
	// TransactionPending means the request is no longer in the queue but not yet confirmed
	TransactionPending TransactionRequestState = 1
	// TransactionConfirmed is entered the first time a confirmation is received. This is a terminal state.
	TransactionConfirmed TransactionRequestState = 2
	// TransactionStatusUnknown is used for all cases where it is unclear wether the transaction was broadcast or not. This is also used for timed-out transactions.
	TransactionStatusUnknown TransactionRequestState = 3
	// TransactionCancelled is used for all cases where it is certain the transaction was and will never be sent
	TransactionCancelled TransactionRequestState = 4
)
