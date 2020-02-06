package chain

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/accounts/abi/bind/backends"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethersphere/swarm/state"
	mock "github.com/ethersphere/swarm/swap/chain/mock"
	"github.com/ethersphere/swarm/testutil"
)

func init() {
	testutil.Init()
}

var (
	senderKey, _  = crypto.HexToECDSA("634fb5a872396d9693e5c9f9d7233cfa93f395c093371017ff44aa9ae6564cdd")
	senderAddress = crypto.PubkeyToAddress(senderKey.PublicKey)
)

var defaultBackend = backends.NewSimulatedBackend(core.GenesisAlloc{
	senderAddress: {Balance: big.NewInt(1000000000000000000)},
}, 8000000)

func newTestBackend() *mock.TestBackend {
	return mock.NewTestBackend(defaultBackend)
}

var TestRequestTypeID = TransactionRequestTypeID{
	Handler:     "test",
	RequestType: "TestRequest",
}

type TestRequest struct {
	Value uint64
}

type TxSchedulerTester struct {
	lock    sync.Mutex
	chans   map[uint64]*TxSchedulerTesterRequest
	backend Backend
}

type TxSchedulerTesterRequest struct {
	ReceiptNotification      chan *TransactionReceiptNotification
	StateChangedNotification chan *TransactionStateChangedNotification
	hash                     common.Hash
}

func newTxSchedulerTester(backend Backend, txq TxScheduler) *TxSchedulerTester {
	t := &TxSchedulerTester{
		backend: backend,
		chans:   make(map[uint64]*TxSchedulerTesterRequest),
	}
	txq.SetHandlers(TestRequestTypeID, &TransactionRequestHandlers{
		Send: t.SendTransactionRequest,
		NotifyReceipt: func(id uint64, notification *TransactionReceiptNotification) error {
			t.getRequest(id).ReceiptNotification <- notification
			return nil
		},
		NotifyStateChanged: func(id uint64, notification *TransactionStateChangedNotification) error {
			t.getRequest(id).StateChangedNotification <- notification
			return nil
		},
	})
	return t
}

func (tc *TxSchedulerTester) getRequest(id uint64) *TxSchedulerTesterRequest {
	tc.lock.Lock()
	defer tc.lock.Unlock()
	c, ok := tc.chans[id]
	if !ok {
		tc.chans[id] = &TxSchedulerTesterRequest{
			ReceiptNotification:      make(chan *TransactionReceiptNotification),
			StateChangedNotification: make(chan *TransactionStateChangedNotification),
		}
		return tc.chans[id]
	}
	return c
}

func (tc *TxSchedulerTester) expectStateChangedNotification(ctx context.Context, id uint64, oldState TransactionRequestState, newState TransactionRequestState) error {
	var notification *TransactionStateChangedNotification
	select {
	case notification = <-tc.getRequest(id).StateChangedNotification:
	case <-ctx.Done():
		return ctx.Err()
	}

	if notification.OldState != oldState {
		return fmt.Errorf("wrong old state. got %v, expected %v", notification.OldState, oldState)
	}

	if notification.NewState != newState {
		return fmt.Errorf("wrong new state. got %v, expected %v", notification.NewState, newState)
	}

	return nil
}

func (tc *TxSchedulerTester) expectReceiptNotification(ctx context.Context, id uint64) error {
	var notification *TransactionReceiptNotification
	request := tc.getRequest(id)
	select {
	case notification = <-request.ReceiptNotification:
	case <-ctx.Done():
		return ctx.Err()
	}

	receipt, err := tc.backend.TransactionReceipt(context.Background(), request.hash)
	if err != nil {
		return err
	}
	if receipt == nil {
		return errors.New("no receipt found for transaction")
	}

	if notification.Receipt.TxHash != request.hash {
		return fmt.Errorf("wrong old state. got %v, expected %v", notification.Receipt.TxHash, request.hash)
	}

	return nil
}

func (tc *TxSchedulerTester) SendTransactionRequest(id uint64, backend Backend, opts *bind.TransactOpts) (hash common.Hash, err error) {
	var nonce uint64
	if opts.Nonce == nil {
		nonce, err = backend.PendingNonceAt(opts.Context, opts.From)
		if err != nil {
			return common.Hash{}, err
		}
	} else {
		nonce = opts.Nonce.Uint64()
	}

	signed, err := opts.Signer(types.HomesteadSigner{}, opts.From, types.NewTransaction(nonce, common.Address{}, big.NewInt(0), 100000, big.NewInt(int64(10000000)), []byte{}))
	if err != nil {
		return common.Hash{}, err
	}
	err = backend.SendTransaction(opts.Context, signed)
	if err != nil {
		return common.Hash{}, err
	}
	tc.getRequest(id).hash = signed.Hash()
	return signed.Hash(), nil
}

func setupTxQueueTest(run bool) (*TxQueue, func()) {
	backend := newTestBackend()
	store := state.NewInmemoryStore()
	txq := NewTxQueue(store, "test", backend, senderKey)
	if run {
		txq.Start()
	}
	return txq, func() {
		if run {
			txq.Stop()
		}
		store.Close()
		backend.Close()
	}
}

func TestNewPersistentQueue(t *testing.T) {
	store := state.NewInmemoryStore()
	defer store.Close()

	queue := NewPersistentQueue(store, "testq")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	var errout error
	var keys [10]string
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			var value uint64
			key, err := queue.Next(ctx, &value)
			if err != nil {
				errout = fmt.Errorf("failed to get next item: %v", err)
				return
			}

			if key != keys[i] {
				errout = fmt.Errorf("keys don't match: got %v, expected %v", key, keys[i])
				return
			}

			if value != uint64(i) {
				errout = fmt.Errorf("values don't match: got %v, expected %v", value, i)
				return
			}

			batch := new(state.StoreBatch)
			queue.Delete(batch, key)
			err = store.WriteBatch(batch)
			if err != nil {
				errout = fmt.Errorf("could not write batch: %v", err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			var value = uint64(i)
			batch := new(state.StoreBatch)
			key, trigger, err := queue.Queue(batch, value)
			keys[i] = key
			if err != nil {
				errout = fmt.Errorf("failed to queue item: %v", err)
				return
			}
			err = store.WriteBatch(batch)
			if err != nil {
				errout = fmt.Errorf("failed to write batch: %v", err)
				return
			}

			trigger()
		}
	}()

	wg.Wait()

	if errout != nil {
		t.Fatal(errout)
	}
}

func TestTxQueue(t *testing.T) {
	txq, clean := setupTxQueueTest(false)
	defer clean()
	tc := newTxSchedulerTester(txq.backend, txq)

	testRequest := &TestRequest{
		Value: 100,
	}

	id, err := txq.ScheduleRequest(TestRequestTypeID, testRequest)
	if err != nil {
		t.Fatal(err)
	}

	if id != 1 {
		t.Fatal("expected id to be 1")
	}

	txq.Start()
	defer txq.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err = tc.expectStateChangedNotification(ctx, id, TransactionQueued, TransactionPending); err != nil {
		t.Fatal(err)
	}

	if err = tc.expectStateChangedNotification(ctx, id, TransactionPending, TransactionConfirmed); err != nil {
		t.Fatal(err)
	}

	if err = tc.expectReceiptNotification(ctx, id); err != nil {
		t.Fatal(err)
	}
}

func TestTxQueueManyRequests(t *testing.T) {
	txq, clean := setupTxQueueTest(true)
	defer clean()
	tc := newTxSchedulerTester(txq.backend, txq)

	var ids []uint64
	count := 200
	for i := 0; i < count; i++ {
		id, err := txq.ScheduleRequest(TestRequestTypeID, 5)
		if err != nil {
			t.Fatal(err)
		}

		ids = append(ids, id)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	for _, id := range ids {
		err := tc.expectStateChangedNotification(ctx, id, TransactionQueued, TransactionPending)
		if err != nil {
			t.Fatal(err)
		}
		err = tc.expectStateChangedNotification(ctx, id, TransactionPending, TransactionConfirmed)
		if err != nil {
			t.Fatal(err)
		}
		err = tc.expectReceiptNotification(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
	}
}
