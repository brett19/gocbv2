package gocb

import (
	"context"
	"fmt"
	"time"

	"gopkg.in/couchbase/gocbcore.v7"
)

type mockClient struct {
	bucketName        string
	useMutationTokens bool
	collectionId      uint32
	scopeId           uint32
	mockKvProvider    kvProvider
}

type mockKvOperator struct {
	opWait                time.Duration
	value                 interface{}
	cas                   gocbcore.Cas
	mt                    gocbcore.MutationToken
	flags                 uint32
	datatype              uint8
	err                   error
	opCancellationSuccess bool
}

type mockPendingOp struct {
	cancelSuccess bool
}

func (mpo *mockPendingOp) Cancel() bool {
	return mpo.cancelSuccess
}

func (mko *mockKvOperator) AddEx(opts gocbcore.AddOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.StoreResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil
}

func (mko *mockKvOperator) SetEx(opts gocbcore.SetOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.StoreResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil

}

func (mko *mockKvOperator) ReplaceEx(opts gocbcore.ReplaceOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.StoreResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil
}

func (mko *mockKvOperator) GetEx(opts gocbcore.GetOptions, cb gocbcore.GetExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.GetResult{
				Cas:      mko.cas,
				Flags:    mko.flags,
				Datatype: mko.datatype,
				Value:    mko.value.([]byte),
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil
}

func (mko *mockKvOperator) GetAndTouchEx(opts gocbcore.GetAndTouchOptions, cb gocbcore.GetAndTouchExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.GetAndTouchResult{
				Cas:      mko.cas,
				Flags:    mko.flags,
				Datatype: mko.datatype,
				Value:    mko.value.([]byte),
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil
}

func (mko *mockKvOperator) GetAndLockEx(opts gocbcore.GetAndLockOptions, cb gocbcore.GetAndLockExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.GetAndLockResult{
				Cas:      mko.cas,
				Flags:    mko.flags,
				Datatype: mko.datatype,
				Value:    mko.value.([]byte),
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil
}

func (mko *mockKvOperator) UnlockEx(opts gocbcore.UnlockOptions, cb gocbcore.UnlockExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.UnlockResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil
}

func (mko *mockKvOperator) TouchEx(opts gocbcore.TouchOptions, cb gocbcore.TouchExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.TouchResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil
}

func (mko *mockKvOperator) DeleteEx(opts gocbcore.DeleteOptions, cb gocbcore.DeleteExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.DeleteResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil
}

func (mko *mockKvOperator) IncrementEx(opts gocbcore.CounterOptions, cb gocbcore.CounterExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.CounterResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
				Value:         mko.value.(uint64),
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil
}

func (mko *mockKvOperator) DecrementEx(opts gocbcore.CounterOptions, cb gocbcore.CounterExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.CounterResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
				Value:         mko.value.(uint64),
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil
}

func (mko *mockKvOperator) AppendEx(opts gocbcore.AdjoinOptions, cb gocbcore.AdjoinExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.AdjoinResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil
}

func (mko *mockKvOperator) PrependEx(opts gocbcore.AdjoinOptions, cb gocbcore.AdjoinExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.AdjoinResult{
				Cas:           mko.cas,
				MutationToken: mko.mt,
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil
}

func (mko *mockKvOperator) LookupInEx(opts gocbcore.LookupInOptions, cb gocbcore.LookupInExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.LookupInResult{
				Cas: mko.cas,
				Ops: mko.value.([]gocbcore.SubDocResult),
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil

}

func (mko *mockKvOperator) MutateInEx(opts gocbcore.MutateInOptions, cb gocbcore.MutateInExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.MutateInResult{
				Cas:           mko.cas,
				Ops:           mko.value.([]gocbcore.SubDocResult),
				MutationToken: mko.mt,
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil
}

func (mko *mockKvOperator) ObserveEx(opts gocbcore.ObserveOptions, cb gocbcore.ObserveExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.ObserveResult{
				Cas:      mko.cas,
				KeyState: mko.value.(gocbcore.KeyState),
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil
}

func (mko *mockKvOperator) GetReplicaEx(opts gocbcore.GetReplicaOptions, cb gocbcore.GetReplicaExCallback) (gocbcore.PendingOp, error) {
	time.AfterFunc(mko.opWait, func() {
		if mko.err == nil {
			cb(&gocbcore.GetReplicaResult{
				Cas:      mko.cas,
				Flags:    mko.flags,
				Datatype: mko.datatype,
				Value:    mko.value.([]byte),
			}, nil)
		} else {
			cb(nil, mko.err)
		}
	})

	return &mockPendingOp{cancelSuccess: mko.opCancellationSuccess}, nil
}

func (mc *mockClient) Hash() string {
	return fmt.Sprintf("%s-%t",
		mc.bucketName,
		mc.useMutationTokens)
}

func (mc *mockClient) connect() error {
	return nil
}

func (mc *mockClient) close() error {
	return nil
}

func (mc *mockClient) fetchCollectionManifest() (bytesOut []byte, errOut error) {
	return []byte{}, nil
}

func (mc *mockClient) fetchCollectionID(ctx context.Context, scopeName string, collectionName string) (uint32, error) {
	return mc.collectionId, nil
}

func (mc *mockClient) getKvProvider() (kvProvider, error) {
	return mc.mockKvProvider, nil
}

func (mc *mockClient) getQueryProvider() (queryProvider, error) {
	return nil, nil
}

func (mc *mockClient) getDiagnosticsProvider() (diagnosticsProvider, error) {
	return nil, nil
}
