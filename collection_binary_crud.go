package gocb

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
	"gopkg.in/couchbase/gocbcore.v7"
)

// CollectionBinary is a set of binary operations.
type CollectionBinary struct {
	*Collection
}

// AppendOptions are the options available to the Append operation.
type AppendOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// Append appends a byte value to a document.
func (c *CollectionBinary) Append(key string, val []byte, opts *AppendOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &AppendOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "BinaryAppend")
	defer span.Finish()

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	collectionID, agent, err := c.getKvProviderAndID(deadlinedCtx)
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.AppendEx(gocbcore.AdjoinOptions{
		Key:          []byte(key),
		Value:        val,
		CollectionID: collectionID,
		TraceContext: span.Context(),
	}, func(res *gocbcore.AdjoinResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}

			errOut = err
			ctrl.Resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.sb.BucketName,
		}
		mutOut = &MutationResult{
			mt: mutTok,
		}
		mutOut.cas = Cas(res.Cas)

		ctrl.Resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// PrependOptions are the options available to the Prepend operation.
type PrependOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// Prepend prepends a byte value to a document.
func (c *CollectionBinary) Prepend(key string, val []byte, opts *PrependOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &PrependOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "BinaryPrepend")
	defer span.Finish()

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	collectionID, agent, err := c.getKvProviderAndID(deadlinedCtx)
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.PrependEx(gocbcore.AdjoinOptions{
		Key:          []byte(key),
		Value:        val,
		CollectionID: collectionID,
		TraceContext: span.Context(),
	}, func(res *gocbcore.AdjoinResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}

			errOut = err
			ctrl.Resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.sb.BucketName,
		}
		mutOut = &MutationResult{
			mt: mutTok,
		}
		mutOut.cas = Cas(res.Cas)

		ctrl.Resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// CounterOptions are the options available to the Counter operation.
type CounterOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	ExpireAt          time.Time
	Initial           int64
	Delta             uint64
}

// Increment performs an atomic addition for an integer document. Passing a
// non-negative `initial` value will cause the document to be created if it did not
// already exist.
func (c *CollectionBinary) Increment(key string, opts *CounterOptions) (mutOut *CounterResult, errOut error) {
	if opts == nil {
		opts = &CounterOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Counter")
	defer span.Finish()

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	realInitial := uint64(0xFFFFFFFFFFFFFFFF)
	if opts.Initial >= 0 {
		realInitial = uint64(opts.Initial)
	}

	var expiry uint32
	if opts.ExpireAt.IsZero() {
		expiry = 0
	} else {
		expiry = uint32(opts.ExpireAt.Unix())
	}

	collectionID, agent, err := c.getKvProviderAndID(deadlinedCtx)
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.IncrementEx(gocbcore.CounterOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Delta:        opts.Delta,
		Initial:      realInitial,
		Expiry:       expiry,
		TraceContext: span.Context(),
	}, func(res *gocbcore.CounterResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}

			errOut = err
			ctrl.Resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.sb.BucketName,
		}
		mutOut = &CounterResult{
			mt:      mutTok,
			cas:     Cas(res.Cas),
			content: res.Value,
		}

		ctrl.Resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// Decrement performs an atomic subtraction for an integer document. Passing a
// non-negative `initial` value will cause the document to be created if it did not
// already exist.
func (c *CollectionBinary) Decrement(key string, opts *CounterOptions) (mutOut *CounterResult, errOut error) {
	if opts == nil {
		opts = &CounterOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Counter")
	defer span.Finish()

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	realInitial := uint64(0xFFFFFFFFFFFFFFFF)
	if opts.Initial >= 0 {
		realInitial = uint64(opts.Initial)
	}

	var expiry uint32
	if opts.ExpireAt.IsZero() {
		expiry = 0
	} else {
		expiry = uint32(opts.ExpireAt.Unix())
	}

	collectionID, agent, err := c.getKvProviderAndID(deadlinedCtx)
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.DecrementEx(gocbcore.CounterOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Delta:        opts.Delta,
		Initial:      realInitial,
		Expiry:       expiry,
		TraceContext: span.Context(),
	}, func(res *gocbcore.CounterResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}

			errOut = err
			ctrl.Resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.sb.BucketName,
		}
		mutOut = &CounterResult{
			mt:      mutTok,
			cas:     Cas(res.Cas),
			content: res.Value,
		}

		ctrl.Resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}
