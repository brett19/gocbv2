package gocb

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/opentracing/opentracing-go"
	"gopkg.in/couchbase/gocbcore.v7"
)

// TODO: Need to handle timeouts here.  It might be neccessary to move
// timeout handling down into gocbcore, but that is still uncertain.

type kvProvider interface {
	AddEx(opts gocbcore.AddOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error)
	SetEx(opts gocbcore.SetOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error)
	ReplaceEx(opts gocbcore.ReplaceOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error)
	GetEx(opts gocbcore.GetOptions, cb gocbcore.GetExCallback) (gocbcore.PendingOp, error)
	DeleteEx(opts gocbcore.DeleteOptions, cb gocbcore.DeleteExCallback) (gocbcore.PendingOp, error)
	LookupInEx(opts gocbcore.LookupInOptions, cb gocbcore.LookupInExCallback) (gocbcore.PendingOp, error)
	MutateInEx(opts gocbcore.MutateInOptions, cb gocbcore.MutateInExCallback) (gocbcore.PendingOp, error)
}

func shortestTime(first, second time.Time) time.Time {
	if first.IsZero() {
		return second
	}
	if second.IsZero() || first.Before(second) {
		return first
	}
	return second
}

func (c *Collection) deadline(ctx context.Context, now time.Time, opTimeout time.Duration) (earliest time.Time) {
	if opTimeout > 0 {
		earliest = now.Add(opTimeout)
	}
	if d, ok := ctx.Deadline(); ok {
		earliest = shortestTime(earliest, d)
	}
	return shortestTime(earliest, now.Add(c.sb.KvTimeout))
}

// UpsertOptions are options that can be applied to an Upsert operation.
type UpsertOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	ExpireAt          time.Time
	Encode            Encode
	PersistTo         uint
	ReplicateTo       uint
	WithDurability    DurabilityLevel
}

// InsertOptions are options that can be applied to an Insert operation.
type InsertOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	ExpireAt          time.Time
	Encode            Encode
	PersistTo         uint
	ReplicateTo       uint
	WithDurability    DurabilityLevel
}

// Insert creates a new document in the Collection.
func (c *Collection) Insert(key string, val interface{}, opts *InsertOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &InsertOptions{}
	}
	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	encodeFn := opts.Encode
	if encodeFn == nil {
		encodeFn = DefaultEncode
	}
	span := c.startKvOpTrace(opts.ParentSpanContext, "Insert")
	defer span.Finish()

	var expiry uint32
	if opts.ExpireAt.IsZero() {
		expiry = 0
	} else {
		expiry = uint32(opts.ExpireAt.Unix())
	}

	log.Printf("Fetching Agent")

	collectionID, agent, err := c.getKvProviderAndId()
	if err != nil {
		return nil, err
	}

	log.Printf("Transcoding")

	bytes, flags, err := encodeFn(val)
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{})
	op, err := agent.AddEx(gocbcore.AddOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Value:        bytes,
		Flags:        flags,
		Expiry:       expiry,
		TraceContext: span.Context(),
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}
			errOut = err
			waitCh <- struct{}{}
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

		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}

	select {
	case <-deadlinedCtx.Done():
		if op.Cancel() {
			if err == context.DeadlineExceeded {
				errOut = TimeoutError{err: err}
			} else {
				errOut = deadlinedCtx.Err()
			}
		} else {
			<-waitCh
		}
	case <-waitCh:
	}

	return
}

// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
func (c *Collection) Upsert(key string, val interface{}, opts *UpsertOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &UpsertOptions{}
	}
	if opts.Context == nil {
		opts.Context = context.Background()
	}

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	if opts.Encode == nil {
		opts.Encode = DefaultEncode
	}
	span := c.startKvOpTrace(opts.ParentSpanContext, "Upsert")
	defer span.Finish()

	var expiry uint32
	if opts.ExpireAt.IsZero() {
		expiry = 0
	} else {
		expiry = uint32(opts.ExpireAt.Unix())
	}

	log.Printf("Fetching Agent")

	collectionID, agent, err := c.getKvProviderAndId()
	if err != nil {
		errOut = err
		return
	}

	log.Printf("Transcoding")

	bytes, flags, err := opts.Encode(val)
	if err != nil {
		errOut = err
		return
	}

	waitCh := make(chan struct{})

	op, err := agent.SetEx(gocbcore.SetOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Value:        bytes,
		Flags:        flags,
		Expiry:       expiry,
		TraceContext: span.Context(),
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}
			errOut = err
			waitCh <- struct{}{}
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

		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}

	select {
	case <-deadlinedCtx.Done():
		if op.Cancel() {
			err := deadlinedCtx.Err()
			if err == context.DeadlineExceeded {
				errOut = TimeoutError{err: err}
			} else {
				errOut = err
			}
		} else {
			<-waitCh
		}
	case <-waitCh:
	}

	return
}

// ReplaceOptions are the options available to a Replace operation
type ReplaceOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	ExpireAt          time.Time
	Cas               Cas
	Encode            Encode
	PersistTo         uint
	ReplicateTo       uint
	WithDurability    DurabilityLevel
}

// Replace updates a document in the collection
func (c *Collection) Replace(key string, val interface{}, opts *ReplaceOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &ReplaceOptions{}
	}

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	if opts.Encode == nil {
		opts.Encode = DefaultEncode
	}
	span := c.startKvOpTrace(opts.ParentSpanContext, "Replace")
	defer span.Finish()

	var expiry uint32
	if opts.ExpireAt.IsZero() {
		expiry = 0
	} else {
		expiry = uint32(opts.ExpireAt.Unix())
	}

	log.Printf("Fetching Agent")

	collectionID, agent, err := c.getKvProviderAndId()
	if err != nil {
		return nil, err
	}

	log.Printf("Transcoding")

	bytes, flags, err := opts.Encode(val)
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{})

	op, err := agent.ReplaceEx(gocbcore.ReplaceOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Value:        bytes,
		Flags:        flags,
		Expiry:       expiry,
		Cas:          gocbcore.Cas(opts.Cas),
		TraceContext: span.Context(),
	}, func(res *gocbcore.StoreResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}
			errOut = errors.Wrap(err, "could not perform replace")
			waitCh <- struct{}{}
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

		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}

	select {
	case <-deadlinedCtx.Done():
		if op.Cancel() {
			if err == context.DeadlineExceeded {
				errOut = TimeoutError{err: err}
			} else {
				errOut = deadlinedCtx.Err()
			}
		} else {
			<-waitCh
		}
	case <-waitCh:
	}

	return
}

// GetOptions are the options available to a Get operation
type GetOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	WithExpiry        bool
	spec              *LookupInOptions
}

func (opts GetOptions) Project(paths ...string) GetOptions {
	spec := LookupInOptions{}
	for _, path := range paths {
		spec = spec.Path(path)
	}

	opts.spec = &spec
	return opts
}

func (c *Collection) Get(key string, opts *GetOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetOptions{}
	}

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	span := c.startKvOpTrace(opts.ParentSpanContext, "Get")
	defer span.Finish()

	if opts.spec == nil {
		if opts.WithExpiry {
			expSpec := LookupInOptions{}.getWithFlags("$document.exptime", SubdocFlagXattr).Path("")
			expSpec.Context = deadlinedCtx
			result, err := c.lookupIn(deadlinedCtx, span.Context(), key, expSpec)
			if err != nil {
				errOut = err
				return
			}

			doc := &GetResult{}
			doc.withExpiry = true

			result.ContentAt(0, &doc.expireAt)
			result.ContentAt(1, &doc.contents)

			docOut = doc
			return
		}

		docOut, errOut = c.get(deadlinedCtx, span.Context(), key, opts)
		return
	}
	result, err := c.lookupIn(deadlinedCtx, span.Context(), key, *opts.spec)
	if err != nil {
		errOut = err
		return
	}

	doc := &GetResult{}
	doc.fromSubDoc(opts.spec.spec.ops, result)
	docOut = doc

	return
}

func (c *Collection) get(ctx context.Context, traceCtx opentracing.SpanContext, key string, opts *GetOptions) (docOut *GetResult, errOut error) {
	collectionID, agent, err := c.getKvProviderAndId()
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{})
	op, err := agent.GetEx(gocbcore.GetOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		TraceContext: traceCtx,
	}, func(res *gocbcore.GetResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}

			errOut = err
			waitCh <- struct{}{}
			return
		}
		if res != nil {
			doc := &GetResult{
				id:       key,
				contents: res.Value,
				flags:    res.Flags,
				cas:      Cas(res.Cas),
			}

			docOut = doc
		}

		waitCh <- struct{}{}
	})
	if errOut != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		if op.Cancel() {
			if err == context.DeadlineExceeded {
				errOut = TimeoutError{err: err}
			} else {
				errOut = ctx.Err()
			}
		} else {
			<-waitCh
		}
	case <-waitCh:
	}

	return
}

type RemoveOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	Cas               Cas
	PersistTo         uint
	ReplicateTo       uint
	WithDurability    DurabilityLevel
}

func (c *Collection) Remove(key string, opts *RemoveOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &RemoveOptions{}
	}

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	collectionID, agent, err := c.getKvProviderAndId()
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{})

	op, err := agent.DeleteEx(gocbcore.DeleteOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Cas:          gocbcore.Cas(opts.Cas),
		TraceContext: opts.ParentSpanContext,
	}, func(res *gocbcore.DeleteResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}
			errOut = err
			waitCh <- struct{}{}
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

		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}

	select {
	case <-deadlinedCtx.Done():
		if op.Cancel() {
			if err == context.DeadlineExceeded {
				errOut = TimeoutError{err: err}
			} else {
				errOut = deadlinedCtx.Err()
			}
		} else {
			<-waitCh
		}
	case <-waitCh:
	}

	return
}

type lookupSpec struct {
	ops   []gocbcore.SubDocOp
	flags gocbcore.SubdocDocFlag
}

// LookupInOptions are the set of options available to LookupIn.
type LookupInOptions struct {
	Context           context.Context
	Timeout           time.Duration
	spec              lookupSpec
	ParentSpanContext opentracing.SpanContext
	WithExpiry        bool
}

// Path indicates a path to be retrieved from the document.  The value of the path
// can later be retrieved from the LookupResult.
// The path syntax follows N1QL's path syntax (e.g. `foo.bar.baz`).
func (opts LookupInOptions) Path(path string) LookupInOptions {
	return opts.getWithFlags(path, SubdocFlagNone)
}

func (opts LookupInOptions) getWithFlags(path string, flags SubdocFlag) LookupInOptions {
	if path == "" {
		op := gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpGetDoc,
			Flags: gocbcore.SubdocFlag(flags),
		}
		opts.spec.ops = append(opts.spec.ops, op)
		return opts
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpGet,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
	}

	opts.spec.ops = append(opts.spec.ops, op)
	return opts
}

// PathExists is similar to Path(), but does not actually retrieve the value from the server.
// This may save bandwidth if you only need to check for the existence of a
// path (without caring for its content). You can check the status of this
// operation by using .ContentAt (and ignoring the value) or .Exists() on the LookupResult.
func (opts LookupInOptions) PathExists(path string) LookupInOptions {
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpExists,
		Path:  path,
		Flags: gocbcore.SubdocFlagNone,
	}
	opts.spec.ops = append(opts.spec.ops, op)

	return opts
}

// LookupIn performs a set of subdocument lookup operations on the document identified by key.
func (c *Collection) LookupIn(key string, opts *LookupInOptions) (docOut *LookupInResult, errOut error) {
	if opts == nil {
		opts = &LookupInOptions{}
	}

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	span := c.startKvOpTrace(opts.ParentSpanContext, "LookupIn")
	defer span.Finish()

	return c.lookupIn(deadlinedCtx, span.Context(), key, *opts)
}

func (c *Collection) lookupIn(ctx context.Context, traceCtx opentracing.SpanContext, key string, opts LookupInOptions) (docOut *LookupInResult, errOut error) {
	collectionID, agent, err := c.getKvProviderAndId()
	if err != nil {
		return nil, err
	}

	if opts.WithExpiry {
		op := gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpGet,
			Path:  "$document.exptime",
			Flags: gocbcore.SubdocFlag(SubdocFlagXattr),
		}

		opts.spec.ops = append([]gocbcore.SubDocOp{op}, opts.spec.ops...)
	}

	waitCh := make(chan struct{})

	op, err := agent.LookupInEx(gocbcore.LookupInOptions{
		Key:          []byte(key),
		Flags:        opts.spec.flags,
		Ops:          opts.spec.ops,
		CollectionID: collectionID,
		TraceContext: traceCtx,
	}, func(res *gocbcore.LookupInResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}
			errOut = err
			waitCh <- struct{}{}
			return
		}

		if res != nil {
			resSet := &LookupInResult{}
			resSet.cas = Cas(res.Cas)
			resSet.contents = make([]lookupInPartial, len(opts.spec.ops))

			for i, opRes := range res.Ops {
				// resSet.contents[i].path = opts.spec.ops[i].Path
				resSet.contents[i].err = opRes.Err
				if opRes.Value != nil {
					resSet.contents[i].data = append([]byte(nil), opRes.Value...)
				}
			}

			if opts.WithExpiry {
				// if expiry was requested then extract and remove it from the results
				resSet.withExpiry = true
				resSet.ContentAt(0, &resSet.expireAt)
				resSet.contents = resSet.contents[1:]
			}

			docOut = resSet
		}

		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}

	select {
	case <-ctx.Done():
		if op.Cancel() {
			if err == context.DeadlineExceeded {
				errOut = TimeoutError{err: err}
			} else {
				errOut = ctx.Err()
			}
		} else {
			<-waitCh
		}
	case <-waitCh:
	}

	return
}

type MutateSpec struct {
	ops   []gocbcore.SubDocOp
	flags gocbcore.SubdocDocFlag
	// errs  []error
}

type MutateOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	Cas               Cas
	PersistTo         uint
	ReplicateTo       uint
}

func (spec *MutateSpec) marshalValue(value interface{}) []byte {
	if value, ok := value.([]byte); ok {
		return value
	}

	if value, ok := value.(*[]byte); ok {
		return *value
	}

	bytes, err := json.Marshal(value)
	if err != nil {
		// set.errs.add(err)
		return nil
	}
	return bytes
}

func (spec MutateSpec) Insert(path string, val interface{}, createParents bool) MutateSpec {
	var flags SubdocFlag
	if createParents {
		flags |= SubdocFlagCreatePath
	}

	return spec.InsertWithFlags(path, val, flags)
}

func (spec MutateSpec) InsertWithFlags(path string, val interface{}, flags SubdocFlag) MutateSpec {
	if path == "" {
		op := gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpAddDoc,
			Flags: gocbcore.SubdocFlag(flags),
			Value: spec.marshalValue(val),
		}
		spec.ops = append(spec.ops, op)
		return spec
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpDictAdd,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: spec.marshalValue(val),
	}
	spec.ops = append(spec.ops, op)
	return spec
}

func (spec MutateSpec) Replace(path string, val interface{}) MutateSpec {
	return spec.ReplaceWithFlags(path, val, SubdocFlagNone)
}

func (spec MutateSpec) ReplaceWithFlags(path string, val interface{}, flags SubdocFlag) MutateSpec {
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpReplace,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: spec.marshalValue(val),
	}
	spec.ops = append(spec.ops, op)
	return spec
}

func (c *Collection) Mutate(key string, spec MutateSpec, opts *MutateOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &MutateOptions{}
	}

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	span := c.startKvOpTrace(opts.ParentSpanContext, "MutateIn")
	defer span.Finish()

	collectionID, agent, err := c.getKvProviderAndId()
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{})

	op, err := agent.MutateInEx(gocbcore.MutateInOptions{
		Key:          []byte(key),
		Flags:        spec.flags,
		Cas:          gocbcore.Cas(opts.Cas),
		CollectionID: collectionID,
		Ops:          spec.ops,
		TraceContext: span.Context(),
	}, func(res *gocbcore.MutateInResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}
			errOut = err
			waitCh <- struct{}{}
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.sb.BucketName,
		}
		mutRes := &MutationResult{
			mt: mutTok,
		}
		mutRes.cas = Cas(res.Cas)
		mutOut = mutRes

		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}

	select {
	case <-deadlinedCtx.Done():
		if op.Cancel() {
			if err == context.DeadlineExceeded {
				errOut = TimeoutError{err: err}
			} else {
				errOut = deadlinedCtx.Err()
			}
		} else {
			<-waitCh
		}
	case <-waitCh:
	}

	return
}

// func (c *Collection) GetAndTouch(key string, opts *TouchOptions) (docOut *GetResult, errOut error) {
// 	if opts == nil {
// 		opts = &TouchOptions{}
// 	}

// 	collectionID, agent, err := c.getKvProviderAndId()
// 	if err != nil {
// 		return nil, err
// 	}

// 	waitCh := make(chan struct{})

// 	agent.GetAndTouchEx(gocbcore.GetAndTouchOptions{
// 		Key:          []byte(key),
// 		CollectionID: collectionID,
// 		Expiry:       opts.expiry,
// 	}, func(res *gocbcore.GetAndTouchResult, err error) {
// 		if err != nil {
// 			errOut = err
// 			waitCh <- struct{}{}
// 			return
// 		}

// 		docOut = &GetResult{
// 			id: key,
// 			contents: []getPartial{
// 				getPartial{bytes: res.Value, path: ""},
// 			},
// 			flags: res.Flags,
// 			cas:   Cas(res.Cas),
// 		}
// 		waitCh <- struct{}{}
// 	})

// 	<-waitCh

// 	return
// }

// type GetAndLockOptions struct {
// 	lockTime uint32
// }

// func (opts GetAndLockOptions) LockTime(lockTime time.Time) GetAndLockOptions {
// 	opts.lockTime = uint32(lockTime.Unix())
// 	return opts
// }

// func (c *Collection) GetAndLock(key string, opts *GetAndLockOptions) (docOut *GetResult, errOut error) {
// 	if opts == nil {
// 		opts = &GetAndLockOptions{}
// 	}

// 	collectionID, agent, err := c.getKvProviderAndId()
// 	if err != nil {
// 		return nil, err
// 	}

// 	waitCh := make(chan struct{})

// 	agent.GetAndLockEx(gocbcore.GetAndLockOptions{
// 		Key:          []byte(key),
// 		CollectionID: collectionID,
// 		LockTime:     opts.lockTime,
// 	}, func(res *gocbcore.GetAndLockResult, err error) {
// 		if err != nil {
// 			errOut = err
// 			waitCh <- struct{}{}
// 			return
// 		}

// 		docOut = &GetResult{
// 			id: key,
// 			contents: []getPartial{
// 				getPartial{bytes: res.Value, path: ""},
// 			},
// 			flags: res.Flags,
// 			cas:   Cas(res.Cas),
// 		}
// 		waitCh <- struct{}{}
// 	})

// 	<-waitCh

// 	return
// }

// type UnlockOptions struct {
// 	cas Cas
// }

// func (opts UnlockOptions) Cas(cas Cas) UnlockOptions {
// 	opts.cas = cas
// 	return opts
// }

// func (c *Collection) Unlock(key string, opts *UnlockOptions) (errOut error) {
// 	if opts == nil {
// 		opts = &UnlockOptions{}
// 	}

// 	collectionID, agent, err := c.getKvProviderAndId()
// 	if err != nil {
// 		return err
// 	}

// 	waitCh := make(chan struct{})

// 	agent.UnlockEx(gocbcore.UnlockOptions{
// 		Key:          []byte(key),
// 		CollectionID: collectionID,
// 		Cas:          gocbcore.Cas(opts.cas),
// 	}, func(res *gocbcore.UnlockResult, err error) {
// 		if err != nil {
// 			errOut = err
// 			waitCh <- struct{}{}
// 			return
// 		}

// 		waitCh <- struct{}{}
// 	})

// 	<-waitCh

// 	return
// }

// type GetReplicaOptions struct {
// 	replicaIdx int
// }

// func (opts GetReplicaOptions) ReplicaIndex(replicaIdx int) GetReplicaOptions {
// 	opts.replicaIdx = replicaIdx
// 	return opts
// }

// func (c *Collection) GetReplica(key string, opts *GetReplicaOptions) (docOut *GetResult, errOut error) {
// 	if opts == nil {
// 		opts = &GetReplicaOptions{}
// 	}

// 	collectionID, agent, err := c.getKvProviderAndId()
// 	if err != nil {
// 		return nil, err
// 	}

// 	waitCh := make(chan struct{})

// 	agent.GetReplicaEx(gocbcore.GetReplicaOptions{
// 		Key:          []byte(key),
// 		CollectionID: collectionID,
// 		ReplicaIdx:   opts.replicaIdx,
// 	}, func(res *gocbcore.GetReplicaResult, err error) {
// 		if err != nil {
// 			errOut = err
// 			waitCh <- struct{}{}
// 			return
// 		}

// 		docOut = &GetResult{
// 			id: key,
// 			contents: []getPartial{
// 				getPartial{bytes: res.Value, path: ""},
// 			},
// 			flags: res.Flags,
// 			cas:   Cas(res.Cas),
// 		}
// 		waitCh <- struct{}{}
// 	})

// 	<-waitCh

// 	return
// }

// type TouchOptions struct {
// 	expiry uint32
// }

// func (opts TouchOptions) ExpireAt(expiry time.Time) TouchOptions {
// 	opts.expiry = uint32(expiry.Unix())
// 	return opts
// }

// func (c *Collection) Touch(key string, opts *TouchOptions) (errOut error) {
// 	if opts == nil {
// 		opts = &TouchOptions{}
// 	}

// 	collectionID, agent, err := c.getKvProviderAndId()
// 	if err != nil {
// 		return err
// 	}

// 	waitCh := make(chan struct{})

// 	agent.TouchEx(gocbcore.TouchOptions{
// 		Key:          []byte(key),
// 		CollectionID: collectionID,
// 		Expiry:       opts.expiry,
// 	}, func(res *gocbcore.TouchResult, err error) {
// 		if err != nil {
// 			errOut = err
// 			waitCh <- struct{}{}
// 			return
// 		}

// 		waitCh <- struct{}{}
// 	})

// 	<-waitCh

// 	return
// }
