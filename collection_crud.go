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
	GetAndTouchEx(opts gocbcore.GetAndTouchOptions, cb gocbcore.GetAndTouchExCallback) (gocbcore.PendingOp, error)
	GetAndLockEx(opts gocbcore.GetAndLockOptions, cb gocbcore.GetAndLockExCallback) (gocbcore.PendingOp, error)
	UnlockEx(opts gocbcore.UnlockOptions, cb gocbcore.UnlockExCallback) (gocbcore.PendingOp, error)
	TouchEx(opts gocbcore.TouchOptions, cb gocbcore.TouchExCallback) (gocbcore.PendingOp, error)
	IncrementEx(opts gocbcore.CounterOptions, cb gocbcore.CounterExCallback) (gocbcore.PendingOp, error)
	DecrementEx(opts gocbcore.CounterOptions, cb gocbcore.CounterExCallback) (gocbcore.PendingOp, error)
	AppendEx(opts gocbcore.AdjoinOptions, cb gocbcore.AdjoinExCallback) (gocbcore.PendingOp, error)
	PrependEx(opts gocbcore.AdjoinOptions, cb gocbcore.AdjoinExCallback) (gocbcore.PendingOp, error)
}

// shortestTime calculates the shortest of two times, this is used for context deadlines.
func shortestTime(first, second time.Time) time.Time {
	if first.IsZero() {
		return second
	}
	if second.IsZero() || first.Before(second) {
		return first
	}
	return second
}

// deadline calculates the shortest timeout from context, operation timeout and collection level kv timeout.
func (c *Collection) deadline(ctx context.Context, now time.Time, opTimeout time.Duration) (earliest time.Time) {
	if opTimeout > 0 {
		earliest = now.Add(opTimeout)
	}
	if d, ok := ctx.Deadline(); ok {
		earliest = shortestTime(earliest, d)
	}
	return shortestTime(earliest, now.Add(c.sb.KvTimeout))
}

type opManager struct {
	signal chan struct{}
	ctx    context.Context
}

func (c *Collection) newOpManager(ctx context.Context) *opManager {
	return &opManager{
		signal: make(chan struct{}, 1),
		ctx:    ctx,
	}
}

func (ctrl *opManager) Resolve() {
	ctrl.signal <- struct{}{}
}

func (ctrl *opManager) Wait(op gocbcore.PendingOp, err error) (errOut error) {
	if err != nil {
		return err
	}

	select {
	case <-ctrl.ctx.Done():
		if op.Cancel() {
			if err == context.DeadlineExceeded {
				errOut = timeoutError{err: ctrl.ctx.Err()}
			} else {
				errOut = ctrl.ctx.Err()
			}
		} else {
			<-ctrl.signal
		}
	case <-ctrl.signal:
	}

	return
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
func (c *Collection) Insert(key string, val interface{}, opts *InsertOptions) (mutOut *StoreResult, errOut error) {
	if opts == nil {
		opts = &InsertOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Insert")
	defer span.Finish()

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	// We could just set context.WithDeadline multiple times as it always accepts the shortest deadline.
	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	encodeFn := opts.Encode
	if encodeFn == nil {
		encodeFn = DefaultEncode
	}

	var expiry uint32
	if opts.ExpireAt.IsZero() {
		expiry = 0
	} else {
		expiry = uint32(opts.ExpireAt.Unix())
	}

	collectionID, agent, err := c.getKvProviderAndID(deadlinedCtx)
	if err != nil {
		errOut = err
		return
	}

	encodeSpan := opentracing.GlobalTracer().StartSpan("Encoding", opentracing.ChildOf(span.Context()))
	bytes, flags, err := encodeFn(val)
	if err != nil {
		errOut = err
		return
	}
	encodeSpan.Finish()

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.AddEx(gocbcore.AddOptions{
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
			errOut = kvError{err}
			ctrl.Resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.sb.BucketName,
		}
		mutOut = &StoreResult{
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

// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
func (c *Collection) Upsert(key string, val interface{}, opts *UpsertOptions) (mutOut *StoreResult, errOut error) {
	if opts == nil {
		opts = &UpsertOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Upsert")
	defer span.Finish()

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

	var expiry uint32
	if opts.ExpireAt.IsZero() {
		expiry = 0
	} else {
		expiry = uint32(opts.ExpireAt.Unix())
	}

	log.Printf("Fetching Agent")

	collectionID, agent, err := c.getKvProviderAndID(deadlinedCtx)
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

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.SetEx(gocbcore.SetOptions{
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
			errOut = kvError{err}
			ctrl.Resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.sb.BucketName,
		}
		mutOut = &StoreResult{
			mt: mutTok,
		}
		mutOut.cas = Cas(res.Cas)

		ctrl.Resolve()
	}))
	if err != nil {
		return
	}

	return
}

// ReplaceOptions are the options available to a Replace operation.
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

// Replace updates a document in the collection.
func (c *Collection) Replace(key string, val interface{}, opts *ReplaceOptions) (mutOut *StoreResult, errOut error) {
	if opts == nil {
		opts = &ReplaceOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Replace")
	defer span.Finish()

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

	expiry := uint32(0)
	if !opts.ExpireAt.IsZero() {
		expiry = uint32(opts.ExpireAt.Unix())
	}

	log.Printf("Fetching Agent")

	collectionID, agent, err := c.getKvProviderAndID(deadlinedCtx)
	if err != nil {
		return nil, err
	}

	log.Printf("Transcoding")

	bytes, flags, err := opts.Encode(val)
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.ReplaceEx(gocbcore.ReplaceOptions{
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
			ctrl.Resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.sb.BucketName,
		}
		mutOut = &StoreResult{
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

// GetOptions are the options available to a Get operation.
type GetOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	WithExpiry        bool
	spec              *LookupInOptions
}

// Project causes the Get operation to only fetch the fields indicated
// by the paths. The result of the operation is then treated as a
// standard GetResult.
func (opts GetOptions) Project(paths ...string) GetOptions {
	spec := LookupInOptions{}
	for _, path := range paths {
		spec = spec.Path(path)
	}

	opts.spec = &spec
	return opts
}

// Get performs a fetch operation against the collection. This can take 3 paths, a standard full document
// fetch, a subdocument full document fetch also fetching document expiry (when WithExpiry is set),
// or a subdocument fetch (when Project is used).
func (c *Collection) Get(key string, opts *GetOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Get")
	defer span.Finish()

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	if opts.spec == nil && !opts.WithExpiry {
		// No projection and no expiry so standard fulldoc
		docOut, errOut = c.get(deadlinedCtx, span.Context(), key, opts)
		docOut.id = key
		return
	}

	spec := opts.spec
	if spec == nil {
		// This is a subdoc full doc
		spec = &LookupInOptions{Context: deadlinedCtx, WithExpiry: opts.WithExpiry}
		*spec = spec.Path("")
	}

	// If len is > 16 then we have to convert to fulldoc
	if len(spec.spec.ops) > 16 {
		*spec = spec.Path("")
	}

	result, err := c.lookupIn(deadlinedCtx, span.Context(), key, *spec)
	if err != nil {
		errOut = err
		return
	}

	doc := &GetResult{}
	doc.withExpiry = result.withExpiry
	doc.expireAt = result.expireAt
	doc.id = key
	doc.fromSubDoc(spec.spec.ops, result)
	docOut = doc

	return
}

// get performs a full document fetch against the collection
func (c *Collection) get(ctx context.Context, traceCtx opentracing.SpanContext, key string, opts *GetOptions) (docOut *GetResult, errOut error) {
	collectionID, agent, err := c.getKvProviderAndID(ctx)
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.Wait(agent.GetEx(gocbcore.GetOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		TraceContext: traceCtx,
	}, func(res *gocbcore.GetResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}

			errOut = err
			ctrl.Resolve()
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

		ctrl.Resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// RemoveOptions are the options available to the Remove command.
type RemoveOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	Cas               Cas
	PersistTo         uint
	ReplicateTo       uint
	WithDurability    DurabilityLevel
}

// Remove removes a document from the collection.
func (c *Collection) Remove(key string, opts *RemoveOptions) (mutOut *StoreResult, errOut error) {
	if opts == nil {
		opts = &RemoveOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Remove")
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
	err = ctrl.Wait(agent.DeleteEx(gocbcore.DeleteOptions{
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
			ctrl.Resolve()
			return
		}

		mutTok := MutationToken{
			token:      res.MutationToken,
			bucketName: c.sb.BucketName,
		}
		mutOut = &StoreResult{
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
	collectionID, agent, err := c.getKvProviderAndID(ctx)
	if err != nil {
		return nil, err
	}

	spec := opts.spec
	if opts.WithExpiry {
		op := gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpGet,
			Path:  "$document.exptime",
			Flags: gocbcore.SubdocFlag(SubdocFlagXattr),
		}

		spec.ops = append([]gocbcore.SubDocOp{op}, spec.ops...)
	}

	if len(spec.ops) > 16 {
		spec = lookupSpec{}
		op := gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpGetDoc,
			Flags: gocbcore.SubdocFlag(spec.flags),
		}
		spec.ops = append(spec.ops, op)

		if opts.WithExpiry {
			op := gocbcore.SubDocOp{
				Op:    gocbcore.SubDocOpGet,
				Path:  "$document.exptime",
				Flags: gocbcore.SubdocFlag(SubdocFlagXattr),
			}

			spec.ops = append([]gocbcore.SubDocOp{op}, spec.ops...)
		}
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.Wait(agent.LookupInEx(gocbcore.LookupInOptions{
		Key:          []byte(key),
		Flags:        spec.flags,
		Ops:          spec.ops,
		CollectionID: collectionID,
		TraceContext: traceCtx,
	}, func(res *gocbcore.LookupInResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}
			errOut = err
			ctrl.Resolve()
			return
		}

		if res != nil {
			resSet := &LookupInResult{}
			resSet.cas = Cas(res.Cas)
			resSet.contents = make([]lookupInPartial, len(spec.ops))

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

		ctrl.Resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

type mutateSpec struct {
	ops   []gocbcore.SubDocOp
	flags gocbcore.SubdocDocFlag
	// errs  []error
}

// MutateInOptions are the set of options available to MutateIn.
type MutateInOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	Cas               Cas
	PersistTo         uint
	ReplicateTo       uint
	spec              mutateSpec
}

func (opts *MutateInOptions) marshalValue(value interface{}) []byte {
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

// Insert inserts a value at the specified path within the document, optionally creating the document first.
func (opts MutateInOptions) Insert(path string, val interface{}, createParents bool) MutateInOptions {
	var flags SubdocFlag
	if createParents {
		flags |= SubdocFlagCreatePath
	}

	return opts.insertWithFlags(path, val, flags)
}

func (opts MutateInOptions) insertWithFlags(path string, val interface{}, flags SubdocFlag) MutateInOptions {
	if path == "" {
		op := gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpAddDoc,
			Flags: gocbcore.SubdocFlag(flags),
			Value: opts.marshalValue(val),
		}
		opts.spec.ops = append(opts.spec.ops, op)
		return opts
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpDictAdd,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: opts.marshalValue(val),
	}
	opts.spec.ops = append(opts.spec.ops, op)
	return opts
}

// Replace replaces the value of the field at path.
func (opts MutateInOptions) Replace(path string, val interface{}) MutateInOptions {
	return opts.replaceWithFlags(path, val, SubdocFlagNone)
}

func (opts MutateInOptions) replaceWithFlags(path string, val interface{}, flags SubdocFlag) MutateInOptions {
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpReplace,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: opts.marshalValue(val),
	}
	opts.spec.ops = append(opts.spec.ops, op)
	return opts
}

// Mutate performs a set of subdocument mutations on the document specified by key.
func (c *Collection) Mutate(key string, opts MutateInOptions) (mutOut *StoreResult, errOut error) {
	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	span := c.startKvOpTrace(opts.ParentSpanContext, "MutateIn")
	defer span.Finish()

	collectionID, agent, err := c.getKvProviderAndID(deadlinedCtx)
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.MutateInEx(gocbcore.MutateInOptions{
		Key:          []byte(key),
		Flags:        opts.spec.flags,
		Cas:          gocbcore.Cas(opts.Cas),
		CollectionID: collectionID,
		Ops:          opts.spec.ops,
		TraceContext: span.Context(),
	}, func(res *gocbcore.MutateInResult, err error) {
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
		mutRes := &StoreResult{
			mt: mutTok,
		}
		mutRes.cas = Cas(res.Cas)
		mutOut = mutRes

		ctrl.Resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// GetAndTouchOptions are the options available to the GetAndTouch operation.
type GetAndTouchOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	ExpireAt          time.Time
}

// GetAndTouch retrieves a document and simultaneously updates its expiry time.
func (c *Collection) GetAndTouch(key string, opts *GetAndTouchOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetAndTouchOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "GetAndTouch")
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

	var expiry uint32
	if opts.ExpireAt.IsZero() {
		expiry = 0
	} else {
		expiry = uint32(opts.ExpireAt.Unix())
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.GetAndTouchEx(gocbcore.GetAndTouchOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Expiry:       expiry,
		TraceContext: span.Context(),
	}, func(res *gocbcore.GetAndTouchResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}

			errOut = err
			ctrl.Resolve()
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

		ctrl.Resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// GetAndLockOptions are the options available to the GetAndLock operation.
type GetAndLockOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	LockTime          uint32
}

// GetAndLock locks a document for a period of time, providing exclusive RW access to it.
func (c *Collection) GetAndLock(key string, opts *GetAndLockOptions) (docOut *GetResult, errOut error) {
	if opts == nil {
		opts = &GetAndLockOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "GetAndLock")
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
	err = ctrl.Wait(agent.GetAndLockEx(gocbcore.GetAndLockOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		LockTime:     opts.LockTime,
		TraceContext: span.Context(),
	}, func(res *gocbcore.GetAndLockResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}

			errOut = err
			ctrl.Resolve()
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

		ctrl.Resolve()
	}))
	if err != nil {
		errOut = err
	}

	return
}

// UnlockOptions are the options available to the GetAndLock operation.
type UnlockOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	Cas               Cas
}

// Unlock unlocks a document which was locked with GetAndLock.
func (c *Collection) Unlock(key string, opts *UnlockOptions) (mutOut *StoreResult, errOut error) {
	if opts == nil {
		opts = &UnlockOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Unlock")
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
	err = ctrl.Wait(agent.UnlockEx(gocbcore.UnlockOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Cas:          gocbcore.Cas(opts.Cas),
		TraceContext: span.Context(),
	}, func(res *gocbcore.UnlockResult, err error) {
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
		mutOut = &StoreResult{
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

// TouchOptions are the options available to the Touch operation.
type TouchOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	ExpireAt          time.Time
}

// Touch touches a document, specifying a new expiry time for it.
// The Cas value must be 0.
func (c *Collection) Touch(key string, opts *GetAndTouchOptions) (mutOut *StoreResult, errOut error) {
	if opts == nil {
		opts = &GetAndTouchOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Touch")
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

	var expiry uint32
	if opts.ExpireAt.IsZero() {
		expiry = 0
	} else {
		expiry = uint32(opts.ExpireAt.Unix())
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.TouchEx(gocbcore.TouchOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Expiry:       expiry,
		TraceContext: span.Context(),
	}, func(res *gocbcore.TouchResult, err error) {
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
		mutOut = &StoreResult{
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
	Delta             int64
}

// Counter performs an atomic addition or subtraction for an integer document.  Passing a
// non-negative `initial` value will cause the document to be created if it did  not
// already exist.
func (c *Collection) Counter(key string, opts *CounterOptions) (mutOut *StoreResult, errOut error) {
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

	if opts.Delta > 0 {
		return c.counterInc(deadlinedCtx, span.Context(), key, uint64(opts.Delta), realInitial, expiry)
	} else if opts.Delta < 0 {
		return c.counterDec(deadlinedCtx, span.Context(), key, uint64(-opts.Delta), realInitial, expiry)
	} else {
		errOut = errors.New("delta must be a non-zero value") //TODO
		return
	}
}

func (c *Collection) counterInc(ctx context.Context, tracectx opentracing.SpanContext, key string,
	delta, initial uint64, expiry uint32) (mutOut *StoreResult, errOut error) {

	collectionID, agent, err := c.getKvProviderAndID(ctx)
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.Wait(agent.IncrementEx(gocbcore.CounterOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Delta:        delta,
		Initial:      initial,
		Expiry:       expiry,
		TraceContext: tracectx,
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
		mutOut = &StoreResult{
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

func (c *Collection) counterDec(ctx context.Context, tracectx opentracing.SpanContext, key string,
	delta, initial uint64, expiry uint32) (mutOut *StoreResult, errOut error) {

	collectionID, agent, err := c.getKvProviderAndID(ctx)
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.Wait(agent.DecrementEx(gocbcore.CounterOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Delta:        delta,
		Initial:      initial,
		Expiry:       expiry,
		TraceContext: tracectx,
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
		mutOut = &StoreResult{
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

// AppendOptions are the options available to the BinaryAppend operation.
type AppendOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// BinaryAppend appends a byte value to a document.
func (c *Collection) BinaryAppend(key string, val []byte, opts *AppendOptions) (mutOut *StoreResult, errOut error) {
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
		mutOut = &StoreResult{
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

// PrependOptions are the options available to the BinaryPrepend operation.
type PrependOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// BinaryPrepend prepends a byte value to a document.
func (c *Collection) BinaryPrepend(key string, val []byte, opts *PrependOptions) (mutOut *StoreResult, errOut error) {
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
		mutOut = &StoreResult{
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

// 	collectionID, agent, err := c.getKvProviderAndID(deadlinedCtx)
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
