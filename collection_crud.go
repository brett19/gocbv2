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

type kvProvider interface {
	AddEx(opts gocbcore.AddOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error)
	SetEx(opts gocbcore.SetOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error)
	ReplaceEx(opts gocbcore.ReplaceOptions, cb gocbcore.StoreExCallback) (gocbcore.PendingOp, error)
	GetEx(opts gocbcore.GetOptions, cb gocbcore.GetExCallback) (gocbcore.PendingOp, error)
	GetReplicaEx(opts gocbcore.GetReplicaOptions, cb gocbcore.GetReplicaExCallback) (gocbcore.PendingOp, error)
	ObserveEx(opts gocbcore.ObserveOptions, cb gocbcore.ObserveExCallback) (gocbcore.PendingOp, error)
	ObserveVbEx(opts gocbcore.ObserveVbOptions, cb gocbcore.ObserveVbExCallback) (gocbcore.PendingOp, error)
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
	NumReplicas() int // UNSURE THIS SSHOULDNT BE ON ANOTHER INTERFACE
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
	Expiration        uint32
	Encode            Encode
	PersistTo         uint
	ReplicateTo       uint
	DurabilityLevel   DurabilityLevel
}

// InsertOptions are options that can be applied to an Insert operation.
type InsertOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	Expiration        uint32
	Encode            Encode
	PersistTo         uint
	ReplicateTo       uint
	DurabilityLevel   DurabilityLevel
}

// Insert creates a new document in the Collection.
func (c *Collection) Insert(key string, val interface{}, opts *InsertOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &InsertOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Insert")
	defer span.Finish()

	res, err := c.insert(span.Context(), key, val, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(opts.Context, span.Context(), key, res.Cas(), res.MutationToken(), opts.ReplicateTo, opts.PersistTo, false)

}
func (c *Collection) insert(traceCtx opentracing.SpanContext, key string, val interface{}, opts InsertOptions) (mutOut *MutationResult, errOut error) {
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

	agent, err := c.getKvProvider()
	if err != nil {
		errOut = err
		return
	}

	encodeSpan := opentracing.GlobalTracer().StartSpan("Encoding", opentracing.ChildOf(traceCtx))
	bytes, flags, err := encodeFn(val)
	if err != nil {
		errOut = err
		return
	}
	encodeSpan.Finish()

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.AddEx(gocbcore.AddOptions{
		Key:          []byte(key),
		CollectionID: c.collectionID(),
		Value:        bytes,
		Flags:        flags,
		Expiry:       opts.Expiration,
		TraceContext: traceCtx,
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

// Upsert creates a new document in the Collection if it does not exist, if it does exist then it updates it.
func (c *Collection) Upsert(key string, val interface{}, opts *UpsertOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &UpsertOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Upsert")
	defer span.Finish()

	res, err := c.upsert(span.Context(), key, val, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(opts.Context, span.Context(), key, res.Cas(), res.MutationToken(), opts.ReplicateTo, opts.PersistTo, false)
}

func (c *Collection) upsert(traceCtx opentracing.SpanContext, key string, val interface{}, opts UpsertOptions) (mutOut *MutationResult, errOut error) {
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

	log.Printf("Fetching Agent")

	agent, err := c.getKvProvider()
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
		CollectionID: c.collectionID(),
		Value:        bytes,
		Flags:        flags,
		Expiry:       opts.Expiration,
		TraceContext: traceCtx,
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
		mutOut = &MutationResult{
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
	Expiration        uint32
	Cas               Cas
	Encode            Encode
	PersistTo         uint
	ReplicateTo       uint
	DurabilityLevel   DurabilityLevel
}

// Replace updates a document in the collection.
func (c *Collection) Replace(key string, val interface{}, opts *ReplaceOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &ReplaceOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Replace")
	defer span.Finish()

	res, err := c.replace(span.Context(), key, val, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(opts.Context, span.Context(), key, res.Cas(), res.MutationToken(), opts.ReplicateTo, opts.PersistTo, false)
}

func (c *Collection) replace(traceCtx opentracing.SpanContext, key string, val interface{}, opts ReplaceOptions) (mutOut *MutationResult, errOut error) {
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

	log.Printf("Fetching Agent")

	agent, err := c.getKvProvider()
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
		CollectionID: c.collectionID(),
		Value:        bytes,
		Flags:        flags,
		Expiry:       opts.Expiration,
		Cas:          gocbcore.Cas(opts.Cas),
		TraceContext: traceCtx,
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
	doc.withExpiration = result.withExpiration
	doc.expiration = result.expiration
	doc.cas = result.cas
	doc.id = key
	err = doc.fromSubDoc(spec.spec.ops, result)
	if err != nil {
		errOut = err
		return
	}

	docOut = doc
	return
}

// get performs a full document fetch against the collection
func (c *Collection) get(ctx context.Context, traceCtx opentracing.SpanContext, key string, opts *GetOptions) (docOut *GetResult, errOut error) {
	span := c.startKvOpTrace(traceCtx, "get")
	defer span.Finish()

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(ctx)
	err = ctrl.Wait(agent.GetEx(gocbcore.GetOptions{
		Key:          []byte(key),
		CollectionID: c.collectionID(),
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

// ExistsOptions are the options available to the Exists command.
type ExistsOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// Exists checks if a document exists for the given key.
func (c *Collection) Exists(key string, opts *ExistsOptions) (docOut *ExistsResult, errOut error) {
	if opts == nil {
		opts = &ExistsOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Exists")
	defer span.Finish()

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.ObserveEx(gocbcore.ObserveOptions{
		Key:          []byte(key),
		CollectionID: c.collectionID(),
		TraceContext: span.Context(),
		ReplicaIdx:   0,
	}, func(res *gocbcore.ObserveResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}

			errOut = err
			ctrl.Resolve()
			return
		}
		if res != nil {
			doc := &ExistsResult{
				id:       key,
				cas:      Cas(res.Cas),
				keyState: res.KeyState,
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

// GetFromReplicaOptions are the options available to the GetFromReplica command.
type GetFromReplicaOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// GetFromReplica returns the value of a particular document from a replica server..
func (c *Collection) GetFromReplica(key string, replicaIdx int, opts *GetFromReplicaOptions) (docOut *GetResult, errOut error) { // TODO: ReplicaMode?
	if opts == nil {
		opts = &GetFromReplicaOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "GetFromReplica")
	defer span.Finish()

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.GetReplicaEx(gocbcore.GetReplicaOptions{
		Key:          []byte(key),
		CollectionID: c.collectionID(),
		TraceContext: span.Context(),
		ReplicaIdx:   0,
	}, func(res *gocbcore.GetReplicaResult, err error) {
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
	DurabilityLevel   DurabilityLevel
}

// Remove removes a document from the collection.
func (c *Collection) Remove(key string, opts *RemoveOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &RemoveOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "Remove")
	defer span.Finish()

	res, err := c.remove(span.Context(), key, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(opts.Context, span.Context(), key, res.Cas(), res.MutationToken(), opts.ReplicateTo, opts.PersistTo, false)
}

func (c *Collection) remove(traceCtx opentracing.SpanContext, key string, opts RemoveOptions) (mutOut *MutationResult, errOut error) {
	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.DeleteEx(gocbcore.DeleteOptions{
		Key:          []byte(key),
		CollectionID: c.collectionID(),
		Cas:          gocbcore.Cas(opts.Cas),
		TraceContext: traceCtx,
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

// Exists is similar to Path(), but does not actually retrieve the value from the server.
// This may save bandwidth if you only need to check for the existence of a
// path (without caring for its content). You can check the status of this
// operation by using .ContentAt (and ignoring the value) or .Exists() on the LookupResult.
func (opts LookupInOptions) Exists(path string) LookupInOptions {
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpExists,
		Path:  path,
		Flags: gocbcore.SubdocFlagNone,
	}
	opts.spec.ops = append(opts.spec.ops, op)

	return opts
}

// Count allows you to retrieve the number of items in an array or keys within an
// dictionary within an element of a document.
func (opts LookupInOptions) Count(path string) LookupInOptions {
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpGetCount,
		Path:  path,
		Flags: gocbcore.SubdocFlagNone,
	}
	opts.spec.ops = append(opts.spec.ops, op)

	return opts
}

// XAttr indicates an extended attribute to be retrieved from the document.  The value of the path
// can later be retrieved from the LookupResult.
// The path syntax follows N1QL's path syntax (e.g. `foo.bar.baz`).
func (opts LookupInOptions) XAttr(path string) LookupInOptions {
	return opts.getWithFlags(path, SubdocFlagXattr)
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
	span := c.startKvOpTrace(traceCtx, "lookupIn")
	defer span.Finish()

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	// Prepend the expiry get if required, xattrs have to be at the front of the ops list.
	spec := opts.spec
	if opts.WithExpiry {
		op := gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpGet,
			Path:  "$document.exptime",
			Flags: gocbcore.SubdocFlag(SubdocFlagXattr),
		}

		spec.ops = append([]gocbcore.SubDocOp{op}, spec.ops...)
	}

	// There is a 16 op limit to subdoc so if it's hit then do full doc.
	if len(spec.ops) > 16 {
		spec = lookupSpec{}
		op := gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpGetDoc,
			Flags: gocbcore.SubdocFlag(SubdocDocFlagNone),
		}
		spec.ops = append(spec.ops, op)

		// If required then expiry op must be re-prepended as the ops list has been reset.
		if opts.WithExpiry {
			op = gocbcore.SubDocOp{
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
		CollectionID: c.collectionID(),
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
				resSet.withExpiration = true
				err = resSet.ContentAt(0, &resSet.expiration)
				if err != nil {
					errOut = err
					ctrl.Resolve()
					return
				}
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
	flags SubdocDocFlag
	// errs  []error
}

// MutateInOptions are the set of options available to MutateIn.
type MutateInOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
	Expiration        uint32
	Cas               Cas
	PersistTo         uint
	ReplicateTo       uint
	DurabilityLevel   DurabilityLevel
	CreateDocument    bool
	spec              mutateSpec
}

func (opts *MutateInOptions) marshalValue(value interface{}) []byte {
	if val, ok := value.([]byte); ok {
		return val
	}

	if val, ok := value.(*[]byte); ok {
		return *val
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

// Upsert creates a new value at the specified path within the document if it does not exist, if it does exist then it
// updates it.
func (opts MutateInOptions) Upsert(path string, val interface{}, createParents bool) MutateInOptions {
	var flags SubdocFlag
	if createParents {
		flags |= SubdocFlagCreatePath
	}

	if path == "" {
		op := gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpSetDoc,
			Flags: gocbcore.SubdocFlag(flags),
			Value: opts.marshalValue(val),
		}
		opts.spec.ops = append(opts.spec.ops, op)
		return opts
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpDictSet,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: opts.marshalValue(val),
	}

	opts.spec.ops = append(opts.spec.ops, op)
	return opts
}

// Replace replaces the value of the field at path.
func (opts MutateInOptions) Replace(path string, val interface{}) MutateInOptions {
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpDelete,
		Path:  path,
		Flags: gocbcore.SubdocFlagNone,
		Value: opts.marshalValue(val),
	}

	opts.spec.ops = append(opts.spec.ops, op)
	return opts
}

// Remove removes the field at path.
func (opts MutateInOptions) Remove(path string) MutateInOptions {
	if path == "" {
		op := gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpDeleteDoc,
			Flags: gocbcore.SubdocFlagNone,
		}

		opts.spec.ops = append(opts.spec.ops, op)
		return opts
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpDelete,
		Path:  path,
		Flags: gocbcore.SubdocFlagNone,
	}

	opts.spec.ops = append(opts.spec.ops, op)
	return opts
}

// ArrayAppend adds an element to the end (i.e. right) of an array
func (opts *MutateInOptions) ArrayAppend(path string, bytes []byte, createParents bool) *MutateInOptions {
	var flags SubdocFlag
	if createParents {
		flags |= SubdocFlagCreatePath
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpArrayPushLast,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: bytes,
	}

	opts.spec.ops = append(opts.spec.ops, op)
	return opts
}

// ArrayPrepend adds an element to the beginning (i.e. left) of an array
func (opts *MutateInOptions) ArrayPrepend(path string, bytes []byte, createParents bool) *MutateInOptions {
	var flags SubdocFlag
	if createParents {
		flags |= SubdocFlagCreatePath
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpArrayPushFirst,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: bytes,
	}

	opts.spec.ops = append(opts.spec.ops, op)
	return opts
}

// ArrayInsert inserts an element at a given position within an array. The position should be
// specified as part of the path, e.g. path.to.array[3]
func (opts *MutateInOptions) ArrayInsert(path string, bytes []byte, createParents bool) *MutateInOptions {
	var flags SubdocFlag
	if createParents {
		flags |= SubdocFlagCreatePath
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpArrayInsert,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: bytes,
	}

	opts.spec.ops = append(opts.spec.ops, op)
	return opts
}

// ArrayAddUnique adds an dictionary add unique operation to this mutation operation set.
func (opts *MutateInOptions) ArrayAddUnique(path string, bytes []byte, createParents bool) *MutateInOptions {
	var flags SubdocFlag
	if createParents {
		flags |= SubdocFlagCreatePath
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpArrayAddUnique,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: bytes,
	}

	opts.spec.ops = append(opts.spec.ops, op)
	return opts
}

// Counter adds an counter operation to this mutation operation set.
func (opts *MutateInOptions) Counter(path string, delta int64, createParents bool) *MutateInOptions {
	var flags SubdocFlag
	if createParents {
		flags |= SubdocFlagCreatePath
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpCounter,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
		Value: opts.marshalValue(delta),
	}

	opts.spec.ops = append(opts.spec.ops, op)
	return opts
}

// Mutate performs a set of subdocument mutations on the document specified by key.
func (c *Collection) Mutate(key string, val interface{}, opts *MutateInOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &MutateInOptions{}
	}

	span := c.startKvOpTrace(opts.ParentSpanContext, "MutateIn")
	defer span.Finish()

	res, err := c.mutate(span.Context(), key, *opts)
	if err != nil {
		return nil, err
	}

	if opts.PersistTo == 0 && opts.ReplicateTo == 0 {
		return res, nil
	}
	return res, c.durability(opts.Context, span.Context(), key, res.Cas(), res.MutationToken(), opts.ReplicateTo, opts.PersistTo, false)
}

func (c *Collection) mutate(traceCtx opentracing.SpanContext, key string, opts MutateInOptions) (mutOut *MutationResult, errOut error) { // TODO: should return MutateInResult
	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := c.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	flags := opts.spec.flags
	if opts.CreateDocument {
		flags |= SubdocDocFlagMkDoc
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.MutateInEx(gocbcore.MutateInOptions{
		Key:          []byte(key),
		Flags:        gocbcore.SubdocDocFlag(flags),
		Cas:          gocbcore.Cas(opts.Cas),
		CollectionID: c.collectionID(),
		Ops:          opts.spec.ops,
		TraceContext: traceCtx,
		Expiry:       opts.Expiration,
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
		mutRes := &MutationResult{
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
}

// GetAndTouch retrieves a document and simultaneously updates its expiry time.
func (c *Collection) GetAndTouch(key string, expiration uint32, opts *GetAndTouchOptions) (docOut *GetResult, errOut error) {
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

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.GetAndTouchEx(gocbcore.GetAndTouchOptions{
		Key:          []byte(key),
		CollectionID: c.collectionID(),
		Expiry:       expiration,
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
}

// GetAndLock locks a document for a period of time, providing exclusive RW access to it.
func (c *Collection) GetAndLock(key string, expiration uint32, opts *GetAndLockOptions) (docOut *GetResult, errOut error) {
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

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.GetAndLockEx(gocbcore.GetAndLockOptions{
		Key:          []byte(key),
		CollectionID: c.collectionID(),
		LockTime:     expiration,
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
func (c *Collection) Unlock(key string, opts *UnlockOptions) (mutOut *MutationResult, errOut error) {
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

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.UnlockEx(gocbcore.UnlockOptions{
		Key:          []byte(key),
		CollectionID: c.collectionID(),
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

// TouchOptions are the options available to the Touch operation.
type TouchOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

// Touch touches a document, specifying a new expiry time for it.
// The Cas value must be 0.
func (c *Collection) Touch(key string, expiration uint32, opts *GetAndTouchOptions) (mutOut *MutationResult, errOut error) {
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

	agent, err := c.getKvProvider()
	if err != nil {
		return nil, err
	}

	ctrl := c.newOpManager(deadlinedCtx)
	err = ctrl.Wait(agent.TouchEx(gocbcore.TouchOptions{
		Key:          []byte(key),
		CollectionID: c.collectionID(),
		Expiry:       expiration,
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

// Binary creates and returns a CollectionBinary object.
func (c *Collection) Binary() *CollectionBinary {
	return &CollectionBinary{c}
}
