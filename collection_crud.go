package gocb

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/pkg/errors"

	opentracing "github.com/opentracing/opentracing-go"
	gocbcore "gopkg.in/couchbase/gocbcore.v7"
)

// TODO: Need to handle timeouts here.  It might be neccessary to move
// timeout handling down into gocbcore, but that is still uncertain.

type SetOptions struct {
	Context        context.Context
	ExpireAt       time.Time
	Encode         Encode
	PersistTo      uint
	ReplicateTo    uint
	Timeout        time.Duration
	WithDurability DurabilityLevel
}

func (c *Collection) Insert(key string, val interface{}, opts *SetOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &SetOptions{}
	}
	if opts.Context == nil {
		opts.Context = context.Background()
	}
	if opts.Encode == nil {
		opts.Encode = DefaultEncode
	}
	span := c.startKvOpTrace(opts.Context, "Insert")
	defer span.Finish()

	var expiry uint32
	if opts.ExpireAt.IsZero() {
		expiry = 0
	} else {
		expiry = uint32(opts.ExpireAt.Unix())
	}

	log.Printf("Fetching Agent")

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return nil, err
	}

	log.Printf("Transcoding")

	bytes, flags, err := opts.Encode(val)
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

	timeout := opts.Timeout
	if timeout > c.sb.KvTimeout || timeout == 0 {
		timeout = c.sb.KvTimeout
	}
	timeoutTmr := gocbcore.AcquireTimer(timeout)
	select {
	case <-opts.Context.Done():
		gocbcore.ReleaseTimer(timeoutTmr, false)
		op.Cancel()
		errOut = opts.Context.Err()
		return
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		op.Cancel()
		// errOut = ErrTimeout
		return
	case <-waitCh:
		gocbcore.ReleaseTimer(timeoutTmr, false)
	}

	return
}

func (c *Collection) Upsert(key string, val interface{}, opts *SetOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &SetOptions{}
	}
	if opts.Context == nil {
		opts.Context = context.Background()
	}
	if opts.Encode == nil {
		opts.Encode = DefaultEncode
	}
	span := c.startKvOpTrace(opts.Context, "Upsert")
	defer span.Finish()

	var expiry uint32
	if opts.ExpireAt.IsZero() {
		expiry = 0
	} else {
		expiry = uint32(opts.ExpireAt.Unix())
	}

	log.Printf("Fetching Agent")

	collectionID, agent, err := c.getAgentAndCollection()
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

	timeout := opts.Timeout
	if timeout > c.sb.KvTimeout || timeout == 0 {
		timeout = c.sb.KvTimeout
	}
	timeoutTmr := gocbcore.AcquireTimer(timeout)
	select {
	case <-opts.Context.Done():
		gocbcore.ReleaseTimer(timeoutTmr, false)
		op.Cancel()
		errOut = opts.Context.Err()
		return
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		op.Cancel()
		// errOut = ErrTimeout
		return
	case <-waitCh:
		gocbcore.ReleaseTimer(timeoutTmr, false)
	}

	return
}

type ReplaceOptions struct {
	Context        context.Context
	ExpireAt       time.Time
	Cas            Cas
	Encode         Encode
	PersistTo      uint
	ReplicateTo    uint
	Timeout        time.Duration
	WithDurability DurabilityLevel
}

func (c *Collection) Replace(key string, val interface{}, opts *ReplaceOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &ReplaceOptions{}
	}
	if opts.Context == nil {
		opts.Context = context.Background()
	}
	if opts.Encode == nil {
		opts.Encode = DefaultEncode
	}
	span := c.startKvOpTrace(opts.Context, "Replace")
	defer span.Finish()

	var expiry uint32
	if opts.ExpireAt.IsZero() {
		expiry = 0
	} else {
		expiry = uint32(opts.ExpireAt.Unix())
	}

	log.Printf("Fetching Agent")

	collectionID, agent, err := c.getAgentAndCollection()
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

	timeout := opts.Timeout
	if timeout > c.sb.KvTimeout || timeout == 0 {
		timeout = c.sb.KvTimeout
	}
	timeoutTmr := gocbcore.AcquireTimer(timeout)
	select {
	case <-opts.Context.Done():
		gocbcore.ReleaseTimer(timeoutTmr, false)
		op.Cancel()
		errOut = opts.Context.Err()
		return
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		op.Cancel()
		// errOut = ErrTimeout
		return
	case <-waitCh:
		gocbcore.ReleaseTimer(timeoutTmr, false)
	}

	return
}

type GetOptions struct {
	Context    context.Context
	WithExpiry bool
	Timeout    time.Duration
}

type getResult struct {
	doc *Document
	err error
}

func (c *Collection) Get(key string, opts *GetOptions) (docOut *Document, errOut error) {
	if opts == nil {
		opts = &GetOptions{}
	}
	if opts.Context == nil {
		opts.Context = context.Background()
	}
	span := c.startKvOpTrace(opts.Context, "Get")
	defer span.Finish()
	ctx := context.WithValue(opts.Context, TracerSpanCtxKey{}, span.Context())

	if opts.WithExpiry {
		spec := LookupSpec{}.GetWithFlags("$document.exptime", SubdocFlagXattr).Get("")
		lookin, err := c.LookupIn(key, spec, &LookupOptions{Context: ctx})
		if err != nil {
			errOut = err
			return
		}
		doc := &Document{}
		lookin.ContentByIndex(0).As(&doc.expireAt)
		doc.bytes = lookin.ContentByIndex(1).data
		doc.cas = lookin.cas
		docOut = doc
		return
	}

	docOut, errOut = c.get(ctx, key, opts)
	return
}

func (c *Collection) get(ctx context.Context, key string, opts *GetOptions) (docOut *Document, errOut error) {
	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{})
	op, err := agent.GetEx(gocbcore.GetOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		TraceContext: ctx.Value(TracerSpanCtxKey{}).(opentracing.SpanContext),
	}, func(res *gocbcore.GetResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}

			errOut = err
			waitCh <- struct{}{}
			return
		}
		var doc *Document
		if res != nil {
			doc = &Document{
				id:    key,
				bytes: res.Value,
				flags: res.Flags,
				cas:   Cas(res.Cas),
			}

			docOut = doc
		}

		waitCh <- struct{}{}
	})
	if errOut != nil {
		return nil, err
	}

	timeout := opts.Timeout
	if timeout > c.sb.KvTimeout || timeout == 0 {
		timeout = c.sb.KvTimeout
	}
	timeoutTmr := gocbcore.AcquireTimer(timeout)
	select {
	case <-opts.Context.Done():
		gocbcore.ReleaseTimer(timeoutTmr, false)
		op.Cancel()
		errOut = opts.Context.Err()
		return
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		op.Cancel()
		// errOut = ErrTimeout
		return
	case <-waitCh:
		gocbcore.ReleaseTimer(timeoutTmr, false)
	}

	return
}

type RemoveOptions struct {
	Context        context.Context
	Cas            Cas
	PersistTo      uint
	ReplicateTo    uint
	Timeout        time.Duration
	WithDurability DurabilityLevel
}

func (c *Collection) Remove(key string, opts *RemoveOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &RemoveOptions{}
	}
	if opts.Context == nil {
		opts.Context = context.Background()
	}
	span := c.startKvOpTrace(opts.Context, "Remove")
	defer span.Finish()

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{})

	op, err := agent.DeleteEx(gocbcore.DeleteOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Cas:          gocbcore.Cas(opts.Cas),
		TraceContext: span.Context(),
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

	timeout := opts.Timeout
	if timeout > c.sb.KvTimeout || timeout == 0 {
		timeout = c.sb.KvTimeout
	}
	timeoutTmr := gocbcore.AcquireTimer(timeout)
	select {
	case <-opts.Context.Done():
		gocbcore.ReleaseTimer(timeoutTmr, false)
		op.Cancel()
		errOut = opts.Context.Err()
		return
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		op.Cancel()
		// errOut = ErrTimeout
		return
	case <-waitCh:
		gocbcore.ReleaseTimer(timeoutTmr, false)
	}

	return
}

type LookupSpec struct {
	ops   []gocbcore.SubDocOp
	flags gocbcore.SubdocDocFlag
}

type LookupOptions struct {
	Context context.Context
	Timeout time.Duration
}

func (spec LookupSpec) Get(path string) LookupSpec {
	return spec.GetWithFlags(path, SubdocFlagNone)
}

func (spec LookupSpec) GetWithFlags(path string, flags SubdocFlag) LookupSpec {
	if path == "" {
		op := gocbcore.SubDocOp{
			Op:    gocbcore.SubDocOpGetDoc,
			Flags: gocbcore.SubdocFlag(flags),
		}
		spec.ops = append(spec.ops, op)
		return spec
	}

	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpGet,
		Path:  path,
		Flags: gocbcore.SubdocFlag(flags),
	}
	spec.ops = append(spec.ops, op)
	return spec
}

func (f LookupSpec) Exists(path string) LookupSpec {
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpExists,
		Path:  path,
		Flags: gocbcore.SubdocFlagNone,
	}
	f.ops = append(f.ops, op)

	return f
}

func (f LookupSpec) Count(path string) LookupSpec {
	op := gocbcore.SubDocOp{
		Op:    gocbcore.SubDocOpGetCount,
		Path:  path,
		Flags: gocbcore.SubdocFlagNone,
	}
	f.ops = append(f.ops, op)

	return f
}

func (c *Collection) LookupIn(key string, spec LookupSpec, opts *LookupOptions) (lookOut *DocumentProjection, errOut error) {
	if opts == nil {
		opts = &LookupOptions{}
	}
	if opts.Context == nil {
		opts.Context = context.Background()
	}
	span := c.startKvOpTrace(opts.Context, "LookupIn")
	defer span.Finish()

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{})

	op, err := agent.LookupInEx(gocbcore.LookupInOptions{
		Key:          []byte(key),
		Flags:        spec.flags,
		Ops:          spec.ops,
		CollectionID: collectionID,
		TraceContext: span.Context(),
	}, func(res *gocbcore.LookupInResult, err error) {
		if err != nil {
			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
				c.setCollectionUnknown()
			}
			errOut = err
			waitCh <- struct{}{}
			return
		}

		resSet := &DocumentProjection{}
		resSet.contents = make([]ProjectionResult, len(res.Ops))
		resSet.cas = Cas(res.Cas)

		for i, opRes := range res.Ops {
			resSet.contents[i].path = spec.ops[i].Path
			resSet.contents[i].err = opRes.Err
			if opRes.Value != nil {
				resSet.contents[i].data = append([]byte(nil), opRes.Value...)
			}
		}

		lookOut = resSet

		waitCh <- struct{}{}
	})
	if err != nil {
		errOut = err
		return
	}

	timeout := opts.Timeout
	if timeout > c.sb.KvTimeout || timeout == 0 {
		timeout = c.sb.KvTimeout
	}
	timeoutTmr := gocbcore.AcquireTimer(timeout)
	select {
	case <-opts.Context.Done():
		gocbcore.ReleaseTimer(timeoutTmr, false)
		if !op.Cancel() {
			<-waitCh
			return
		}
		errOut = opts.Context.Err()
		return
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		if !op.Cancel() {
			<-waitCh
			return
		}
		errOut = errors.New("")
		// errOut = ErrTimeout
		return
	case <-waitCh:
		gocbcore.ReleaseTimer(timeoutTmr, false)
	}

	return
}

type MutateSpec struct {
	ops   []gocbcore.SubDocOp
	flags gocbcore.SubdocDocFlag
	// errs  []error
}

type MutateOptions struct {
	Context     context.Context
	Cas         Cas
	PersistTo   uint
	ReplicateTo uint
	Timeout     time.Duration
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

func (c *Collection) MutateIn(key string, spec MutateSpec, opts *MutateOptions) (mutOut *MutationResult, errOut error) {
	if opts == nil {
		opts = &MutateOptions{}
	}
	if opts.Context == nil {
		opts.Context = context.Background()
	}
	span := c.startKvOpTrace(opts.Context, "MutateIn")
	defer span.Finish()

	collectionID, agent, err := c.getAgentAndCollection()
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

	timeout := opts.Timeout
	if timeout > c.sb.KvTimeout || timeout == 0 {
		timeout = c.sb.KvTimeout
	}
	timeoutTmr := gocbcore.AcquireTimer(timeout)
	select {
	case <-opts.Context.Done():
		gocbcore.ReleaseTimer(timeoutTmr, false)
		op.Cancel()
		errOut = opts.Context.Err()
		return
	case <-timeoutTmr.C:
		gocbcore.ReleaseTimer(timeoutTmr, true)
		op.Cancel()
		// errOut = ErrTimeout
		return
	case <-waitCh:
		gocbcore.ReleaseTimer(timeoutTmr, false)
	}

	return
}

func (c *Collection) GetAndTouch(key string, opts *TouchOptions) (docOut *Document, errOut error) {
	if opts == nil {
		opts = &TouchOptions{}
	}

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{})

	agent.GetAndTouchEx(gocbcore.GetAndTouchOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Expiry:       opts.expiry,
	}, func(res *gocbcore.GetAndTouchResult, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		docOut = &Document{
			bytes: res.Value,
			flags: res.Flags,
			cas:   Cas(res.Cas),
		}
		waitCh <- struct{}{}
	})

	<-waitCh

	return
}

type GetAndLockOptions struct {
	lockTime uint32
}

func (opts GetAndLockOptions) LockTime(lockTime time.Time) GetAndLockOptions {
	opts.lockTime = uint32(lockTime.Unix())
	return opts
}

func (c *Collection) GetAndLock(key string, opts *GetAndLockOptions) (docOut *Document, errOut error) {
	if opts == nil {
		opts = &GetAndLockOptions{}
	}

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{})

	agent.GetAndLockEx(gocbcore.GetAndLockOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		LockTime:     opts.lockTime,
	}, func(res *gocbcore.GetAndLockResult, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		docOut = &Document{
			bytes: res.Value,
			flags: res.Flags,
			cas:   Cas(res.Cas),
		}
		waitCh <- struct{}{}
	})

	<-waitCh

	return
}

type UnlockOptions struct {
	cas Cas
}

func (opts UnlockOptions) Cas(cas Cas) UnlockOptions {
	opts.cas = cas
	return opts
}

func (c *Collection) Unlock(key string, opts *UnlockOptions) (errOut error) {
	if opts == nil {
		opts = &UnlockOptions{}
	}

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return err
	}

	waitCh := make(chan struct{})

	agent.UnlockEx(gocbcore.UnlockOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Cas:          gocbcore.Cas(opts.cas),
	}, func(res *gocbcore.UnlockResult, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		waitCh <- struct{}{}
	})

	<-waitCh

	return
}

type GetReplicaOptions struct {
	replicaIdx int
}

func (opts GetReplicaOptions) ReplicaIndex(replicaIdx int) GetReplicaOptions {
	opts.replicaIdx = replicaIdx
	return opts
}

func (c *Collection) GetReplica(key string, opts *GetReplicaOptions) (docOut *Document, errOut error) {
	if opts == nil {
		opts = &GetReplicaOptions{}
	}

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return nil, err
	}

	waitCh := make(chan struct{})

	agent.GetReplicaEx(gocbcore.GetReplicaOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		ReplicaIdx:   opts.replicaIdx,
	}, func(res *gocbcore.GetReplicaResult, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		docOut = &Document{
			bytes: res.Value,
			flags: res.Flags,
			cas:   Cas(res.Cas),
		}
		waitCh <- struct{}{}
	})

	<-waitCh

	return
}

type TouchOptions struct {
	expiry uint32
}

func (opts TouchOptions) ExpireAt(expiry time.Time) TouchOptions {
	opts.expiry = uint32(expiry.Unix())
	return opts
}

func (c *Collection) Touch(key string, opts *TouchOptions) (errOut error) {
	if opts == nil {
		opts = &TouchOptions{}
	}

	collectionID, agent, err := c.getAgentAndCollection()
	if err != nil {
		return err
	}

	waitCh := make(chan struct{})

	agent.TouchEx(gocbcore.TouchOptions{
		Key:          []byte(key),
		CollectionID: collectionID,
		Expiry:       opts.expiry,
	}, func(res *gocbcore.TouchResult, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		waitCh <- struct{}{}
	})

	<-waitCh

	return
}
