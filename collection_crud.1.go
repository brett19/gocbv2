package gocb

// import (
// 	"context"
// 	"encoding/json"
// 	"log"
// 	"time"

// 	opentracing "github.com/opentracing/opentracing-go"
// 	gocbcore "gopkg.in/couchbase/gocbcore.v7"
// )

// // TODO: Need to handle timeouts here.  It might be neccessary to move
// // timeout handling down into gocbcore, but that is still uncertain.

// type SetOperation interface {
// 	name() string
// 	ctx() context.Context
// 	// execute(key string, value interface{}, agent gocbcore.Agent, uint32 collectionID, transcoder Transcoder) (mutOut MutationResult, errOut error)
// }

// type SetOptions struct {
// 	ExpireAt time.Time
// 	Context  context.Context
// }

// type SetFieldsOperation struct {
// 	ops   []gocbcore.SubDocOp
// 	flags gocbcore.SubdocDocFlag
// 	errs  []error
// 	Cas   Cas
// }

// type setResult struct {
// 	res *MutationResult
// 	err error
// }

// type UpsertOperation struct {
// 	ExpireAt time.Time
// 	Context  context.Context
// }

// func (uo UpsertOperation) name() string {
// 	return "Upsert"
// }

// func (uo UpsertOperation) ctx() context.Context {
// 	return uo.Context
// }

// type InsertOperation struct {
// 	ExpireAt time.Time
// 	Context  context.Context
// }

// func (io InsertOperation) name() string {
// 	return "Insert"
// }

// func (io InsertOperation) ctx() context.Context {
// 	return io.Context
// }

// type ReplaceOperation struct {
// 	ExpireAt time.Time
// 	Context  context.Context
// 	Cas      Cas
// }

// func (ro ReplaceOperation) name() string {
// 	return "Replace"
// }

// func (ro ReplaceOperation) ctx() context.Context {
// 	return ro.Context
// }

// type RemoveOperation struct {
// 	Context context.Context
// 	Cas     Cas
// }

// func (ro RemoveOperation) name() string {
// 	return "Remove"
// }

// func (ro RemoveOperation) ctx() context.Context {
// 	return ro.Context
// }

// func (c *Collection) Mutate(key string, value interface{}, operation SetOperation) (mutOut *MutationResult, errOut error) {
// 	ctx := operation.ctx()
// 	if operation.ctx() == nil {
// 		ctx = context.Background()
// 	}
// 	span := c.startKvOpTrace(ctx, operation.name())
// 	defer span.Finish()
// 	ctx = context.WithValue(ctx, TracerCtxKey{}, span.Context())

// 	waitCh := make(chan setResult)
// 	var op gocbcore.PendingOp
// 	var err error
// 	switch opType := operation.(type) {
// 	case InsertOperation:
// 		op, err = c.insert(ctx, key, value, opType, waitCh)
// 	case UpsertOperation:
// 		op, err = c.upsert(ctx, key, value, opType, waitCh)
// 	case ReplaceOperation:
// 		op, err = c.replace(ctx, key, value, opType, waitCh)
// 	case RemoveOperation:
// 		op, err = c.remove(ctx, key, opType, waitCh)
// 	}
// 	if err != nil {
// 		errOut = err
// 		return
// 	}

// 	timeoutTmr := gocbcore.AcquireTimer(c.sb.KvTimeout)
// 	select {
// 	case <-ctx.Done():
// 		op.Cancel()
// 		errOut = ctx.Err()
// 		return
// 	case <-timeoutTmr.C:
// 		op.Cancel()
// 		// errOut = ErrTimeout
// 		return
// 	case result := <-waitCh:
// 		mutOut = result.res
// 		errOut = result.err
// 	}

// 	return
// }

// func (c *Collection) insert(ctx context.Context, key string, value interface{}, operation InsertOperation, waitCh chan setResult) (gocbcore.PendingOp, error) {
// 	var expiry uint32
// 	if operation.ExpireAt.IsZero() {
// 		expiry = 0
// 	} else {
// 		expiry = uint32(operation.ExpireAt.Unix())
// 	}

// 	log.Printf("Fetching Agent")

// 	collectionID, agent, err := c.getAgentAndCollection()
// 	if err != nil {
// 		return nil, err
// 	}

// 	log.Printf("Transcoding")

// 	tdoc, flags, err := c.transcoder.Encode(value)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return agent.AddEx(gocbcore.AddOptions{
// 		Key:          []byte(key),
// 		CollectionID: collectionID,
// 		Value:        tdoc,
// 		Flags:        flags,
// 		Expiry:       expiry,
// 		TraceContext: ctx.Value(TracerCtxKey{}).(opentracing.SpanContext),
// 	}, func(res *gocbcore.StoreResult, err error) {
// 		var mutRes *MutationResult
// 		if err != nil {
// 			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
// 				c.setCollectionUnknown()
// 			}
// 		}

// 		if res != nil {
// 			mutTok := MutationToken{
// 				token:      res.MutationToken,
// 				bucketName: c.sb.BucketName,
// 			}
// 			mutRes = &MutationResult{
// 				mt: mutTok,
// 			}
// 			mutRes.cas = Cas(res.Cas)
// 		}

// 		waitCh <- setResult{
// 			res: mutRes,
// 			err: err,
// 		}
// 	})
// }

// func (c *Collection) upsert(ctx context.Context, key string, value interface{}, operation UpsertOperation, waitCh chan setResult) (gocbcore.PendingOp, error) {
// 	var expiry uint32
// 	if operation.ExpireAt.IsZero() {
// 		expiry = 0
// 	} else {
// 		expiry = uint32(operation.ExpireAt.Unix())
// 	}

// 	log.Printf("Fetching Agent")

// 	collectionID, agent, err := c.getAgentAndCollection()
// 	if err != nil {
// 		return nil, err
// 	}

// 	log.Printf("Transcoding")

// 	tdoc, flags, err := c.transcoder.Encode(value)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return agent.SetEx(gocbcore.SetOptions{
// 		Key:          []byte(key),
// 		CollectionID: collectionID,
// 		Value:        tdoc,
// 		Flags:        flags,
// 		Expiry:       expiry,
// 		TraceContext: ctx.Value(TracerCtxKey{}).(opentracing.SpanContext),
// 	}, func(res *gocbcore.StoreResult, err error) {
// 		var mutRes *MutationResult
// 		if err != nil {
// 			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
// 				c.setCollectionUnknown()
// 			}
// 		}

// 		if res != nil {
// 			mutTok := MutationToken{
// 				token:      res.MutationToken,
// 				bucketName: c.sb.BucketName,
// 			}
// 			mutRes = &MutationResult{
// 				mt: mutTok,
// 			}
// 			mutRes.cas = Cas(res.Cas)
// 		}

// 		waitCh <- setResult{
// 			res: mutRes,
// 			err: err,
// 		}
// 	})
// }

// func (c *Collection) replace(ctx context.Context, key string, value interface{}, operation ReplaceOperation, waitCh chan setResult) (gocbcore.PendingOp, error) {
// 	var expiry uint32
// 	if operation.ExpireAt.IsZero() {
// 		expiry = 0
// 	} else {
// 		expiry = uint32(operation.ExpireAt.Unix())
// 	}

// 	log.Printf("Fetching Agent")

// 	collectionID, agent, err := c.getAgentAndCollection()
// 	if err != nil {
// 		return nil, err
// 	}

// 	log.Printf("Transcoding")

// 	tdoc, flags, err := c.transcoder.Encode(value)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return agent.ReplaceEx(gocbcore.ReplaceOptions{
// 		Key:          []byte(key),
// 		CollectionID: collectionID,
// 		Value:        tdoc,
// 		Flags:        flags,
// 		Expiry:       expiry,
// 		Cas:          gocbcore.Cas(operation.Cas),
// 		TraceContext: ctx.Value(TracerCtxKey{}).(opentracing.SpanContext),
// 	}, func(res *gocbcore.StoreResult, err error) {
// 		var mutRes *MutationResult
// 		if err != nil {
// 			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
// 				c.setCollectionUnknown()
// 			}
// 		}

// 		if res != nil {
// 			mutTok := MutationToken{
// 				token:      res.MutationToken,
// 				bucketName: c.sb.BucketName,
// 			}
// 			mutRes = &MutationResult{
// 				mt: mutTok,
// 			}
// 			mutRes.cas = Cas(res.Cas)
// 		}

// 		waitCh <- setResult{
// 			res: mutRes,
// 			err: err,
// 		}
// 	})
// }

// func (c *Collection) remove(ctx context.Context, key string, operation RemoveOperation, waitCh chan setResult) (gocbcore.PendingOp, error) {
// 	log.Printf("Fetching Agent")

// 	collectionID, agent, err := c.getAgentAndCollection()
// 	if err != nil {
// 		return nil, err
// 	}

// 	return agent.DeleteEx(gocbcore.DeleteOptions{
// 		Key:          []byte(key),
// 		CollectionID: collectionID,
// 		Cas:          gocbcore.Cas(operation.Cas),
// 		TraceContext: ctx.Value(TracerCtxKey{}).(opentracing.SpanContext),
// 	}, func(res *gocbcore.DeleteResult, err error) {
// 		var mutRes *MutationResult
// 		if err != nil {
// 			if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) {
// 				c.setCollectionUnknown()
// 			}
// 		}

// 		if res != nil {
// 			mutTok := MutationToken{
// 				token:      res.MutationToken,
// 				bucketName: c.sb.BucketName,
// 			}
// 			mutRes = &MutationResult{
// 				mt: mutTok,
// 			}
// 			mutRes.cas = Cas(res.Cas)
// 		}

// 		waitCh <- setResult{
// 			res: mutRes,
// 			err: err,
// 		}
// 	})
// }

// func (f *SetFieldsOperation) marshalValue(value interface{}) []byte {
// 	if value, ok := value.([]byte); ok {
// 		return value
// 	}

// 	if value, ok := value.(*[]byte); ok {
// 		return *value
// 	}

// 	bytes, err := json.Marshal(value)
// 	if err != nil {
// 		f.errs = append(f.errs, err)
// 		return nil
// 	}
// 	return bytes
// }

// // func (c *Collection) setFields(ctx context.Context, key string, value interface{}, operation SetFieldsOperation, waitCh chan setResult) (gocbcore.PendingOp, error) {
// // 	var expiry uint32
// // 	if operation.ExpireAt.IsZero() {
// // 		expiry = 0
// // 	} else {
// // 		expiry = uint32(operation.ExpireAt.Unix())
// // 	}

// // 	var flags gocbcore.SubdocFlag
// // 	// if createParents {
// // 	// 	flags |= SubdocFlagCreatePath
// // 	// }

// // 	collectionID, agent, err := c.getAgentAndCollection()
// // 	if err != nil {
// // 		return nil, err
// // 	}

// // 	op, err := agent.MutateInEx(gocbcore.MutateInOptions{
// // 		Key:    []byte(key),
// // 		Flags:  operation.flags,
// // 		Cas:    gocbcore.Cas(operation.Cas),
// // 		Expiry: expiry,
// // 		Ops:    operation.ops,
// // 	}, func(res *gocbcore.MutateInResult, err error) {
// // 		var mutRes *MutationResult
// // 		if res != nil {
// // 			mutTok := MutationToken{
// // 				token:      res.MutationToken,
// // 				bucketName: c.sb.BucketName,
// // 			}
// // 			mutRes = &MutationResult{
// // 				mt: mutTok,
// // 			}
// // 			mutRes.cas = Cas(res.Cas)
// // 			resSet.contents = make([]subDocResult, len(res.Ops))

// // 			for i, opRes := range res.Ops {
// // 				resSet.contents[i].path = set.ops[i].Path
// // 				resSet.contents[i].err = opRes.Err
// // 				if opRes.Value != nil {
// // 					resSet.contents[i].data = append([]byte(nil), opRes.Value...)
// // 				}
// // 			}

// // 			resOut = resSet
// // 		}

// // 		signal <- true
// // 	})
// // 	if err != nil {
// // 		return nil, err
// // 	}

// // 	return f
// // }

// type GetOptions struct {
// 	Context context.Context
// }

// type GetFields struct {
// 	ops   []gocbcore.SubDocOp
// 	flags gocbcore.SubdocDocFlag
// }

// type getResult struct {
// 	doc *DocumentProjection
// 	err error
// }

// func (f GetFields) Get(path string) GetFields {
// 	var op gocbcore.SubDocOp
// 	if path == "" {
// 		op = gocbcore.SubDocOp{
// 			Op:    gocbcore.SubDocOpGetDoc,
// 			Flags: gocbcore.SubdocFlagNone,
// 		}
// 	} else {
// 		op = gocbcore.SubDocOp{
// 			Op:    gocbcore.SubDocOpGet,
// 			Path:  path,
// 			Flags: gocbcore.SubdocFlagNone,
// 		}
// 	}
// 	f.ops = append(f.ops, op)

// 	return f
// }

// func (f GetFields) Exists(path string) GetFields {
// 	op := gocbcore.SubDocOp{
// 		Op:    gocbcore.SubDocOpExists,
// 		Path:  path,
// 		Flags: gocbcore.SubdocFlagNone,
// 	}
// 	f.ops = append(f.ops, op)

// 	return f
// }

// func (f GetFields) Count(path string) GetFields {
// 	op := gocbcore.SubDocOp{
// 		Op:    gocbcore.SubDocOpGetCount,
// 		Path:  path,
// 		Flags: gocbcore.SubdocFlagNone,
// 	}
// 	f.ops = append(f.ops, op)

// 	return f
// }

// func (c *Collection) Get(key string, fields *GetFields, opts *GetOptions) (docOut *DocumentProjection, errOut error) {
// 	if opts == nil {
// 		opts = &GetOptions{}
// 	}
// 	if opts.Context == nil {
// 		opts.Context = context.Background()
// 	}
// 	span := c.startKvOpTrace(opts.Context, "Get")
// 	defer span.Finish()
// 	ctx := context.WithValue(opts.Context, TracerCtxKey{}, span.Context())

// 	waitCh := make(chan getResult)

// 	var op gocbcore.PendingOp
// 	var err error
// 	if fields == nil || len(fields.ops) == 0 || len(fields.ops) > 16 {
// 		op, err = c.getFull(ctx, key, opts, waitCh)
// 	} else {
// 		op, err = c.getPartial(ctx, key, fields, opts, waitCh)
// 	}
// 	if err != nil {
// 		errOut = err
// 		return
// 	}

// 	timeoutTmr := gocbcore.AcquireTimer(c.sb.KvTimeout)
// 	select {
// 	case <-opts.Context.Done():
// 		op.Cancel()
// 		errOut = opts.Context.Err()
// 		return
// 	case <-timeoutTmr.C:
// 		op.Cancel()
// 		// errOut = ErrTimeout
// 		return
// 	case result := <-waitCh:
// 		docOut = result.doc
// 		errOut = result.err
// 	}

// 	return
// }

// func (c *Collection) getPartial(ctx context.Context, key string, fields *GetFields, opts *GetOptions, waitCh chan getResult) (gocbcore.PendingOp, error) {
// 	collectionID, agent, err := c.getAgentAndCollection()
// 	if err != nil {
// 		return nil, err
// 	}

// 	return agent.LookupInEx(gocbcore.LookupInOptions{
// 		Key:          []byte(key),
// 		Flags:        fields.flags,
// 		Ops:          fields.ops,
// 		CollectionID: collectionID,
// 		TraceContext: ctx.Value(TracerCtxKey{}).(opentracing.SpanContext),
// 	}, func(res *gocbcore.LookupInResult, err error) {
// 		var doc *DocumentProjection
// 		if res != nil {
// 			resSet := &DocumentProjection{}
// 			resSet.contents = make([]ProjectionResult, len(res.Ops))
// 			resSet.cas = Cas(res.Cas)

// 			for i, opRes := range res.Ops {
// 				resSet.contents[i].path = fields.ops[i].Path
// 				resSet.contents[i].err = opRes.Err
// 				if opRes.Value != nil {
// 					resSet.contents[i].data = append([]byte(nil), opRes.Value...)
// 				}
// 			}

// 			doc = resSet
// 		}

// 		waitCh <- getResult{
// 			doc: doc,
// 			err: err,
// 		}
// 	})
// }

// func (c *Collection) getFull(ctx context.Context, key string, opts *GetOptions, waitCh chan getResult) (gocbcore.PendingOp, error) {
// 	collectionID, agent, err := c.getAgentAndCollection()
// 	if err != nil {
// 		return nil, err
// 	}

// 	return agent.GetEx(gocbcore.GetOptions{
// 		Key:          []byte(key),
// 		CollectionID: collectionID,
// 		TraceContext: ctx.Value(TracerCtxKey{}).(opentracing.SpanContext),
// 	}, func(res *gocbcore.GetResult, err error) {
// 		var doc *DocumentProjection
// 		if res != nil {
// 			doc = &DocumentProjection{}
// 			doc.contents = make([]ProjectionResult, 1)
// 			doc.contents[0] = ProjectionResult{path: "", data: res.Value}
// 			doc.cas = Cas(res.Cas)
// 		}

// 		waitCh <- getResult{
// 			doc: doc,
// 			err: err,
// 		}
// 	})
// }

// func (c *Collection) GetAndTouch(key string, opts *TouchOptions) (docOut *Document, errOut error) {
// 	if opts == nil {
// 		opts = &TouchOptions{}
// 	}

// 	collectionID, agent, err := c.getAgentAndCollection()
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

// 		docOut = &Document{
// 			bytes: res.Value,
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

// func (c *Collection) GetAndLock(key string, opts *GetAndLockOptions) (docOut *Document, errOut error) {
// 	if opts == nil {
// 		opts = &GetAndLockOptions{}
// 	}

// 	collectionID, agent, err := c.getAgentAndCollection()
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

// 		docOut = &Document{
// 			bytes: res.Value,
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

// 	collectionID, agent, err := c.getAgentAndCollection()
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

// func (c *Collection) GetReplica(key string, opts *GetReplicaOptions) (docOut *Document, errOut error) {
// 	if opts == nil {
// 		opts = &GetReplicaOptions{}
// 	}

// 	collectionID, agent, err := c.getAgentAndCollection()
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

// 		docOut = &Document{
// 			bytes: res.Value,
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

// 	collectionID, agent, err := c.getAgentAndCollection()
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
