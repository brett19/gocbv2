package gocb

import (
	"context"
	"errors"
	"sync"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	gocbcore "gopkg.in/couchbase/gocbcore.v7"
)

// type Collection interface {
// 	// Name() string
// 	Get(id string, spec *GetSpec, opts *GetOptions) (*GetResult, error)
// 	// Append(id string, val interface{}, opts *AppendOptions) (*MutationResult, error)
// 	// Prepend(id string, val interface{}, opts *PrependOptions) (*MutationResult, error)
// 	// Touch(id string, opts *TouchOptions) (*MutationResult, error)
// 	// Unlock(id string, opts *UnlockOptions) (*MutationResult, error)
// 	// Increment(id string, opts *IncrementOptions) (*MutationResult, error)
// 	// Decrement(id string, opts *DecrementOptions) (*MutationResult, error)
// 	Upsert(id string, val interface{}, opts *UpsertOptions) (*MutationResult, error)
// 	Insert(id string, val interface{}, opts *InsertOptions) (*MutationResult, error)
// 	Replace(id string, val interface{}, opts *ReplaceOptions) (*MutationResult, error)
// 	Remove(id string, opts *RemoveOptions) (*MutationResult, error)
// 	Mutate(id string, spec MutateSpec, opts *MutateOptions) (*MutationResult, error)

// 	SetKvTimeout(duration time.Duration) Collection
// }

type Collection struct {
	sb   stateBlock
	csb  *collectionStateBlock
	lock sync.Mutex
}

type CollectionOptions struct {
	ParentSpanContext opentracing.SpanContext
	Timeout           time.Duration
	Context           context.Context
}

func (c *Collection) setCollectionID(collectionID uint32) error {
	if c.initialized() {
		return errors.New("collection already initialized")
	}

	c.lock.Lock()
	c.csb.CollectionInitialized = true
	c.csb.CollectionID = collectionID
	// c.csb.ScopeID = scopeID
	c.lock.Unlock()

	return nil
}

func (c *Collection) collectionID() uint32 {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.csb.CollectionID
}

func (c *Collection) initialized() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.csb.CollectionInitialized
}

func (c *Collection) setCollectionUnknown() {
	c.csb.CollectionUnknown = true
}

func (c *Collection) setScopeUnknown() {
	c.csb.ScopeUnknown = true
}

func (c *Collection) scopeUnknown() bool {
	return c.csb.ScopeUnknown
}

func (c *Collection) collectionUnknown() bool {
	return c.csb.CollectionUnknown
}

func newCollection(scope *Scope, collectionName string, opts *CollectionOptions) (*Collection, error) {
	if opts == nil {
		opts = &CollectionOptions{}
	}

	collection := &Collection{
		sb:  scope.stateBlock(),
		csb: &collectionStateBlock{},
	}
	collection.sb.CollectionName = collectionName
	collection.sb.KvTimeout = 10 * time.Second
	collection.sb.recacheClient()

	span := collection.startKvOpTrace(opts.ParentSpanContext, "GetCollectionID")
	defer span.Finish()

	deadlinedCtx := opts.Context
	if deadlinedCtx == nil {
		deadlinedCtx = context.Background()
	}

	d := collection.deadline(deadlinedCtx, time.Now(), opts.Timeout)
	deadlinedCtx, cancel := context.WithDeadline(deadlinedCtx, d)
	defer cancel()

	cli := collection.sb.getClient()
	collectionID, err := cli.fetchCollectionID(deadlinedCtx, collection.sb.ScopeName, collection.sb.CollectionName)
	if err != nil {
		if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) { //TODO: is this how we want to do this?
			collection.setCollectionUnknown()
			return nil, kvError{gocbcore.ErrCollectionUnknown}
		}
		return nil, err
	}

	err = collection.setCollectionID(collectionID)
	if err != nil {
		return nil, err
	}

	return collection, nil
}

func (c *Collection) clone() *Collection {
	newC := *c
	return &newC
}

func (c *Collection) getKvProviderAndID(ctx context.Context) (uint32, kvProvider, error) {
	cli := c.sb.getClient()
	agent, err := cli.getKvProvider()
	if err != nil {
		return 0, nil, err
	}

	if c.scopeUnknown() {
		return 0, nil, kvError{gocbcore.ErrScopeUnknown} // TODO: probably not how we want to do this
	}

	if c.collectionUnknown() {
		return 0, nil, kvError{gocbcore.ErrCollectionUnknown} // TODO: probably not how we want to do this
	}

	return c.collectionID(), agent, nil
}

// func (c *Collection) WithDurability(persistTo, replicateTo uint) *Collection {
// 	n := c.clone()
// 	n.sb.PersistTo = persistTo
// 	n.sb.ReplicateTo = replicateTo
// 	n.sb.recacheClient()
// 	return n
// }

func (c *Collection) WithOperationTimeout(duration time.Duration) *Collection {
	n := c.clone()
	n.sb.KvTimeout = duration
	n.sb.recacheClient()
	return n
}

// startKvOpTrace starts a new span for a given operationName. If parentSpanCtx is not nil then the span will be a
// ChildOf that span context.
func (c *Collection) startKvOpTrace(parentSpanCtx opentracing.SpanContext, operationName string) opentracing.Span {
	var span opentracing.Span
	if parentSpanCtx == nil {
		span = opentracing.GlobalTracer().StartSpan(operationName,
			opentracing.Tag{Key: "couchbase.collection", Value: c.sb.CollectionName},
			opentracing.Tag{Key: "couchbase.service", Value: "kv"})
	} else {
		span = opentracing.GlobalTracer().StartSpan(operationName,
			opentracing.Tag{Key: "couchbase.collection", Value: c.sb.CollectionName},
			opentracing.Tag{Key: "couchbase.service", Value: "kv"}, opentracing.ChildOf(parentSpanCtx))
	}

	return span
}

func (c *Collection) SetKvTimeout(duration time.Duration) *Collection {
	c.sb.KvTimeout = duration
	return c
}
