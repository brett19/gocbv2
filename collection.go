package gocb

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	gocbcore "gopkg.in/couchbase/gocbcore.v7"
)

type Collection struct {
	sb   stateBlock
	csb  *collectionStateBlock
	lock sync.Mutex
}

func (c *Collection) setCollectionID(scopeID uint32, collectionID uint32) error {
	if c.initialized() {
		return errors.New("collection already initialized")
	}

	c.lock.Lock()
	c.csb.CollectionInitialized = true
	c.csb.CollectionID = collectionID
	c.csb.ScopeID = scopeID
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

func newCollection(scope *Scope, collectionName string) *Collection {
	collection := &Collection{
		sb:  scope.sb,
		csb: &collectionStateBlock{},
	}
	collection.sb.CollectionName = collectionName
	collection.sb.KvTimeout = 10 * time.Second
	collection.sb.recacheClient()
	return collection
}

func (c *Collection) clone() *Collection {
	newC := *c
	return &newC
}

func (c *Collection) transcodeDocument(doc Document) (Document, error) {
	if doc.value != nil {
		docBytes, err := json.Marshal(doc.value)
		if err != nil {
			return Document{}, err
		}

		doc.bytes = docBytes
		doc.value = nil
	}

	if doc.bytes == nil {
		return Document{}, errors.New("invalid document value")
	}

	return doc, nil
}

func (c *Collection) getAgentAndCollection() (uint32, *gocbcore.Agent, error) {
	client := c.sb.getClient()
	agent, err := client.getAgent()
	if err != nil {
		return 0, nil, err
	}

	if c.scopeUnknown() {
		return 0, nil, gocbcore.ErrScopeUnknown // TODO: probably not how we want to do this
	}

	if c.collectionUnknown() {
		return 0, nil, gocbcore.ErrCollectionUnknown // TODO: probably not how we want to do this
	}

	if c.initialized() {
		return c.collectionID(), agent, nil
	}

	scopeID, collectionID, err := client.fetchCollectionID(c.sb.ScopeName, c.sb.CollectionName)
	if err != nil {
		if gocbcore.IsErrorStatus(err, gocbcore.StatusScopeUnknown) { //TODO: is this how we want to do this?
			c.setScopeUnknown()
		}
		if gocbcore.IsErrorStatus(err, gocbcore.StatusCollectionUnknown) { //TODO: is this how we want to do this?
			c.setCollectionUnknown()
		}
		return 0, nil, err
	}

	c.setCollectionID(scopeID, collectionID)

	return collectionID, agent, nil
}

func (c *Collection) WithDurability(persistTo, replicateTo uint) *Collection {
	n := c.clone()
	n.sb.PersistTo = persistTo
	n.sb.ReplicateTo = replicateTo
	n.sb.recacheClient()
	return n
}

func (c *Collection) WithOperationTimeout(duration time.Duration) *Collection {
	n := c.clone()
	n.sb.KvTimeout = duration
	n.sb.recacheClient()
	return n
}

func (c *Collection) WithMutationTokens() *Collection {
	n := c.clone()
	n.sb.UseMutationTokens = true
	n.sb.recacheClient()
	return n
}

func (c *Collection) startKvOpTrace(ctx context.Context, operationName string) opentracing.Span {
	var span opentracing.Span
	parentctx, _ := ctx.Value(TracerSpanCtxKey{}).(opentracing.SpanContext)
	if parentctx == nil {
		span = c.sb.tracer.StartSpan("Read",
			opentracing.Tag{Key: "couchbase.collection", Value: c.sb.CollectionName},
			opentracing.Tag{Key: "couchbase.service", Value: "kv"})
	} else {
		span = c.sb.tracer.StartSpan("Read",
			opentracing.Tag{Key: "couchbase.collection", Value: c.sb.CollectionName},
			opentracing.Tag{Key: "couchbase.service", Value: "kv"}, opentracing.ChildOf(parentctx))
	}

	return span
}
