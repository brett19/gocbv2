package gocb

import (
	"encoding/json"
	"errors"
	"time"

	gocbcore "gopkg.in/couchbase/gocbcore.v7"
)

type Collection struct {
	sb stateBlock
}

func newCollection(scope *Scope, collectionName string) *Collection {
	collection := &Collection{
		sb: scope.sb,
	}
	collection.sb.CollectionName = collectionName
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

	collectionID, err := client.fetchCollectionID(c.sb.ScopeName, c.sb.CollectionName)
	if err != nil {
		return 0, nil, err
	}

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
