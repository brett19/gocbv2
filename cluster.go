package gocb

import (
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
	"sync"
)

type Cluster struct {
	connSpec gocbconnstr.ConnSpec
	authenticator Authenticator

	connectionsLock sync.RWMutex
	connections map[string]*client

	sb stateBlock
}

func Connect(connStr string, auth Authenticator) (*Cluster, error) {
	connSpec, err := gocbconnstr.Parse(connStr)
	if err != nil {
		return nil, err
	}

	cluster := &Cluster{
		connSpec: connSpec,
		authenticator: auth,
		connections: make(map[string]*client),
	}
	return cluster, nil
}

func (c *Cluster) Bucket(bucketName string) *Bucket {
	return newBucket(c, bucketName)
}

func (c *Cluster) Close() error {
	return nil
}

func (c *Cluster) getClient(sb *clientStateBlock) *client {
	c.connectionsLock.Lock()
	defer c.connectionsLock.Unlock()

	hash := sb.Hash()
	if client, ok := c.connections[hash]; ok {
		return client
	}

	client := newClient(c, sb)
	c.connections[hash] = client

	return client
}
