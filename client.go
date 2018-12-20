package gocb

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"gopkg.in/couchbase/gocbcore.v7"
)

type client interface {
	Hash() string
	connect() error
	fetchCollectionManifest() (bytesOut []byte, errOut error)
	fetchCollectionID(scopeName string, collectionName string) (uint32, uint32, error)
	getKvProvider() (kvProvider, error)
	getQueryProvider() (queryProvider, error)
	close() error
}

type stdClient struct {
	cluster *Cluster
	state   clientStateBlock
	lock    sync.Mutex
	agent   *gocbcore.Agent
}

func newClient(cluster *Cluster, sb *clientStateBlock) *stdClient {
	client := &stdClient{
		cluster: cluster,
		state:   *sb,
	}
	return client
}

func (c *stdClient) Hash() string {
	return c.state.Hash()
}

// TODO: This probably needs to be deadlined...
func (c *stdClient) connect() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	config := &gocbcore.AgentConfig{
		// TODO: Generate the UserString appropriately
		UserString:           "gocb/2.0.0-dev",
		ConnectTimeout:       60000 * time.Millisecond,
		ServerConnectTimeout: 7000 * time.Millisecond,
		NmvRetryDelay:        100 * time.Millisecond,
		UseKvErrorMaps:       true,
		UseDurations:         true,
		NoRootTraceSpans:     true,
		UseCollections:       true,
		UseEnhancedErrors:    true,
	}

	config.BucketName = c.state.BucketName
	config.UseMutationTokens = c.state.UseMutationTokens
	config.Auth = &coreAuthWrapper{
		auth:       c.cluster.authenticator(),
		bucketName: c.state.BucketName,
	}

	err := config.FromConnStr(c.cluster.connSpec().String())
	if err != nil {
		return err
	}

	agent, err := gocbcore.CreateAgent(config)
	if err != nil {
		return err
	}

	c.agent = agent
	return nil
}

func (c *stdClient) getKvProvider() (kvProvider, error) {
	if c.agent == nil {
		return nil, errors.New("Cluster not yet connected")
	}
	return c.agent, nil
}

func (c *stdClient) getQueryProvider() (queryProvider, error) {
	if c.agent == nil {
		return nil, errors.New("Cluster not yet connected")
	}
	return c.agent, nil
}

func (c *stdClient) fetchCollectionManifest() (bytesOut []byte, errOut error) {
	if c.agent == nil {
		errOut = errors.New("Cluster not yet connected")
		return
	}

	waitCh := make(chan struct{})

	c.agent.GetCollectionManifest(func(bytes []byte, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		bytesOut = bytes
		waitCh <- struct{}{}
	})

	<-waitCh

	return
}

func (c *stdClient) fetchCollectionID(scopeName string, collectionName string) (uint32, uint32, error) {
	if scopeName == "_default" && collectionName == "_default" {
		return 0, 0, nil
	}

	manifestBytes, err := c.fetchCollectionManifest()
	if err != nil {
		return 0, 0, err
	}

	var manifest gocbcore.CollectionManifest
	err = json.Unmarshal(manifestBytes, &manifest)
	if err != nil {
		return 0, 0, err
	}

	var foundScope *gocbcore.CollectionManifestScope
	for _, scope := range manifest.Scopes {
		if scope.Name == scopeName {
			foundScope = &scope
			break
		}
	}
	if foundScope == nil {
		return 0, 0, errors.New("Invalid Scope Name")
	}

	var foundCollection *gocbcore.CollectionManifestCollection
	for _, coll := range foundScope.Collections {
		if coll.Name == collectionName {
			foundCollection = &coll
			break
		}
	}
	if foundCollection == nil {
		return 0, 0, gocbcore.ErrCollectionUnknown //TODO: won't be how we want to do this
	}

	scopeID := uint32(foundScope.UID)
	collectionID := uint32(foundCollection.UID)

	return scopeID, collectionID, nil
}

func (c *stdClient) close() error {
	if c.agent == nil {
		return errors.New("Cluster not yet connected") //TODO
	}
	return c.agent.Close()
}
