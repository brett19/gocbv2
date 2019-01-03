package gocb

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"gopkg.in/couchbase/gocbcore.v7"
)

type client interface {
	Hash() string
	connect() error
	fetchCollectionManifest() (bytesOut []byte, errOut error)
	fetchCollectionID(ctx context.Context, scopeName string, collectionName string) (uint32, error)
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
		return kvError{err: errors.Wrap(err, "could not connect to cluster")}
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

// TODO: we need to deadline this
func (c *stdClient) fetchCollectionManifest() (bytesOut []byte, errOut error) {
	if c.agent == nil {
		errOut = errors.New("Cluster not yet connected")
		return
	}

	waitCh := make(chan struct{})

	c.agent.GetCollectionID("_default", "_default", func(manifestID uint64, collectionID uint32, err error) {
		if err != nil {
			errOut = err
			waitCh <- struct{}{}
			return
		}

		// bytesOut = bytes
		waitCh <- struct{}{}
	})

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

func (c *stdClient) fetchCollectionID(ctx context.Context, scopeName string, collectionName string) (uint32, error) {
	if scopeName == "_default" && collectionName == "_default" {
		return 0, nil
	}

	if c.agent == nil {
		return 0, errors.New("Cluster not yet connected")
	}

	waitCh := make(chan struct{})
	var collectionID uint32
	var colErr error

	op, err := c.agent.GetCollectionID(scopeName, collectionName, func(manifestID uint64, cid uint32, err error) {
		if err != nil {
			colErr = err
			waitCh <- struct{}{}
			return
		}

		collectionID = cid
		waitCh <- struct{}{}
	})
	if err != nil {
		return 0, err
	}

	select {
	case <-ctx.Done():
		if op.Cancel() {
			if err == context.DeadlineExceeded {
				colErr = timeoutError{err: ctx.Err()}
			} else {
				colErr = ctx.Err()
			}
		} else {
			<-waitCh
		}
	case <-waitCh:
	}

	return collectionID, colErr
}

func (c *stdClient) close() error {
	if c.agent == nil {
		return errors.New("Cluster not yet connected") //TODO
	}
	return c.agent.Close()
}
