package gocb

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
)

type Cluster struct {
	cSpec gocbconnstr.ConnSpec
	auth  Authenticator

	connectionsLock sync.RWMutex
	connections     map[string]client

	clusterLock sync.RWMutex
	queryCache  map[string]*n1qlCache

	sb  stateBlock
	ssb servicesStateBlock
}

func Connect(connStr string, auth Authenticator) (*Cluster, error) {
	connSpec, err := gocbconnstr.Parse(connStr)
	if err != nil {
		return nil, err
	}

	cluster := &Cluster{
		cSpec:       connSpec,
		auth:        auth,
		connections: make(map[string]client),
		ssb: servicesStateBlock{
			n1qlTimeout: 75 * time.Second,
		},
		sb: stateBlock{
			N1qlRetryBehavior: StandardDelayRetryBehavior(10, 2, 500*time.Millisecond, ExponentialDelayFunction),
		},
	}

	err = cluster.parseExtraConnStrOptions(connSpec)
	if err != nil {
		return nil, err
	}

	if !opentracing.IsGlobalTracerRegistered() {
		// we'd add threshold logging here
		opentracing.SetGlobalTracer(opentracing.NoopTracer{})
	}

	return cluster, nil
}

func (c *Cluster) parseExtraConnStrOptions(spec gocbconnstr.ConnSpec) error {
	fetchOption := func(name string) (string, bool) {
		optValue := spec.Options[name]
		if len(optValue) == 0 {
			return "", false
		}
		return optValue[len(optValue)-1], true
	}

	if valStr, ok := fetchOption("n1ql_timeout"); ok {
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return fmt.Errorf("n1ql_timeout option must be a number")
		}
		c.ssb.n1qlTimeout = time.Duration(val) * time.Millisecond
	}

	return nil
}

func (c *Cluster) Bucket(bucketName string, opts *BucketOptions) (*Bucket, error) {
	if opts == nil {
		opts = &BucketOptions{}
	}
	b := newBucket(c, bucketName, *opts)
	err := b.connect()
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (c *Cluster) Close() error {
	return nil
}

func (c *Cluster) getClient(sb *clientStateBlock) client {
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

// N1qlTimeout returns the maximum time to wait for a cluster-level N1QL query to complete.
func (c *Cluster) N1qlTimeout() time.Duration {
	return c.ssb.n1qlTimeout
}

// SetN1qlTimeout sets the maximum time to wait for a cluster-level N1QL query to complete.
func (c *Cluster) SetN1qlTimeout(timeout time.Duration) {
	c.ssb.n1qlTimeout = timeout
}

func (c *Cluster) randomClient() (client, error) {
	c.connectionsLock.RLock()
	if len(c.connections) == 0 {
		c.connectionsLock.RUnlock()
		return nil, nil // TODO: return an error
	}
	var randomClient client
	for _, c := range c.connections { // This is ugly
		randomClient = c
		break
	}
	c.connectionsLock.RUnlock()
	return randomClient, nil
}

func (c *Cluster) authenticator() Authenticator {
	return c.auth
}

func (c *Cluster) connSpec() gocbconnstr.ConnSpec {
	return c.cSpec
}

func (c *Cluster) Diagnostics(reportId string) (*DiagnosticsResult, error) {
	return nil, errors.New("Not implemented")
}

func (c *Cluster) Manager() (*ClusterManager, error) {
	return nil, errors.New("Not implemented")
}

func (c *Cluster) getQueryProvider() (queryProvider, error) {
	client, err := c.randomClient()
	if err != nil {
		return nil, err
	}

	provider, err := client.getQueryProvider()
	if err != nil {
		return nil, err
	}

	return provider, nil
}
