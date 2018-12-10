package gocb

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	gocbcore "gopkg.in/couchbase/gocbcore.v7"
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
)

type Cluster interface {
	Bucket(bucketName string) Bucket
	// Query(statement string, params *QueryParameters, opts *QueryOptions) (QueryResults, error)
	// AnalyticsQuery(statement string, params *AnalyticsParameters, opts *AnalyticsOptions) (AnalyticsResults, error)
	// SearchQuery(statement string, opts *SearchOptions) (SearchResults, error)

	// Diagnostics() (DiagnosticsResult, error)
	// Manager() (ClusterManger, error)
	Tracer() opentracing.Tracer
	SetN1qlTimeout(timeout time.Duration)

	authenticator() Authenticator
	connSpec() gocbconnstr.ConnSpec
	getClient(sb *clientStateBlock) *client
}

type StdCluster struct {
	cSpec gocbconnstr.ConnSpec
	auth  Authenticator

	connectionsLock sync.RWMutex
	connections     map[string]*client

	clusterLock sync.RWMutex
	// queryCache  map[string]*n1qlCache

	sb  stateBlock
	ssb servicesStateBlock
}

func Connect(connStr string, auth Authenticator) (Cluster, error) {
	connSpec, err := gocbconnstr.Parse(connStr)
	if err != nil {
		return nil, err
	}

	cluster := &StdCluster{
		cSpec:       connSpec,
		auth:        auth,
		connections: make(map[string]*client),
		ssb: servicesStateBlock{
			n1qlTimeout: 75 * time.Second,
		},
		sb: stateBlock{
			tracer:            opentracing.NoopTracer{},
			N1qlRetryBehavior: StandardDelayRetryBehavior(10, 2, 500*time.Millisecond, ExponentialDelayFunction),
		},
	}

	err = cluster.parseExtraConnStrOptions(connSpec)
	if err != nil {
		return nil, err
	}

	return cluster, nil
}

func (c *StdCluster) parseExtraConnStrOptions(spec gocbconnstr.ConnSpec) error {
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

func (c *StdCluster) Bucket(bucketName string) Bucket {
	return newBucket(c, bucketName)
}

func (c *StdCluster) Close() error {
	return nil
}

func (c *StdCluster) getClient(sb *clientStateBlock) *client {
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
func (c *StdCluster) N1qlTimeout() time.Duration {
	return c.ssb.n1qlTimeout
}

// SetN1qlTimeout sets the maximum time to wait for a cluster-level N1QL query to complete.
func (c *StdCluster) SetN1qlTimeout(timeout time.Duration) {
	c.ssb.n1qlTimeout = timeout
}

// SetTracer allows you to specify a custom tracer to use for this cluster.
// EXPERIMENTAL
func (c *StdCluster) SetTracer(tracer opentracing.Tracer) {
	if c.sb.tracer != nil {
		tracerDecRef(c.sb.tracer)
	}

	tracerAddRef(tracer)
	c.sb.tracer = tracer
}

func (c *StdCluster) randomAgent() (*gocbcore.Agent, error) {
	c.connectionsLock.RLock()
	if len(c.connections) == 0 {
		c.connectionsLock.RUnlock()
		return nil, nil // TODO: return an error
	}
	var randomClient *client
	for _, c := range c.connections { // This is ugly
		randomClient = c
		break
	}
	c.connectionsLock.RUnlock()
	return randomClient.getAgent()
}

func (c *StdCluster) Tracer() opentracing.Tracer {
	return c.sb.tracer
}

func (c *StdCluster) authenticator() Authenticator {
	return c.auth
}

func (c *StdCluster) connSpec() gocbconnstr.ConnSpec {
	return c.cSpec
}
