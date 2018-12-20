package gocb

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/couchbase/gocbcore"
	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
)

// Cluster represents a connection to a specific Couchbase cluster.
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

// ClusterOptions is the set of options available for creating a Cluster.
type ClusterOptions struct {
	Authenticator Authenticator
}

// ClusterCloseOptions is the set of options available when disconnecting from a Cluster.
type ClusterCloseOptions struct {
}

// NewCluster creates and returns a Cluster instance created using the provided options and connection string.
// The connection string properties are copied from (and should stay in sync with) the gocbcore agent.FromConnStr comment.
// Supported connSpecStr options are:
//   cacertpath (string) - Path to the CA certificate
//   certpath (string) - Path to your authentication certificate
//   keypath (string) - Path to your authentication key
//   config_total_timeout (int) - Maximum period to attempt to connect to cluster in ms.
//   config_node_timeout (int) - Maximum period to attempt to connect to a node in ms.
//   http_redial_period (int) - Maximum period to keep HTTP config connections open in ms.
//   http_retry_delay (int) - Period to wait between retrying nodes for HTTP config in ms.
//   config_poll_floor_interval (int) - Minimum time to wait between fetching configs via CCCP in ms.
//   config_poll_interval (int) - Period to wait between CCCP config polling in ms.
//   kv_pool_size (int) - The number of connections to establish per node.
//   max_queue_size (int) - The maximum size of the operation queues per node.
//   use_kverrmaps (bool) - Whether to enable error maps from the server.
//   use_enhanced_errors (bool) - Whether to enable enhanced error information.
//   fetch_mutation_tokens (bool) - Whether to fetch mutation tokens for operations.
//   compression (bool) - Whether to enable network-wise compression of documents.
//   compression_min_size (int) - The minimal size of the document to consider compression.
//   compression_min_ratio (float64) - The minimal compress ratio (compressed / original) for the document to be sent compressed.
//   server_duration (bool) - Whether to enable fetching server operation durations.
//   http_max_idle_conns (int) - Maximum number of idle http connections in the pool.
//   http_max_idle_conns_per_host (int) - Maximum number of idle http connections in the pool per host.
//   http_idle_conn_timeout (int) - Maximum length of time for an idle connection to stay in the pool in ms.
//   network (string) - The network type to use.
//   orphaned_response_logging (bool) - Whether to enable orphan response logging.
//   orphaned_response_logging_interval (int) - How often to log orphan responses in ms.
//   orphaned_response_logging_sample_size (int) - The number of samples to include in each orphaned response log.
//   operation_tracing (bool) - Whether to enable tracing.
//   n1ql_timeout (int) - Maximum execution time for n1ql queries in ms.
//   fts_timeout (int) - Maximum execution time for fts searches in ms.
//   analytics_timeout (int) - Maximum execution time for analytics queries in ms.
func NewCluster(connStr string, opts ClusterOptions) (*Cluster, error) {
	connSpec, err := gocbconnstr.Parse(connStr)
	if err != nil {
		return nil, err
	}

	cluster := &Cluster{
		cSpec:       connSpec,
		auth:        opts.Authenticator,
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

// Bucket connects the cluster to server(s) and returns a new Bucket instance.
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

// Diagnostics returns information about the internal state of the SDK.
//
// Experimental: This API is subject to change at any time.
func (c *Cluster) Diagnostics(reportID string) (*DiagnosticsResult, error) {
	return nil, errors.New("Not implemented")
}

// Users returns a new UsersManager for the Cluster.
func (c *Cluster) Users() (*UsersManager, error) {
	return nil, errors.New("Not implemented")
}

// Buckets returns a new BucketsManager for the Cluster.
func (c *Cluster) Buckets() (*BucketsManager, error) {
	return nil, errors.New("Not implemented")
}

// QueryIndexes returns a new QueryIndexesManager for the Cluster.
func (c *Cluster) QueryIndexes() (*QueryIndexesManager, error) {
	return nil, errors.New("Not implemented")
}

// Close shuts down all buckets in this cluster and invalidates any references this cluster has.
func (c *Cluster) Close(opts *ClusterCloseOptions) error {
	var overallErr error

	c.clusterLock.Lock()
	for key, conn := range c.connections {
		err := conn.close()
		if err != nil && gocbcore.ErrorCause(err) != gocbcore.ErrShutdown {
			logWarnf("Failed to close a client in cluster close: %s", err)
			overallErr = err
		}

		delete(c.connections, key)
	}
	c.clusterLock.Unlock()

	return overallErr
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

func (c *Cluster) n1qlTimeout() time.Duration {
	return c.ssb.n1qlTimeout
}
