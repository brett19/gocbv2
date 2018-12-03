package gocb

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
)

type Cluster struct {
	connSpec      gocbconnstr.ConnSpec
	authenticator Authenticator

	connectionsLock sync.RWMutex
	connections     map[string]*client

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
		connSpec:      connSpec,
		authenticator: auth,
		connections:   make(map[string]*client),
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

// SetTracer allows you to specify a custom tracer to use for this cluster.
// EXPERIMENTAL
func (c *Cluster) SetTracer(tracer opentracing.Tracer) {
	if c.sb.tracer != nil {
		tracerDecRef(c.sb.tracer)
	}

	tracerAddRef(tracer)
	c.sb.tracer = tracer
}
