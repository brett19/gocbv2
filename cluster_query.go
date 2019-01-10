package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/opentracing/opentracing-go"
	"gopkg.in/couchbase/gocbcore.v7"
)

type n1qlCache struct {
	name        string
	encodedPlan string
}

type n1qlError struct {
	Code    uint32 `json:"code"`
	Message string `json:"msg"`
}

func (e *n1qlError) Error() string {
	return fmt.Sprintf("[%d] %s", e.Code, e.Message)
}

type n1qlResponseMetrics struct {
	ElapsedTime   string `json:"elapsedTime"`
	ExecutionTime string `json:"executionTime"`
	ResultCount   uint   `json:"resultCount"`
	ResultSize    uint   `json:"resultSize"`
	MutationCount uint   `json:"mutationCount,omitempty"`
	SortCount     uint   `json:"sortCount,omitempty"`
	ErrorCount    uint   `json:"errorCount,omitempty"`
	WarningCount  uint   `json:"warningCount,omitempty"`
}

type n1qlResponse struct {
	RequestID       string              `json:"requestID"`
	ClientContextID string              `json:"clientContextID"`
	Results         []json.RawMessage   `json:"results,omitempty"`
	Errors          []n1qlError         `json:"errors,omitempty"`
	Status          string              `json:"status"`
	Metrics         n1qlResponseMetrics `json:"metrics"`
}

type n1qlMultiError []n1qlError

func (e *n1qlMultiError) Error() string {
	return (*e)[0].Error()
}

// Code returns the error code for the error
func (e *n1qlMultiError) Code() uint32 {
	return (*e)[0].Code
}

// QueryResultMetrics encapsulates various metrics gathered during a queries execution.
type QueryResultMetrics struct {
	ElapsedTime   time.Duration
	ExecutionTime time.Duration
	ResultCount   uint
	ResultSize    uint
	MutationCount uint
	SortCount     uint
	ErrorCount    uint
	WarningCount  uint
}

// QueryResults allows access to the results of a N1QL query.
type QueryResults interface {
	One(valuePtr interface{}) error
	Next(valuePtr interface{}) bool
	NextBytes() []byte
	Close() error

	RequestID() string
	ClientContextID() string
	Metrics() QueryResultMetrics

	// SourceEndpoint returns the source endpoint where the request was sent to.
	// VOLATILE
	SourceEndpoint() string
}

type n1qlResults struct {
	closed          bool
	index           int
	rows            []json.RawMessage
	err             error
	requestID       string
	clientContextID string
	metrics         QueryResultMetrics
	sourceAddr      string
}

// Next assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *n1qlResults) Next(valuePtr interface{}) bool {
	if r.err != nil {
		return false
	}

	row := r.NextBytes()
	if row == nil {
		return false
	}

	r.err = json.Unmarshal(row, valuePtr)
	if r.err != nil {
		return false
	}

	return true
}

// NextBytes returns the next result from the results as a byte array.
func (r *n1qlResults) NextBytes() []byte {
	if r.err != nil {
		return nil
	}

	if r.index+1 >= len(r.rows) {
		r.closed = true
		return nil
	}
	r.index++

	return r.rows[r.index]
}

// Close marks the results as closed, returning any errors that occurred during reading the results.
func (r *n1qlResults) Close() error {
	r.closed = true
	return r.err
}

// One assigns the first value from the results into the value pointer.
func (r *n1qlResults) One(valuePtr interface{}) error {
	if !r.Next(valuePtr) {
		err := r.Close()
		if err != nil {
			return err
		}
		// return ErrNoResults
	}

	// Ignore any errors occurring after we already have our result
	err := r.Close()
	if err != nil {
		// Return no error as we got the one result already.
		return nil
	}

	return nil
}

// SourceEndpoint returns the endpoint used for execution of this query.
func (r *n1qlResults) SourceEndpoint() string {
	return r.sourceAddr
}

// RequestID returns the request ID used for this query.
func (r *n1qlResults) RequestID() string {
	if !r.closed {
		panic("Result must be closed before accessing meta-data")
	}

	return r.requestID
}

// ClientContextID returns the context ID used for this query.
func (r *n1qlResults) ClientContextID() string {
	if !r.closed {
		panic("Result must be closed before accessing meta-data")
	}

	return r.clientContextID
}

// Metrics returns metrics about execution of this result.
func (r *n1qlResults) Metrics() QueryResultMetrics {
	if !r.closed {
		panic("Result must be closed before accessing meta-data")
	}

	return r.metrics
}

type queryProvider interface {
	DoHttpRequest(req *gocbcore.HttpRequest) (*gocbcore.HttpResponse, error)
}

// Query executes the N1QL query statement on the server n1qlEp.
// This function assumes that `opts` already contains all the required
// settings. This function will inject any additional connection or request-level
// settings into the `opts` map (currently this is only the timeout).
func (c *Cluster) Query(statement string, opts *QueryOptions) (QueryResults, error) {
	if opts == nil {
		opts = &QueryOptions{}
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	var span opentracing.Span
	if opts.ParentSpanContext == nil {
		span = opentracing.GlobalTracer().StartSpan("ExecuteN1QLQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "n1ql"})
	} else {
		span = opentracing.GlobalTracer().StartSpan("ExecuteN1QLQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "n1ql"}, opentracing.ChildOf(opts.ParentSpanContext))
	}
	defer span.Finish()

	provider, err := c.getQueryProvider()
	if err != nil {
		return nil, err
	}

	return c.query(ctx, span.Context(), statement, opts, provider)
}

func (c *Cluster) query(ctx context.Context, traceCtx opentracing.SpanContext, statement string, opts *QueryOptions,
	provider queryProvider) (QueryResults, error) {

	queryOpts, err := opts.toMap(statement)
	if err != nil {
		return nil, err
	}

	// Work out which timeout to use, the cluster level default or query specific one
	timeout := c.n1qlTimeout()
	var optTimeout time.Duration
	tmostr, castok := queryOpts["timeout"].(string)
	if castok {
		optTimeout, err = time.ParseDuration(tmostr)
		if err != nil {
			return nil, err
		}
	}
	if optTimeout > 0 && optTimeout < timeout {
		timeout = optTimeout
	}
	queryOpts["timeout"] = timeout.String()

	// Doing this will set the context deadline to whichever is shorter, what is already set or the timeout
	// value
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()

	var retries uint
	var res QueryResults
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			retries++
			if opts.Prepared {
				etrace := opentracing.GlobalTracer().StartSpan("execute", opentracing.ChildOf(traceCtx))
				res, err = c.doPreparedN1qlQuery(ctx, traceCtx, queryOpts, provider)
				etrace.Finish()
			} else {
				res, err = c.executeN1qlQuery(ctx, traceCtx, queryOpts, provider)
			}
			if err == nil {
				return res, err
			}

			if !isRetryableError(err) || c.sb.N1qlRetryBehavior == nil || !c.sb.N1qlRetryBehavior.CanRetry(retries) {
				return res, err
			}

			time.Sleep(c.sb.N1qlRetryBehavior.NextInterval(retries))
		}
	}
}

func (c *Cluster) doPreparedN1qlQuery(ctx context.Context, traceCtx opentracing.SpanContext, queryOpts map[string]interface{},
	provider queryProvider) (QueryResults, error) {

	stmtStr, isStr := queryOpts["statement"].(string)
	if !isStr {
		// return nil, ErrCliInternalError
	}

	c.clusterLock.RLock()
	cachedStmt := c.queryCache[stmtStr]
	c.clusterLock.RUnlock()

	if cachedStmt != nil {
		// Attempt to execute our cached query plan
		delete(queryOpts, "statement")
		queryOpts["prepared"] = cachedStmt.name
		queryOpts["encoded_plan"] = cachedStmt.encodedPlan

		etrace := opentracing.GlobalTracer().StartSpan("execute", opentracing.ChildOf(traceCtx))

		results, err := c.executeN1qlQuery(ctx, etrace.Context(), queryOpts, provider)
		if err == nil {
			etrace.Finish()
			return results, nil
		}

		etrace.Finish()

		// If we get error 4050, 4070 or 5000, we should attempt
		//   to re-prepare the statement immediately before failing.
		if !isRetryableError(err) {
			return nil, err
		}
	}

	// Prepare the query
	ptrace := opentracing.GlobalTracer().StartSpan("prepare", opentracing.ChildOf(traceCtx))

	var err error
	cachedStmt, err = c.prepareN1qlQuery(ctx, ptrace.Context(), queryOpts, provider)
	if err != nil {
		ptrace.Finish()
		return nil, err
	}

	ptrace.Finish()

	// Save new cached statement
	c.clusterLock.Lock()
	c.queryCache[stmtStr] = cachedStmt
	c.clusterLock.Unlock()

	// Update with new prepared data
	delete(queryOpts, "statement")
	queryOpts["prepared"] = cachedStmt.name
	queryOpts["encoded_plan"] = cachedStmt.encodedPlan

	etrace := opentracing.GlobalTracer().StartSpan("execute", opentracing.ChildOf(traceCtx))
	defer etrace.Finish()

	return c.executeN1qlQuery(ctx, etrace.Context(), queryOpts, provider)
}

func (c *Cluster) prepareN1qlQuery(ctx context.Context, traceCtx opentracing.SpanContext, opts map[string]interface{},
	provider queryProvider) (*n1qlCache, error) {

	prepOpts := make(map[string]interface{})
	for k, v := range opts {
		prepOpts[k] = v
	}
	prepOpts["statement"] = "PREPARE " + opts["statement"].(string)

	prepRes, err := c.executeN1qlQuery(ctx, traceCtx, opts, provider)
	if err != nil {
		return nil, err
	}

	var preped n1qlPrepData
	err = prepRes.One(&preped)
	if err != nil {
		return nil, err
	}

	return &n1qlCache{
		name:        preped.Name,
		encodedPlan: preped.EncodedPlan,
	}, nil
}

type n1qlPrepData struct {
	EncodedPlan string `json:"encoded_plan"`
	Name        string `json:"name"`
}

// Executes the N1QL query (in opts) on the server n1qlEp.
// This function assumes that `opts` already contains all the required
// settings. This function will inject any additional connection or request-level
// settings into the `opts` map.
func (c *Cluster) executeN1qlQuery(ctx context.Context, traceCtx opentracing.SpanContext, opts map[string]interface{},
	provider queryProvider) (QueryResults, error) {

	reqJSON, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.N1qlService,
		Path:    "/query/service",
		Method:  "POST",
		Context: ctx,
		Body:    reqJSON,
	}

	dtrace := opentracing.GlobalTracer().StartSpan("dispatch", opentracing.ChildOf(traceCtx))

	resp, err := provider.DoHttpRequest(req)
	if err != nil {
		dtrace.Finish()
		return nil, err
	}

	dtrace.Finish()

	strace := opentracing.GlobalTracer().StartSpan("streaming", opentracing.ChildOf(traceCtx))

	n1qlResp := n1qlResponse{}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&n1qlResp)
	if err != nil {
		strace.Finish()
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	// TODO(brett19): place the server_duration in the right place...
	// srvDuration, _ := time.ParseDuration(n1qlResp.Metrics.ExecutionTime)
	// strace.SetTag("server_duration", srvDuration)

	strace.SetTag("couchbase.operation_id", n1qlResp.RequestID)
	strace.Finish()

	if len(n1qlResp.Errors) > 0 {
		return nil, (*n1qlMultiError)(&n1qlResp.Errors)
	}

	if resp.StatusCode != 200 {
		// return nil, &viewError{
		// 	Message: "HTTP Error",
		// 	Reason:  fmt.Sprintf("Status code was %d.", resp.StatusCode),
		// }
	}

	elapsedTime, err := time.ParseDuration(n1qlResp.Metrics.ElapsedTime)
	if err != nil {
		logDebugf("Failed to parse elapsed time duration (%s)", err)
	}

	executionTime, err := time.ParseDuration(n1qlResp.Metrics.ExecutionTime)
	if err != nil {
		logDebugf("Failed to parse execution time duration (%s)", err)
	}

	epInfo, err := url.Parse(resp.Endpoint)
	if err != nil {
		logWarnf("Failed to parse N1QL source address")
		epInfo = &url.URL{
			Host: "",
		}
	}

	return &n1qlResults{
		sourceAddr:      epInfo.Host,
		requestID:       n1qlResp.RequestID,
		clientContextID: n1qlResp.ClientContextID,
		index:           -1,
		rows:            n1qlResp.Results,
		metrics: QueryResultMetrics{
			ElapsedTime:   elapsedTime,
			ExecutionTime: executionTime,
			ResultCount:   n1qlResp.Metrics.ResultCount,
			ResultSize:    n1qlResp.Metrics.ResultSize,
			MutationCount: n1qlResp.Metrics.MutationCount,
			SortCount:     n1qlResp.Metrics.SortCount,
			ErrorCount:    n1qlResp.Metrics.ErrorCount,
			WarningCount:  n1qlResp.Metrics.WarningCount,
		},
	}, nil
}
