package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"gopkg.in/couchbase/gocbcore.v7"
)

type analyticsError struct {
	Code    uint32 `json:"code"`
	Message string `json:"msg"`
}

func (e *analyticsError) Error() string {
	return fmt.Sprintf("[%d] %s", e.Code, e.Message)
}

// AnalyticsWarning represents any warning generating during the execution of an Analytics query.
type AnalyticsWarning struct {
	Code    uint32 `json:"code"`
	Message string `json:"msg"`
}

type analyticsResponse struct {
	RequestID       string                   `json:"requestID"`
	ClientContextID string                   `json:"clientContextID"`
	Results         []json.RawMessage        `json:"results,omitempty"`
	Errors          []analyticsError         `json:"errors,omitempty"`
	Warnings        []AnalyticsWarning       `json:"warnings,omitempty"`
	Status          string                   `json:"status,omitempty"`
	Signature       interface{}              `json:"signature,omitempty"`
	Metrics         analyticsResponseMetrics `json:"metrics,omitempty"`
	Handle          string                   `json:"handle,omitempty"`
}

type analyticsResponseMetrics struct {
	ElapsedTime      string `json:"elapsedTime"`
	ExecutionTime    string `json:"executionTime"`
	ResultCount      uint   `json:"resultCount"`
	ResultSize       uint   `json:"resultSize"`
	MutationCount    uint   `json:"mutationCount,omitempty"`
	SortCount        uint   `json:"sortCount,omitempty"`
	ErrorCount       uint   `json:"errorCount,omitempty"`
	WarningCount     uint   `json:"warningCount,omitempty"`
	ProcessedObjects uint   `json:"processedObjects,omitempty"`
}

type analyticsResponseHandle struct {
	Status string `json:"status,omitempty"`
	Handle string `json:"handle,omitempty"`
}

type analyticsMultiError []analyticsError

func (e *analyticsMultiError) Error() string {
	return (*e)[0].Error()
}

// Code is the error code for this error.
func (e *analyticsMultiError) Code() uint32 {
	return (*e)[0].Code
}

// AnalyticsResultMetrics encapsulates various metrics gathered during a queries execution.
type AnalyticsResultMetrics struct {
	ElapsedTime      time.Duration
	ExecutionTime    time.Duration
	ResultCount      uint
	ResultSize       uint
	MutationCount    uint
	SortCount        uint
	ErrorCount       uint
	WarningCount     uint
	ProcessedObjects uint
}

// AnalyticsDeferredResultHandle allows access to the handle of a deferred Analytics query.
//
// Experimental: This API is subject to change at any time.
type AnalyticsDeferredResultHandle interface {
	One(valuePtr interface{}) error
	Next(valuePtr interface{}) bool
	NextBytes() []byte
	Close() error

	Status() (string, error)
}

type analyticsDeferredResultHandle struct {
	handleUri string
	status    string
	rows      *analyticsRows
	err       error
	provider  queryProvider
	hasResult bool
	timeout   time.Duration
}

type analyticsRows struct {
	closed bool
	index  int
	rows   []json.RawMessage
}

// AnalyticsResults allows access to the results of a Analytics query.
type AnalyticsResults interface {
	One(valuePtr interface{}) error
	Next(valuePtr interface{}) bool
	NextBytes() []byte
	Close() error

	RequestID() string
	ClientContextID() string
	Status() string
	Warnings() []AnalyticsWarning
	Signature() interface{}
	Metrics() AnalyticsResultMetrics
	Handle() AnalyticsDeferredResultHandle
}

type analyticsResults struct {
	rows            *analyticsRows
	err             error
	requestID       string
	clientContextID string
	status          string
	warnings        []AnalyticsWarning
	signature       interface{}
	metrics         AnalyticsResultMetrics
	handle          AnalyticsDeferredResultHandle
}

// NextBytes assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *analyticsResults) Next(valuePtr interface{}) bool {
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
func (r *analyticsResults) NextBytes() []byte {
	if r.err != nil {
		return nil
	}

	return r.rows.NextBytes()
}

// Close marks the results as closed, returning any errors that occurred during reading the results.
func (r *analyticsResults) Close() error {
	r.rows.Close()
	return r.err
}

// One assigns the first value from the results into the value pointer.
func (r *analyticsResults) One(valuePtr interface{}) error {
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

// Warnings returns any warnings that occurred during query execution.
func (r *analyticsResults) Warnings() []AnalyticsWarning {
	return r.warnings
}

// Status returns the status for the results.
func (r *analyticsResults) Status() string {
	return r.status
}

// Signature returns TODO
func (r *analyticsResults) Signature() interface{} {
	return r.signature
}

// Metrics returns metrics about execution of this result.
func (r *analyticsResults) Metrics() AnalyticsResultMetrics {
	return r.metrics
}

// RequestID returns the request ID used for this query.
func (r *analyticsResults) RequestID() string {
	if !r.rows.closed {
		panic("Result must be closed before accessing meta-data")
	}

	return r.requestID
}

// ClientContextID returns the context ID used for this query.
func (r *analyticsResults) ClientContextID() string {
	if !r.rows.closed {
		panic("Result must be closed before accessing meta-data")
	}

	return r.clientContextID
}

// Handle returns a deferred result handle. This can be polled to verify whether
// the result is ready to be read.
//
// Experimental: This API is subject to change at any time.
func (r *analyticsResults) Handle() AnalyticsDeferredResultHandle {
	return r.handle
}

// NextBytes returns the next result from the rows as a byte array.
func (r *analyticsRows) NextBytes() []byte {
	if r.index+1 >= len(r.rows) {
		r.closed = true
		return nil
	}
	r.index++

	return r.rows[r.index]
}

// Close marks the rows as closed.
func (r *analyticsRows) Close() {
	r.closed = true
}

// Next assigns the next result from the results into the value pointer, returning whether the read was successful.
func (r *analyticsDeferredResultHandle) Next(valuePtr interface{}) bool {
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
// TODO: how to deadline/timeout this?
func (r *analyticsDeferredResultHandle) NextBytes() []byte {
	if r.err != nil {
		return nil
	}

	if r.status == "success" && !r.hasResult {
		req := &gocbcore.HttpRequest{
			Service: gocbcore.CbasService,
			Path:    r.handleUri,
			Method:  "GET",
		}

		err := r.executeHandle(req, &r.rows.rows)
		if err != nil {
			r.err = err
			return nil
		}
		r.hasResult = true
	} else if r.status != "success" {
		return nil
	}

	return r.rows.NextBytes()
}

// Close marks the results as closed, returning any errors that occurred during reading the results.
func (r *analyticsDeferredResultHandle) Close() error {
	r.rows.Close()
	return r.err
}

// One assigns the first value from the results into the value pointer.
func (r *analyticsDeferredResultHandle) One(valuePtr interface{}) error {
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

// Status triggers a network call to the handle URI, returning the current status of the long running query.
// TODO: how to deadline/timeout this?
func (r *analyticsDeferredResultHandle) Status() (string, error) {
	req := &gocbcore.HttpRequest{
		Service: gocbcore.CbasService,
		Path:    r.handleUri,
		Method:  "GET",
	}

	var resp *analyticsResponseHandle
	err := r.executeHandle(req, &resp)
	if err != nil {
		r.err = err
		return "", err
	}

	r.status = resp.Status
	r.handleUri = resp.Handle
	return r.status, nil
}

// TODO: how to deadline/timeout this?
func (r *analyticsDeferredResultHandle) executeHandle(req *gocbcore.HttpRequest, valuePtr interface{}) error {
	resp, err := r.provider.DoHttpRequest(req)
	if err != nil {
		return err
	}

	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(valuePtr)
	if err != nil {
		return err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	return nil
}

func createAnalyticsOpts(statement string, opts *AnalyticsQueryOptions) (map[string]interface{}, error) {
	execOpts := make(map[string]interface{})
	execOpts["statement"] = statement
	for k, v := range opts.options {
		execOpts[k] = v
	}

	if opts.positionalParams != nil {
		execOpts["args"] = opts.positionalParams
	} else if opts.namedParams != nil {
		for key, value := range opts.namedParams {
			if !strings.HasPrefix(key, "$") {
				key = "$" + key
			}
			execOpts[key] = value
		}
	}

	return execOpts, nil
}

// AnalyticsQuery performs an analytics query and returns a list of rows or an error.
func (c *Cluster) AnalyticsQuery(statement string, opts *AnalyticsQueryOptions) (AnalyticsResults, error) {
	if opts == nil {
		opts = &AnalyticsQueryOptions{}
	}
	ctx := opts.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	var span opentracing.Span
	if opts.parentSpanContext == nil {
		span = opentracing.GlobalTracer().StartSpan("ExecuteAnalyticsQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "cbas"})
	} else {
		span = opentracing.GlobalTracer().StartSpan("ExecuteAnalyticsQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "cbas"}, opentracing.ChildOf(opts.parentSpanContext))
	}
	defer span.Finish()

	provider, err := c.getQueryProvider()
	if err != nil {
		return nil, err
	}

	return c.analyticsQuery(ctx, span.Context(), statement, opts, provider)
}

func (c *Cluster) analyticsQuery(ctx context.Context, traceCtx opentracing.SpanContext, statement string, opts *AnalyticsQueryOptions,
	provider queryProvider) (resultsOut AnalyticsResults, errOut error) {

	queryOpts, err := createAnalyticsOpts(statement, opts)
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

	// TODO: clientcontextid?

	var retries uint
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			retries++
			var res AnalyticsResults
			res, err = c.executeAnalyticsQuery(ctx, traceCtx, queryOpts, provider)
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

func (c *Cluster) executeAnalyticsQuery(ctx context.Context, traceCtx opentracing.SpanContext, opts map[string]interface{},
	provider queryProvider) (AnalyticsResults, error) {

	// priority is sent as a header not in the body
	priority, priorityCastOK := opts["priority"].(int)
	if priorityCastOK {
		delete(opts, "priority")
	}

	reqJSON, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	req := &gocbcore.HttpRequest{
		Service: gocbcore.CbasService,
		Path:    "/analytics/service",
		Method:  "POST",
		Context: ctx,
		Body:    reqJSON,
	}

	if priorityCastOK {
		req.Headers = make(map[string]string)
		req.Headers["Analytics-Priority"] = strconv.Itoa(priority)
	}

	dtrace := opentracing.GlobalTracer().StartSpan("dispatch", opentracing.ChildOf(traceCtx))

	resp, err := provider.DoHttpRequest(req)
	if err != nil {
		dtrace.Finish()
		return nil, err
	}

	dtrace.Finish()

	strace := opentracing.GlobalTracer().StartSpan("streaming", opentracing.ChildOf(traceCtx))

	analyticsResp := analyticsResponse{}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&analyticsResp)
	if err != nil {
		strace.Finish()
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	strace.SetTag("couchbase.operation_id", analyticsResp.RequestID)
	strace.Finish()

	elapsedTime, err := time.ParseDuration(analyticsResp.Metrics.ElapsedTime)
	if err != nil {
		logDebugf("Failed to parse elapsed time duration (%s)", err)
	}

	executionTime, err := time.ParseDuration(analyticsResp.Metrics.ExecutionTime)
	if err != nil {
		logDebugf("Failed to parse execution time duration (%s)", err)
	}

	if len(analyticsResp.Errors) > 0 {
		return nil, (*analyticsMultiError)(&analyticsResp.Errors)
	}

	if resp.StatusCode != 200 {
		// return nil, &viewError{
		// 	Message: "HTTP Error",
		// 	Reason:  fmt.Sprintf("Status code was %d.", resp.StatusCode),
		// }	TODO
	}

	return &analyticsResults{
		requestID:       analyticsResp.RequestID,
		clientContextID: analyticsResp.ClientContextID,
		rows: &analyticsRows{
			rows:  analyticsResp.Results,
			index: -1,
		},
		signature: analyticsResp.Signature,
		status:    analyticsResp.Status,
		warnings:  analyticsResp.Warnings,
		metrics: AnalyticsResultMetrics{
			ElapsedTime:      elapsedTime,
			ExecutionTime:    executionTime,
			ResultCount:      analyticsResp.Metrics.ResultCount,
			ResultSize:       analyticsResp.Metrics.ResultSize,
			MutationCount:    analyticsResp.Metrics.MutationCount,
			SortCount:        analyticsResp.Metrics.SortCount,
			ErrorCount:       analyticsResp.Metrics.ErrorCount,
			WarningCount:     analyticsResp.Metrics.WarningCount,
			ProcessedObjects: analyticsResp.Metrics.ProcessedObjects,
		},
		handle: &analyticsDeferredResultHandle{
			handleUri: analyticsResp.Handle,
			rows: &analyticsRows{
				index: -1,
			},
			status:   analyticsResp.Status,
			provider: provider,
		},
	}, nil
}
