package gocb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/opentracing/opentracing-go"
	gocbcore "gopkg.in/couchbase/gocbcore.v7"
)

type viewError struct {
	Message string `json:"message"`
	Reason  string `json:"reason"`
}

type viewResponse struct {
	TotalRows int               `json:"total_rows,omitempty"`
	Rows      []json.RawMessage `json:"rows,omitempty"`
	Error     string            `json:"error,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	Errors    []viewError       `json:"errors,omitempty"`
}

func (e *viewError) Error() string {
	return e.Message + " - " + e.Reason
}

// ViewResults implements an iterator interface which can be used to iterate over the rows of the query results.
type ViewResults interface {
	One(valuePtr interface{}) error
	Next(valuePtr interface{}) bool
	NextBytes() []byte
	Close() error
}

// ViewResultMetrics allows access to the TotalRows value from the view response.  This is
// implemented as an additional interface to maintain ABI compatibility for the 1.x series.
type ViewResultMetrics interface {
	TotalRows() int
}

type viewResults struct {
	index     int
	rows      []json.RawMessage
	totalRows int
	err       error
	endErr    error
}

func (r *viewResults) Next(valuePtr interface{}) bool {
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

func (r *viewResults) NextBytes() []byte {
	if r.err != nil {
		return nil
	}

	if r.index+1 >= len(r.rows) {
		return nil
	}
	r.index++

	return r.rows[r.index]
}

func (r *viewResults) Close() error {
	if r.err != nil {
		return r.err
	}

	if r.endErr != nil {
		return r.endErr
	}

	return nil
}

func (r *viewResults) One(valuePtr interface{}) error {
	if !r.Next(valuePtr) {
		err := r.Close()
		if err != nil {
			return err
		}
		// return ErrNoResults TODO
	}

	// Ignore any errors occurring after we already have our result
	err := r.Close()
	if err != nil {
		// Return no error as we got the one result already.
		return nil
	}

	return nil
}

func (r *viewResults) TotalRows() int {
	return r.totalRows
}

// ViewQuery performs a view query and returns a list of rows or an error.
func (b *Bucket) ViewQuery(designDoc string, viewName string, opts *ViewOptions) (ViewResults, error) {
	if opts == nil {
		opts = &ViewOptions{}
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	var span opentracing.Span
	if opts.ParentSpanContext == nil {
		span = opentracing.GlobalTracer().StartSpan("ExecuteViewQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "views"})
	} else {
		span = opentracing.GlobalTracer().StartSpan("ExecuteViewQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "views"}, opentracing.ChildOf(opts.ParentSpanContext))
	}
	defer span.Finish()

	provider, err := b.sb.cluster.getQueryProvider()
	if err != nil {
		return nil, err
	}

	designDoc = b.maybePrefixDevDocument(opts.Development, designDoc)

	urlValues, err := opts.toURLValues()
	if err != nil {
		return nil, err
	}

	return b.executeViewQuery(ctx, span.Context(), "_view", designDoc, viewName, *urlValues, provider)
}

// SpatialViewQuery performs a spatial query and returns a list of rows or an error.
func (b *Bucket) SpatialViewQuery(designDoc string, viewName string, opts *SpatialViewOptions) (ViewResults, error) {
	if opts == nil {
		opts = &SpatialViewOptions{}
	}
	ctx := opts.Context
	if ctx == nil {
		ctx = context.Background()
	}

	var span opentracing.Span
	if opts.ParentSpanContext == nil {
		span = opentracing.GlobalTracer().StartSpan("ExecuteSpatialQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "views"})
	} else {
		span = opentracing.GlobalTracer().StartSpan("ExecuteSpatialQuery",
			opentracing.Tag{Key: "couchbase.service", Value: "views"}, opentracing.ChildOf(opts.ParentSpanContext))
	}
	defer span.Finish()

	provider, err := b.sb.cluster.getQueryProvider()
	if err != nil {
		return nil, err
	}

	designDoc = b.maybePrefixDevDocument(opts.Development, designDoc)

	urlValues, err := opts.toURLValues()
	if err != nil {
		return nil, err
	}

	return b.executeViewQuery(ctx, span.Context(), "_spatial", designDoc, viewName, *urlValues, provider)
}

func (b *Bucket) executeViewQuery(ctx context.Context, traceCtx opentracing.SpanContext, viewType, ddoc, viewName string,
	options url.Values, provider queryProvider) (ViewResults, error) {

	reqUri := fmt.Sprintf("/_design/%s/%s/%s?%s", ddoc, viewType, viewName, options.Encode())
	req := &gocbcore.HttpRequest{
		Service: gocbcore.CapiService,
		Path:    reqUri,
		Method:  "GET",
		Context: ctx,
	}

	dtrace := opentracing.GlobalTracer().StartSpan("dispatch", opentracing.ChildOf(traceCtx))

	resp, err := provider.DoHttpRequest(req)
	if err != nil {
		dtrace.Finish()
		return nil, err
	}

	dtrace.Finish()

	strace := opentracing.GlobalTracer().StartSpan("streaming",
		opentracing.ChildOf(traceCtx))

	viewResp := viewResponse{}
	jsonDec := json.NewDecoder(resp.Body)
	err = jsonDec.Decode(&viewResp)
	if err != nil {
		strace.Finish()
		return nil, err
	}

	err = resp.Body.Close()
	if err != nil {
		logDebugf("Failed to close socket (%s)", err)
	}

	strace.Finish()

	if resp.StatusCode != 200 {
		if viewResp.Error != "" {
			// return nil, &viewError{
			// 	Message: viewResp.Error,
			// 	Reason:  viewResp.Reason,
			// } TODO: errors
		}

		// return nil, &viewError{
		// 	Message: "HTTP Error",
		// 	Reason:  fmt.Sprintf("Status code was %d.", resp.StatusCode),
		// }
	}

	// var endErrs MultiError
	// for _, endErr := range viewResp.Errors {
	// 	endErrs.add(&viewError{
	// 		Message: endErr.Message,
	// 		Reason:  endErr.Reason,
	// 	})
	// }

	return &viewResults{
		index:     -1,
		rows:      viewResp.Rows,
		totalRows: viewResp.TotalRows,
		// endErr:    endErrs.get(),
	}, nil
}

func (b *Bucket) maybePrefixDevDocument(val bool, ddoc string) string {
	designDoc := ddoc
	if val {
		if !strings.HasPrefix(ddoc, "dev_") {
			designDoc = "dev_" + ddoc
		}
	} else {
		designDoc = strings.TrimPrefix(ddoc, "dev_")
	}

	return designDoc
}
