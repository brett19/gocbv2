package gocb

import (
	"context"
	"strconv"
	"time"

	"github.com/opentracing/opentracing-go"
)

// ConsistencyMode indicates the level of data consistency desired for a query.
type ConsistencyMode int

const (
	// NotBounded indicates no data consistency is required.
	NotBounded = ConsistencyMode(1)
	// RequestPlus indicates that request-level data consistency is required.
	RequestPlus = ConsistencyMode(2)
	// StatementPlus indicates that statement-level data consistency is required.
	StatementPlus = ConsistencyMode(3)
)

// QueryOptions represents a pending N1QL query.
type QueryOptions struct {
	options           map[string]interface{}
	positionalParams  []interface{}
	namedParams       map[string]interface{}
	prepared          bool
	ctx               context.Context
	parentSpanContext opentracing.SpanContext
}

// Consistency specifies the level of consistency required for this query.
func (nq *QueryOptions) Consistency(stale ConsistencyMode) *QueryOptions {
	if _, ok := nq.options["scan_vectors"]; ok {
		panic("Consistent and ConsistentWith must be used exclusively")
	}
	if stale == NotBounded {
		nq.options["scan_consistency"] = "not_bounded"
	} else if stale == RequestPlus {
		nq.options["scan_consistency"] = "request_plus"
	} else if stale == StatementPlus {
		nq.options["scan_consistency"] = "statement_plus"
	} else {
		panic("Unexpected consistency option")
	}
	return nq
}

// ConsistentWith specifies a mutation state to be consistent with for this query.
func (nq *QueryOptions) ConsistentWith(state *MutationState) *QueryOptions {
	if _, ok := nq.options["scan_consistency"]; ok {
		panic("Consistent and ConsistentWith must be used exclusively")
	}
	nq.options["scan_consistency"] = "at_plus"
	nq.options["scan_vectors"] = state
	return nq
}

// Prepared specifies that this query should be prepared.
func (nq *QueryOptions) Prepared(prepared bool) *QueryOptions {
	nq.prepared = prepared
	return nq
}

// Profile specifies a profiling mode to use for this N1QL query.
func (nq *QueryOptions) Profile(profileMode QueryProfileType) *QueryOptions {
	nq.options["profile"] = profileMode
	return nq
}

// ScanCap specifies the maximum buffered channel size between the indexer
// client and the query service for index scans. This parameter controls
// when to use scan backfill. Use 0 or a negative number to disable.
func (nq *QueryOptions) ScanCap(scanCap int) *QueryOptions {
	nq.options["scan_cap"] = strconv.Itoa(scanCap)
	return nq
}

// PipelineBatch controls the number of items execution operators can
// batch for fetch from the KV node.
func (nq *QueryOptions) PipelineBatch(pipelineBatch int) *QueryOptions {
	nq.options["pipeline_batch"] = strconv.Itoa(pipelineBatch)
	return nq
}

// PipelineCap controls the maximum number of items each execution operator
// can buffer between various operators.
func (nq *QueryOptions) PipelineCap(pipelineCap int) *QueryOptions {
	nq.options["pipeline_cap"] = strconv.Itoa(pipelineCap)
	return nq
}

// ReadOnly controls whether a query can change a resulting recordset.  If
// readonly is true, then only SELECT statements are permitted.
func (nq *QueryOptions) ReadOnly(readOnly bool) *QueryOptions {
	nq.options["readonly"] = readOnly
	return nq
}

// Custom allows specifying custom query options.
func (nq *QueryOptions) Custom(name string, value interface{}) *QueryOptions {
	nq.options[name] = value
	return nq
}

// Timeout indicates the maximum time to wait for this query to complete.
func (nq *QueryOptions) Timeout(timeout time.Duration) *QueryOptions {
	nq.options["timeout"] = timeout.String()
	return nq
}

// WithContext sets the context.Context to used for this query.
func (nq *QueryOptions) WithContext(ctx context.Context) *QueryOptions {
	nq.ctx = ctx
	return nq
}

// WithParentSpanContext set the opentracing.SpanContext to use for this query.
func (nq *QueryOptions) WithParentSpanContext(ctx opentracing.SpanContext) *QueryOptions {
	nq.parentSpanContext = ctx
	return nq
}

// PositionalParameters adds a set of positional parameters to be used in this query.
func (nq *QueryOptions) PositionalParameters(params []interface{}) *QueryOptions {
	nq.positionalParams = params
	return nq
}

// NamedParameters adds a set of positional parameters to be used in this query.
func (nq *QueryOptions) NamedParameters(params map[string]interface{}) *QueryOptions {
	nq.namedParams = params
	return nq
}

// NewQueryOptions creates a new QueryOptions object.
func NewQueryOptions() *QueryOptions {
	nq := &QueryOptions{
		options:          make(map[string]interface{}),
		positionalParams: nil,
		namedParams:      nil,
	}
	return nq
}
