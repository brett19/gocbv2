package gocb

import (
	"context"
	"time"

	"github.com/opentracing/opentracing-go"
)

// AnalyticsQueryOptions is the set of options available to an Analytics query.
type AnalyticsQueryOptions struct {
	options           map[string]interface{}
	positionalParams  []interface{}
	namedParams       map[string]interface{}
	ctx               context.Context
	parentSpanContext opentracing.SpanContext
}

// NewAnalyticsQuery creates a new AnalyticsQuery .
func NewAnalyticsQuery() *AnalyticsQueryOptions {
	nq := &AnalyticsQueryOptions{
		options:          make(map[string]interface{}),
		positionalParams: nil,
		namedParams:      nil,
	}
	return nq
}

// ServerSideTimeout indicates the maximum time to wait for this query to complete.
func (aq *AnalyticsQueryOptions) ServerSideTimeout(timeout time.Duration) *AnalyticsQueryOptions {
	aq.options["timeout"] = timeout.String()
	return aq
}

// Pretty indicates whether the response should be nicely formatted.
func (aq *AnalyticsQueryOptions) Pretty(pretty bool) *AnalyticsQueryOptions {
	aq.options["pretty"] = pretty
	return aq
}

// ContextID sets the client context id for the request, for use with tracing.
func (aq *AnalyticsQueryOptions) ContextID(clientContextID string) *AnalyticsQueryOptions {
	aq.options["client_context_id"] = clientContextID
	return aq
}

// RawParam allows specifying custom query options.
func (aq *AnalyticsQueryOptions) RawParam(name string, value interface{}) *AnalyticsQueryOptions {
	aq.options[name] = value
	return aq
}

// Priority sets whether or not the query should be run with priority status.
func (aq *AnalyticsQueryOptions) Priority(priority bool) *AnalyticsQueryOptions {
	if priority {
		aq.options["priority"] = -1
	} else {
		delete(aq.options, "priority")
	}
	return aq
}

// PositionalParameters adds a set of positional parameters to be used in this query.
func (aq *AnalyticsQueryOptions) PositionalParameters(params []interface{}) *AnalyticsQueryOptions {
	aq.positionalParams = params
	return aq
}

// NamedParameters adds a set of positional parameters to be used in this query.
func (aq *AnalyticsQueryOptions) NamedParameters(params map[string]interface{}) *AnalyticsQueryOptions {
	aq.namedParams = params
	return aq
}

// Deferred sets whether or not the query should be run as a deferred query.
//
// Experimental: This API is subject to change at any time.
func (aq *AnalyticsQueryOptions) Deferred(deferred bool) *AnalyticsQueryOptions {
	if deferred {
		aq.options["mode"] = "async"
	} else {
		delete(aq.options, "mode")
	}
	return aq
}
