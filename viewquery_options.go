package gocb

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"strconv"

	"github.com/opentracing/opentracing-go"
)

// StaleMode specifies the consistency required for a view query.
type StaleMode int

const (
	// Before indicates to update the index before querying it.
	Before = StaleMode(1)
	// None indicates that no special behaviour should be used.
	None = StaleMode(2)
	// After indicates to update the index asynchronously after querying.
	After = StaleMode(3)
)

// SortOrder specifies the ordering for the view queries results.
type SortOrder int

const (
	// Ascending indicates the query results should be sorted from lowest to highest.
	Ascending = SortOrder(1)
	// Descending indicates the query results should be sorted from highest to lowest.
	Descending = SortOrder(2)
)

// ViewOptions represents the options available when executing view query.
type ViewOptions struct {
	options           url.Values
	development       bool
	ctx               context.Context
	parentSpanContext opentracing.SpanContext
	// errs    MultiError TODO
}

func (vq *ViewOptions) marshalJson(value interface{}) []byte {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	err := enc.Encode(value)
	if err != nil {
		// vq.errs.add(err)
		return nil
	}
	return buf.Bytes()
}

// Stale specifies the level of consistency required for this query.
func (vq *ViewOptions) Stale(stale StaleMode) *ViewOptions {
	if stale == Before {
		vq.options.Set("stale", "false")
	} else if stale == None {
		vq.options.Set("stale", "ok")
	} else if stale == After {
		vq.options.Set("stale", "update_after")
	} else {
		panic("Unexpected stale option")
	}
	return vq
}

// Skip specifies how many results to skip at the beginning of the result list.
func (vq *ViewOptions) Skip(num uint) *ViewOptions {
	vq.options.Set("skip", strconv.FormatUint(uint64(num), 10))
	return vq
}

// Limit specifies a limit on the number of results to return.
func (vq *ViewOptions) Limit(num uint) *ViewOptions {
	vq.options.Set("limit", strconv.FormatUint(uint64(num), 10))
	return vq
}

// Order specifies the order to sort the view results in.
func (vq *ViewOptions) Order(order SortOrder) *ViewOptions {
	if order == Ascending {
		vq.options.Set("descending", "false")
	} else if order == Descending {
		vq.options.Set("descending", "true")
	} else {
		panic("Unexpected order option")
	}
	return vq
}

// Reduce specifies whether to run the reduce part of the map-reduce.
func (vq *ViewOptions) Reduce(reduce bool) *ViewOptions {
	if reduce == true {
		vq.options.Set("reduce", "true")
	} else {
		vq.options.Set("reduce", "false")
	}
	return vq
}

// Group specifies whether to group the map-reduce results.
func (vq *ViewOptions) Group(useGrouping bool) *ViewOptions {
	if useGrouping {
		vq.options.Set("group", "true")
	} else {
		vq.options.Set("group", "false")
	}
	return vq
}

// GroupLevel specifies at what level to group the map-reduce results.
func (vq *ViewOptions) GroupLevel(groupLevel uint) *ViewOptions {
	vq.options.Set("group_level", strconv.FormatUint(uint64(groupLevel), 10))
	return vq
}

// Key specifies a specific key to retrieve from the index.
func (vq *ViewOptions) Key(key interface{}) *ViewOptions {
	jsonKey := vq.marshalJson(key)
	vq.options.Set("key", string(jsonKey))
	return vq
}

// Keys specifies a list of specific keys to retrieve from the index.
func (vq *ViewOptions) Keys(keys []interface{}) *ViewOptions {
	jsonKeys := vq.marshalJson(keys)
	vq.options.Set("keys", string(jsonKeys))
	return vq
}

// Range specifies a value range to get results within.
func (vq *ViewOptions) Range(start, end interface{}, inclusiveEnd bool) *ViewOptions {
	// TODO(brett19): Not currently handling errors due to no way to return the error
	if start != nil {
		jsonStartKey := vq.marshalJson(start)
		vq.options.Set("startkey", string(jsonStartKey))
	} else {
		vq.options.Del("startkey")
	}
	if end != nil {
		jsonEndKey := vq.marshalJson(end)
		vq.options.Set("endkey", string(jsonEndKey))
	} else {
		vq.options.Del("endkey")
	}
	if start != nil || end != nil {
		if inclusiveEnd {
			vq.options.Set("inclusive_end", "true")
		} else {
			vq.options.Set("inclusive_end", "false")
		}
	} else {
		vq.options.Del("inclusive_end")
	}
	return vq
}

// IdRange specifies a range of document id's to get results within.
// Usually requires Range to be specified as well.
func (vq *ViewOptions) IdRange(start, end string) *ViewOptions {
	if start != "" {
		vq.options.Set("startkey_docid", start)
	} else {
		vq.options.Del("startkey_docid")
	}
	if end != "" {
		vq.options.Set("endkey_docid", end)
	} else {
		vq.options.Del("endkey_docid")
	}
	return vq
}

// Development specifies whether to query the production or development design document.
func (vq *ViewOptions) Development(val bool) *ViewOptions {
	vq.development = val
	return vq
}

// Custom allows specifying custom query options.
func (vq *ViewOptions) Custom(name, value string) *ViewOptions {
	vq.options.Set(name, value)
	return vq
}

// WithContext sets the context.Context to used for this query.
func (vq *ViewOptions) WithContext(ctx context.Context) *ViewOptions {
	vq.ctx = ctx
	return vq
}

// WithParentSpanContext set the opentracing.SpanContext to use for this query.
func (vq *ViewOptions) WithParentSpanContext(ctx opentracing.SpanContext) *ViewOptions {
	vq.parentSpanContext = ctx
	return vq
}

// NewViewQueryOptions creates a new ViewOptions object.
func NewViewQueryOptions() *ViewOptions {
	return &ViewOptions{
		options: url.Values{},
	}
}
