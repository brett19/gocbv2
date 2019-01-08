package gocb

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/opentracing/opentracing-go"
)

// SpatialViewOptions represents the options available when executing a spatial query.
type SpatialViewOptions struct {
	options           url.Values
	development       bool
	ctx               context.Context
	parentSpanContext opentracing.SpanContext
}

// Stale specifies the level of consistency required for this query.
func (vq *SpatialViewOptions) Stale(stale StaleMode) *SpatialViewOptions {
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
func (vq *SpatialViewOptions) Skip(num uint) *SpatialViewOptions {
	vq.options.Set("skip", strconv.FormatUint(uint64(num), 10))
	return vq
}

// Limit specifies a limit on the number of results to return.
func (vq *SpatialViewOptions) Limit(num uint) *SpatialViewOptions {
	vq.options.Set("limit", strconv.FormatUint(uint64(num), 10))
	return vq
}

// Bbox specifies the bounding region to use for the spatial query.
func (vq *SpatialViewOptions) Bbox(bounds []float64) *SpatialViewOptions {
	if len(bounds) == 4 {
		vq.options.Set("bbox", fmt.Sprintf("%f,%f,%f,%f", bounds[0], bounds[1], bounds[2], bounds[3]))
	} else {
		vq.options.Del("bbox")
	}
	return vq
}

// Development specifies whether to query the production or development design document.
func (vq *SpatialViewOptions) Development(val bool) *SpatialViewOptions {
	// if val {
	// 	if !strings.HasPrefix(vq.ddoc, "dev_") {
	// 		vq.ddoc = "dev_" + vq.ddoc
	// 	}
	// } else {
	// 	vq.ddoc = strings.TrimPrefix(vq.ddoc, "dev_")
	// }
	vq.development = val
	return vq
}

// Custom allows specifying custom query options.
func (vq *SpatialViewOptions) Custom(name, value string) *SpatialViewOptions {
	vq.options.Set(name, value)
	return vq
}

// WithContext sets the context.Context to used for this query.
func (vq *SpatialViewOptions) WithContext(ctx context.Context) *SpatialViewOptions {
	vq.ctx = ctx
	return vq
}

// WithParentSpanContext set the opentracing.SpanContext to use for this query.
func (vq *SpatialViewOptions) WithParentSpanContext(ctx opentracing.SpanContext) *SpatialViewOptions {
	vq.parentSpanContext = ctx
	return vq
}

// NewSpatialQueryOptions creates a new SpatialViewOptions object.
func NewSpatialQueryOptions() *SpatialViewOptions {
	return &SpatialViewOptions{
		options: url.Values{},
	}
}
