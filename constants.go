package gocb

import (
	"gopkg.in/couchbase/gocbcore.v7"
)

// ServiceType specifies a particular Couchbase service type.
type ServiceType gocbcore.ServiceType

const (
	// MemdService represents a memcached service.
	MemdService = ServiceType(gocbcore.MemdService)

	// MgmtService represents a management service (typically ns_server).
	MgmtService = ServiceType(gocbcore.MgmtService)

	// CapiService represents a CouchAPI service (typically for views).
	CapiService = ServiceType(gocbcore.CapiService)

	// N1qlService represents a N1QL service (typically for query).
	N1qlService = ServiceType(gocbcore.N1qlService)

	// FtsService represents a full-text-search service.
	FtsService = ServiceType(gocbcore.FtsService)

	// CbasService represents an analytics service.
	CbasService = ServiceType(gocbcore.CbasService)
)

