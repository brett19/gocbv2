package gocb

import (
	"gopkg.in/couchbase/gocbcore.v7"
)

// ExistsResult is the return type of Exist operations.
type ExistsResult struct {
	id       string
	cas      Cas
	keyState gocbcore.KeyState
}

// Cas returns the cas of the result.
func (d *ExistsResult) Cas() Cas {
	return d.cas
}

// Exists returns whether or not the document exists.
func (d *ExistsResult) Exists() bool {
	return d.keyState == gocbcore.KeyStateNotFound
}
