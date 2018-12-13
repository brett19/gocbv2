package gocb

import (
	"encoding/json"
)

// LookupInResult is the return type for LookupIn.
type LookupInResult struct {
	cas      Cas
	contents []lookupInPartial
	pathMap  map[string]int
}

type lookupInPartial struct {
	data json.RawMessage
	err  error
}

func (pr *lookupInPartial) as(valuePtr interface{}) error {
	if pr.err != nil {
		return pr.err
	}

	if valuePtr == nil {
		return nil
	}

	if valuePtr, ok := valuePtr.(*[]byte); ok {
		*valuePtr = pr.data
		return nil
	}

	return json.Unmarshal(pr.data, valuePtr)
}

func (pr *lookupInPartial) exists() bool {
	err := pr.as(nil)
	return err == nil
}

// Cas returns the Cas of the Document
func (lir *LookupInResult) Cas() Cas {
	return lir.cas
}

// ContentAt retrieves the value of the operation by its index. The index is the position of
// the operation as it was added to the builder.
func (lir *LookupInResult) ContentAt(idx int, valuePtr interface{}) error {
	return lir.contents[idx].as(valuePtr)
}

// Exists verifies that the item at idx exists.
func (lir *LookupInResult) Exists(idx int) bool {
	return lir.contents[idx].exists()
}
