package gocb

import (
	"encoding/json"
	"errors"
)

type ProjectionResult struct {
	path string
	data []byte
	err  error
}

// DocumentProjection represents multiple chunks of a full Document.
type DocumentProjection struct {
	cas      Cas
	contents []ProjectionResult
	pathMap  map[string]int
}

func (pr *ProjectionResult) As(valuePtr interface{}) error {
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

func (pr *ProjectionResult) Exists() bool {
	err := pr.As(nil)
	return err == nil
}

// Cas returns the Cas of the Document
func (dp *DocumentProjection) Cas() Cas {
	return dp.cas
}

// ContentByIndex retrieves the value of the operation by its index. The index is the position of
// the operation as it was added to the builder.
func (dp *DocumentProjection) ContentByIndex(idx int) *ProjectionResult {
	return &dp.contents[idx]
}

// Content retrieves the value of the operation by its path. The path is the path provided
// to the operation
func (dp *DocumentProjection) Content(path string) *ProjectionResult {
	if dp.pathMap == nil {
		dp.pathMap = make(map[string]int)
		for i, v := range dp.contents {
			dp.pathMap[v.path] = i
		}
	}

	content, ok := dp.pathMap[path]
	if !ok {
		return &ProjectionResult{
			err: errors.New("Nope"),
		}
	}
	return dp.ContentByIndex(content)
}

func (dp *DocumentProjection) As(valuePtr interface{}) error {
	return json.Unmarshal(dp.contents[0].data, valuePtr)
}
