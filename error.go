package gocb

import (
	"gopkg.in/couchbase/gocbcore.v7"

	"github.com/pkg/errors"
)

type retryAbleError interface {
	isRetryable() bool
}

// type QueryError struct {
// 	errors     []n1qlError
// 	HTTPStatus uint16
// 	ContextId  string
// 	Endpoint   string
// 	Payload    interface{}
// }

type kvError struct {
	err error
}

// IsCasMismatchError verifies whether or not the cause for an error is a cas mismatch
func IsCasMismatchError(err error) bool {
	cause := errors.Cause(err)
	if kvError, ok := cause.(*gocbcore.KvError); ok {
		return gocbcore.IsErrorStatus(kvError, gocbcore.StatusKeyExists)
	}

	return false
}

// IsScopeUnknownError verifies whether or not the cause for an error is scope unknown
func IsScopeUnknownError(err error) bool {
	cause := errors.Cause(err)
	if kvError, ok := cause.(*gocbcore.KvError); ok {
		return gocbcore.IsErrorStatus(kvError, gocbcore.StatusScopeUnknown)
	}

	return false
}

// IsCollectionUnknownError verifies whether or not the cause for an error is scope unknown
func IsCollectionUnknownError(err error) bool {
	cause := errors.Cause(err)
	if kvError, ok := cause.(*gocbcore.KvError); ok {
		return gocbcore.IsErrorStatus(kvError, gocbcore.StatusCollectionUnknown)
	}

	return false
}

func (err kvError) Error() string {
	return err.err.Error()
}

func (err kvError) Cause() error {
	return err.err
}

// TimeoutError occurs when an operation times out
type timeoutError struct {
	err error
}

func (err timeoutError) Error() string {
	return err.err.Error()
}

// IsTimeoutError verifies whether or not the cause for an error is a timeout
func IsTimeoutError(err error) bool {
	cause := errors.Cause(err)
	if _, ok := cause.(timeoutError); ok {
		return true
	}

	return false
}

// func (qe QueryError) isRetryable() bool {
// 	for _, n1qlErr := range qe.errors {
// 		if n1qlErr.Code == 4050 || n1qlErr.Code == 4070 || n1qlErr.Code == 5000 {
// 			return true
// 		}
// 	}

// 	return false
// }

// func (qe QueryError) Error() string {
// 	var errors []string
// 	for _, err := range qe.errors {
// 		errors = append(errors, err.Error())
// 	}
// 	return strings.Join(errors, ", ")
// }

func isRetryableError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case retryAbleError:
		return errType.isRetryable()
	default:
		return false
	}
}
