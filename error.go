package gocb

import (
	"fmt"

	"gopkg.in/couchbase/gocbcore.v7"

	"github.com/pkg/errors"
)

// BucketMissingError occurs when the application attempts to open or use a bucket which does exist or is not available at that time.
type BucketMissingError struct {
	bucketName string
}

func (bme BucketMissingError) Error() string {
	return fmt.Sprintf("The requested bucket %s cannot be found", bme.bucketName)
}

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

type KvError struct {
	err error
}

func IsCasMismatch(err error) bool {
	cause := errors.Cause(err)
	if kvError, ok := cause.(*gocbcore.KvError); ok {
		return gocbcore.IsErrorStatus(kvError, gocbcore.StatusKeyExists)
	}

	return false
}

func (err KvError) Error() string {
	return err.err.Error()
}

func (err KvError) Cause() error {
	return err.err
}

// TimeoutError occurs when an operation times out
type TimeoutError struct {
	err error
}

func (err TimeoutError) Error() string {
	return err.err.Error()
}

func IsTimeoutError(err error) bool {
	cause := errors.Cause(err)
	if _, ok := cause.(TimeoutError); ok {
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
