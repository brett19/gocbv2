package gocb

import (
	"fmt"
	"strings"

	"gopkg.in/couchbase/gocbcore.v7"

	"github.com/pkg/errors"
)

type retryAbleError interface {
	isRetryable() bool
}

type bucketNotFoundError struct {
}

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
	if kverr, ok := cause.(*gocbcore.KvError); ok {
		return gocbcore.IsErrorStatus(kverr, gocbcore.StatusScopeUnknown)
	}

	return false
}

// IsCollectionUnknownError verifies whether or not the cause for an error is scope unknown
func IsCollectionUnknownError(err error) bool {
	cause := errors.Cause(err)
	if kverr, ok := cause.(*gocbcore.KvError); ok {
		return gocbcore.IsErrorStatus(kverr, gocbcore.StatusCollectionUnknown)
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

type HTTPError interface {
	error
	StatusCode() int
}

type httpError struct {
	statusCode int
}

func (e httpError) Error() string {
	return fmt.Sprintf("an http error occurred with status code: %d", e.statusCode)
}

func (e httpError) StatusCode() int {
	return e.statusCode
}

type ViewQueryError interface {
	error
	Reason() string
	Message() string
}

type viewError struct {
	ErrorMessage string `json:"message"`
	ErrorReason  string `json:"reason"`
}

func (e *viewError) Error() string {
	return e.ErrorMessage + " - " + e.ErrorReason
}

func (e *viewError) Reason() string {
	return e.ErrorReason
}

func (e *viewError) Message() string {
	return e.ErrorMessage
}

type ViewQueryErrors interface {
	error
	Errors() []ViewQueryError
	HTTPStatus() int
	Endpoint() string
}

type viewMultiError struct {
	errors     []ViewQueryError
	httpStatus int
	endpoint   string
}

func (e viewMultiError) Error() string {
	var errs []string
	for _, err := range e.errors {
		errs = append(errs, err.Error())
	}
	return strings.Join(errs, ", ")
}

func (e viewMultiError) HTTPStatus() int {
	return e.httpStatus
}

func (e viewMultiError) Endpoint() string {
	return e.endpoint
}

func (e viewMultiError) Errors() []ViewQueryError {
	return e.errors
}

type AnalyticsQueryError interface {
	error
	Code() uint32
	Message() string
}

type analyticsQueryError struct {
	ErrorCode    uint32 `json:"code"`
	ErrorMessage string `json:"msg"`
}

func (e analyticsQueryError) Error() string {
	return fmt.Sprintf("[%d] %s", e.ErrorCode, e.ErrorMessage)
}

func (e analyticsQueryError) Code() uint32 {
	return e.ErrorCode
}

func (e analyticsQueryError) Message() string {
	return e.ErrorMessage
}

func (e analyticsQueryError) isRetryable() bool {
	if e.Code() != 21002 && e.Code() != 23000 && e.Code() != 23003 && e.Code() != 23007 {
		return true
	}

	return false
}

type AnalyticsQueryErrors interface {
	error
	Errors() []AnalyticsQueryError
	HTTPStatus() int
	Endpoint() string
	ContextID() string
}

type analyticsQueryMultiError struct {
	errors     []AnalyticsQueryError
	httpStatus int
	endpoint   string
	contextID  string
}

func (e analyticsQueryMultiError) isRetryable() bool {
	for _, aErr := range e.errors {
		if isRetryableError(aErr) {
			return true
		}
	}

	return false
}

func (e analyticsQueryMultiError) Error() string {
	var errs []string
	for _, err := range e.errors {
		errs = append(errs, err.Error())
	}
	return strings.Join(errs, ", ")
}

func (e analyticsQueryMultiError) HTTPStatus() int {
	return e.httpStatus
}

func (e analyticsQueryMultiError) Endpoint() string {
	return e.endpoint
}

func (e analyticsQueryMultiError) ContextID() string {
	return e.contextID
}

func (e analyticsQueryMultiError) Errors() []AnalyticsQueryError {
	return e.errors
}

type QueryError interface {
	error
	Code() uint32
	Message() string
}

type queryError struct {
	ErrorCode    uint32 `json:"code"`
	ErrorMessage string `json:"msg"`
}

func (e queryError) Error() string {
	return fmt.Sprintf("[%d] %s", e.ErrorCode, e.ErrorMessage)
}

func (e queryError) Code() uint32 {
	return e.ErrorCode
}

func (e queryError) Message() string {
	return e.ErrorMessage
}

func (e queryError) isRetryable() bool {
	if e.ErrorCode == 4050 || e.ErrorCode == 4070 || e.ErrorCode == 5000 {
		return true
	}

	return false
}

type QueryErrors interface {
	error
	Errors() []QueryError
	HTTPStatus() int
	Endpoint() string
	ContextID() string
}

type queryMultiError struct {
	errors     []QueryError
	httpStatus int
	endpoint   string
	contextID  string
}

func (e queryMultiError) isRetryable() bool {
	for _, n1qlErr := range e.errors {
		if isRetryableError(n1qlErr) {
			return true
		}
	}

	return false
}

func (e queryMultiError) Error() string {
	var errs []string
	for _, err := range e.errors {
		errs = append(errs, err.Error())
	}
	return strings.Join(errs, ", ")
}

func (e queryMultiError) HTTPStatus() int {
	return e.httpStatus
}

func (e queryMultiError) Endpoint() string {
	return e.endpoint
}

func (e queryMultiError) ContextID() string {
	return e.contextID
}

func (e queryMultiError) Errors() []QueryError {
	return e.errors
}

// ErrorCause returns the underlying cause of an error.
func ErrorCause(err error) error {
	return errors.Cause(err)
}

func isRetryableError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case retryAbleError:
		return errType.isRetryable()
	default:
		return false
	}
}
