package gocb

import (
	"fmt"
	"strings"

	"gopkg.in/couchbase/gocbcore.v7"

	"github.com/pkg/errors"
)

type retryAbleError interface {
	retryable() bool
}

// KeyValueError represents an error that occurred while
// executing a K/V operation. Assumes that the service has returned a response.
type KeyValueError interface {
	error
	ID() string
	StatusCode() int // ?
	Opaque() uint32
}

type kvError struct {
	id          string
	status      gocbcore.StatusCode
	description string
	opaque      uint32
}

func (err kvError) Error() string {
	return err.description
}

func (err kvError) StatusCode() int {
	return int(err.status)
}

func (err kvError) ID() string {
	return err.id
}

func (err kvError) Opaque() uint32 {
	return err.opaque
}

// IsScopeUnknownError verifies whether or not the cause for an error is scope unknown
func IsScopeUnknownError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(kvError); ok {
		return kvErr.status == gocbcore.StatusScopeUnknown
	}

	return false
}

// IsCollectionUnknownError verifies whether or not the cause for an error is scope unknown
func IsCollectionUnknownError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(kvError); ok {
		return kvErr.status == gocbcore.StatusCollectionUnknown
	}

	return false

}

// IsKeyExistsError indicates whether the passed error is a
// key-value "Key Already Exists" error.
func IsKeyExistsError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(kvError); ok {
		return kvErr.status == gocbcore.StatusKeyExists
	}

	return false
}

// IsKeyNotFoundError indicates whether the passed error is a
// key-value "Key Not Found" error.
func IsKeyNotFoundError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(kvError); ok {
		return kvErr.status == gocbcore.StatusKeyNotFound
	}

	return false
}

// IsTempFailError indicates whether the passed error is a
// key-value "temporary failure, try again later" error.
func IsTempFailError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(kvError); ok {
		return kvErr.status == gocbcore.StatusTmpFail
	}

	return false
}

// IsValueTooBigError indicates whether the passed error is a
// key-value "document value was too large" error.
func IsValueTooBigError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(kvError); ok {
		return kvErr.status == gocbcore.StatusTooBig
	}

	return false
}

// IsKeyLockedError indicates whether the passed error is a
// key-value operation failed due to the document being locked.
func IsKeyLockedError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(kvError); ok {
		return kvErr.status == gocbcore.StatusLocked
	}

	return false
}

// IsPathNotFoundError indicates whether the passed error is a
// key-value "sub-document path does not exist" error.
func IsPathNotFoundError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(kvError); ok {
		return kvErr.status == gocbcore.StatusSubDocPathNotFound
	}

	return false
}

// IsPathExistsError indicates whether the passed error is a
// key-value "given path already exists in the document" error.
func IsPathExistsError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(kvError); ok {
		return kvErr.status == gocbcore.StatusSubDocPathExists
	}

	return false
}

// IsInvalidRangeError indicates whether the passed error is a
// key-value "requested value is outside range" error.
func IsInvalidRangeError(err error) bool {
	cause := errors.Cause(err)
	if kvErr, ok := cause.(kvError); ok {
		return kvErr.status == gocbcore.StatusRangeError
	}

	return false
}

// IsNetworkError indicates whether the passed error is a
// network error.
func IsNetworkError(err error) bool {
	cause := errors.Cause(err)
	if nErr, ok := cause.(NetworkError); ok {
		return nErr.NetworkError()
	}

	return false
}

// IsServiceNotFoundError indicates whether the passed error occured due to
// the requested service not being found.
func IsServiceNotFoundError(err error) bool {
	cause := errors.Cause(err)
	if nErr, ok := cause.(ServiceNotFoundError); ok {
		return nErr.ServiceNotFoundError()
	}

	return false
}

// IsTimeoutError verifies whether or not the cause for an error is a timeout.
func IsTimeoutError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case TimeoutError:
		return errType.Timeout()
	default:
		return false
	}
}

// IsPartialResultsError indicates whether or not the response also contains data.
func IsPartialResultsError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case PartialResultError:
		return errType.PartialResults()
	default:
		return false
	}
}

// TimeoutError occurs when an operation times out.
type TimeoutError interface {
	Timeout() bool
}

type timeoutError struct {
}

func (err timeoutError) Error() string {
	return "operation timed out"
}

func (err timeoutError) Timeout() bool {
	return true
}

type PartialResultError interface {
	PartialResults() bool
}

// ServiceNotFoundError is a generic error for HTTP errors.
type ServiceNotFoundError interface {
	error
	StatusCode() int
	ServiceNotFoundError() bool
}

type serviceNotFoundError struct {
}

func (e serviceNotFoundError) Error() string {
	return fmt.Sprintf("the service requested is not enabled or cannot be found on the node requested")
}

// ServiceNotFoundError returns whether or not the error is a service not found error.
func (e serviceNotFoundError) ServiceNotFoundError() bool {
	return true
}

// NetworkError occurs when there is a network error.
type NetworkError interface {
	error
	StatusCode() int
	NetworkError() bool
	retryable() bool
}

type networkError struct {
	statusCode  int
	isRetryable bool
}

func (e networkError) Error() string {
	if e.statusCode == 0 {
		return fmt.Sprintf("a network error occurred")
	}
	return fmt.Sprintf("a network error occurred with status code: %d", e.statusCode)
}

// StatusCode returns the HTTP status code for the error, only applicable to HTTP services.
func (e networkError) StatusCode() int {
	return e.statusCode
}

// NetworkError returns whether or not the error is a network error.
func (e networkError) NetworkError() bool {
	return true
}

func (e networkError) retryable() bool {
	return e.isRetryable
}

// ViewQueryError is the error type for an error that occurs during view query execution.
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

// Reason is the reason for the error occurring.
func (e *viewError) Reason() string {
	return e.ErrorReason
}

// Message
func (e *viewError) Message() string {
	return e.ErrorMessage
}

type ViewQueryErrors interface {
	error
	Errors() []ViewQueryError
	HTTPStatus() int
	Endpoint() string
	PartialResults() bool
}

type viewMultiError struct {
	errors     []ViewQueryError
	httpStatus int
	endpoint   string
	partial    bool
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

func (e viewMultiError) PartialResults() bool {
	return e.partial
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

func (e analyticsQueryError) retryable() bool {
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

func (e analyticsQueryMultiError) retryable() bool {
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

func (e queryError) retryable() bool {
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

func (e queryMultiError) retryable() bool {
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

type SearchError interface {
	error
	Message() string
}

type searchError struct {
	message string
}

func (e searchError) Error() string {
	return e.message
}

func (e searchError) Message() string {
	return e.message
}

type SearchErrors interface {
	error
	Errors() []SearchError
	HTTPStatus() int
	Endpoint() string
	ContextID() string
	PartialResults() bool
}

type searchMultiError struct {
	errors     []SearchError
	httpStatus int
	endpoint   string
	contextID  string
	partial    bool
}

func (e searchMultiError) Error() string {
	var errs []string
	for _, err := range e.errors {
		errs = append(errs, err.Error())
	}
	return strings.Join(errs, ", ")
}

func (e searchMultiError) HTTPStatus() int {
	return e.httpStatus
}

func (e searchMultiError) Endpoint() string {
	return e.endpoint
}

func (e searchMultiError) ContextID() string {
	return e.contextID
}

func (e searchMultiError) Errors() []SearchError {
	return e.errors
}

func (e searchMultiError) PartialResults() bool {
	return e.partial
}

// ErrorCause returns the underlying cause of an error.
func ErrorCause(err error) error {
	return errors.Cause(err)
}

func isRetryableError(err error) bool {
	switch errType := errors.Cause(err).(type) {
	case retryAbleError:
		return errType.retryable()
	default:
		return false
	}
}

func maybeEnhanceErr(err error, key string) error {
	cause := errors.Cause(err)
	switch errType := cause.(type) {
	case gocbcore.KvError:
		return kvError{
			id:          key,
			status:      errType.Code,
			description: errType.Description,
			opaque:      errType.Opaque,
		}
	default:
	}

	if cause == gocbcore.ErrNetwork {
		return networkError{}
	} else if errIsGocbcoreInvalidService(err) {
		return serviceNotFoundError{}
	}

	return err
}

func errIsGocbcoreInvalidService(err error) bool {
	return err == gocbcore.ErrNoCapiService ||
		err == gocbcore.ErrNoCbasService ||
		err == gocbcore.ErrNoFtsService ||
		err == gocbcore.ErrNoN1qlService
}

var (
	// ErrNotEnoughReplicas occurs when not enough replicas exist to match the specified durability requirements.
	ErrNotEnoughReplicas = errors.New("Not enough replicas to match durability requirements.")
	// ErrDurabilityTimeout occurs when the server took too long to meet the specified durability requirements.
	ErrDurabilityTimeout = errors.New("Failed to meet durability requirements in time.")
	// ErrNoResults occurs when no results are available to a query.
	ErrNoResults = errors.New("No results returned.")
	// ErrNoOpenBuckets occurs when a cluster-level operation is performed before any buckets are opened.
	ErrNoOpenBuckets = errors.New("You must open a bucket before you can perform cluster level operations.")
	// ErrIndexInvalidName occurs when an invalid name was specified for an index.
	ErrIndexInvalidName = errors.New("An invalid index name was specified.")
	// ErrIndexNoFields occurs when an index with no fields is created.
	ErrIndexNoFields = errors.New("You must specify at least one field to index.")
	// ErrIndexNotFound occurs when an operation expects an index but it was not found.
	ErrIndexNotFound = errors.New("The index specified does not exist.")
	// ErrIndexAlreadyExists occurs when an operation expects an index not to exist, but it was found.
	ErrIndexAlreadyExists = errors.New("The index specified already exists.")
	// ErrFacetNoRanges occurs when a range-based facet is specified but no ranges were indicated.
	ErrFacetNoRanges = errors.New("At least one range must be specified on a facet.")

	// ErrSearchIndexInvalidName occurs when an invalid name was specified for a search index.
	ErrSearchIndexInvalidName = errors.New("An invalid search index name was specified.")
	// ErrSearchIndexMissingType occurs when no type was specified for a search index.
	ErrSearchIndexMissingType = errors.New("No search index type was specified.")
	// ErrSearchIndexInvalidSourceType occurs when an invalid source type was specific for a search index.
	ErrSearchIndexInvalidSourceType = errors.New("An invalid search index source type was specified.")
	// ErrSearchIndexInvalidSourceName occurs when an invalid source name was specific for a search index.
	ErrSearchIndexInvalidSourceName = errors.New("An invalid search index source name was specified.")
	// ErrSearchIndexAlreadyExists occurs when an invalid source name was specific for a search index.
	ErrSearchIndexAlreadyExists = errors.New("The search index specified already exists.")
	// ErrSearchIndexInvalidIngestControlOp occurs when an invalid ingest control op was specific for a search index.
	ErrSearchIndexInvalidIngestControlOp = errors.New("An invalid search index ingest control op was specified.")
	// ErrSearchIndexInvalidQueryControlOp occurs when an invalid query control op was specific for a search index.
	ErrSearchIndexInvalidQueryControlOp = errors.New("An invalid search index query control op was specified.")
	// ErrSearchIndexInvalidPlanFreezeControlOp occurs when an invalid plan freeze control op was specific for a search index.
	ErrSearchIndexInvalidPlanFreezeControlOp = errors.New("An invalid search index plan freeze control op was specified.")

	// ErrMixedAuthentication occurs when a combination of certification authentication and password authentication are used.
	ErrMixedAuthentication = errors.New("Invalid mixed authentication configuration, cannot use cluster level authentication with bucket password authentication.")
	// ErrMixedCertAuthentication occurs when client certificate authentication is setup but CertAuthenticator is not used or vise versa.
	ErrMixedCertAuthentication = errors.New("Invalid mixed authentication configuration, client certificate and CertAuthenticator must be used together.")

	// ErrDispatchFail occurs when we failed to execute an operation due to internal routing issues.
	ErrDispatchFail = gocbcore.ErrDispatchFail
	// ErrBadHosts occurs when an invalid list of hosts is specified for bootstrapping.
	ErrBadHosts = gocbcore.ErrBadHosts
	// ErrProtocol occurs when an invalid protocol is specified for bootstrapping.
	ErrProtocol = gocbcore.ErrProtocol
	// ErrNoReplicas occurs when an operation expecting replicas is performed, but no replicas are available.
	ErrNoReplicas = gocbcore.ErrNoReplicas
	// ErrInvalidServer occurs when a specified server index is invalid.
	ErrInvalidServer = gocbcore.ErrInvalidServer
	// ErrInvalidVBucket occurs when a specified vbucket index is invalid.
	ErrInvalidVBucket = gocbcore.ErrInvalidVBucket
	// ErrInvalidReplica occurs when a specified replica index is invalid.
	ErrInvalidReplica = gocbcore.ErrInvalidReplica
	// ErrInvalidCert occurs when the specified certificate is not valid.
	ErrInvalidCert = gocbcore.ErrInvalidCert
	// ErrInvalidCredentials is returned when an invalid set of credentials is provided for a service.
	ErrInvalidCredentials = gocbcore.ErrInvalidCredentials
	// ErrNonZeroCas occurs when an operation that require a CAS value of 0 is used with a non-zero value.
	ErrNonZeroCas = gocbcore.ErrNonZeroCas

	// ErrShutdown occurs when an operation is performed on a bucket that has been closed.
	ErrShutdown = gocbcore.ErrShutdown
	// ErrOverload occurs when more operations were dispatched than the client is capable of writing.
	ErrOverload = gocbcore.ErrOverload
	// // ErrNetwork occurs when various generic network errors occur.
	// ErrNetwork = gocbcore.ErrNetwork
	// // ErrTimeout occurs when an operation times out.
	// ErrTimeout = gocbcore.ErrTimeout
	// ErrCliInternalError indicates an internal error occurred within the client.
	// ErrCliInternalError = gocbcore.ErrCliInternalError

	// // ErrStreamClosed occurs when an error is related to a stream closing.
	// ErrStreamClosed = gocbcore.ErrStreamClosed
	// // ErrStreamStateChanged occurs when an error is related to a cluster rebalance.
	// ErrStreamStateChanged = gocbcore.ErrStreamStateChanged
	// // ErrStreamDisconnected occurs when a stream is closed due to a connection dropping.
	// ErrStreamDisconnected = gocbcore.ErrStreamDisconnected
	// // ErrStreamTooSlow occurs when a stream is closed due to being too slow at consuming data.
	// ErrStreamTooSlow = gocbcore.ErrStreamTooSlow

	// // ErrKeyNotFound occurs when the key is not found on the server.
	// ErrKeyNotFound = gocbcore.ErrKeyNotFound
	// // ErrKeyExists occurs when the key already exists on the server.
	// ErrKeyExists = gocbcore.ErrKeyExists
	// // ErrTooBig occurs when the document is too big to be stored.
	// ErrTooBig = gocbcore.ErrTooBig
	// // ErrNotStored occurs when an item fails to be stored.  Usually an append/prepend to missing key.
	// ErrNotStored = gocbcore.ErrNotStored
	// // ErrAuthError occurs when there is an issue with authentication (bad password?).
	// ErrAuthError = gocbcore.ErrAuthError
	// // ErrRangeError occurs when an invalid range is specified.
	// ErrRangeError = gocbcore.ErrRangeError
	// // ErrRollback occurs when a server rollback has occurred making the operation no longer valid.
	// ErrRollback = gocbcore.ErrRollback
	// // ErrAccessError occurs when you do not have access to the specified resource.
	// ErrAccessError = gocbcore.ErrAccessError
	// // ErrOutOfMemory occurs when the server has run out of memory to process requests.
	// ErrOutOfMemory = gocbcore.ErrOutOfMemory
	// // ErrNotSupported occurs when an operation is performed which is not supported.
	// ErrNotSupported = gocbcore.ErrNotSupported
	// // ErrInternalError occurs when an internal error has prevented an operation from succeeding.
	// ErrInternalError = gocbcore.ErrInternalError
	// // ErrBusy occurs when the server is too busy to handle your operation.
	// ErrBusy = gocbcore.ErrBusy
	// // ErrTmpFail occurs when the server is not immediately able to handle your request.
	// ErrTmpFail = gocbcore.ErrTmpFail

	// // ErrSubDocPathNotFound occurs when a sub-document operation targets a path
	// // which does not exist in the specifie document.
	// ErrSubDocPathNotFound = gocbcore.ErrSubDocPathNotFound
	// // ErrSubDocPathMismatch occurs when a sub-document operation specifies a path
	// // which does not match the document structure (field access on an array).
	// ErrSubDocPathMismatch = gocbcore.ErrSubDocPathMismatch
	// // ErrSubDocPathInvalid occurs when a sub-document path could not be parsed.
	// ErrSubDocPathInvalid = gocbcore.ErrSubDocPathInvalid
	// // ErrSubDocPathTooBig occurs when a sub-document path is too big.
	// ErrSubDocPathTooBig = gocbcore.ErrSubDocPathTooBig
	// // ErrSubDocDocTooDeep occurs when an operation would cause a document to be
	// // nested beyond the depth limits allowed by the sub-document specification.
	// ErrSubDocDocTooDeep = gocbcore.ErrSubDocDocTooDeep
	// // ErrSubDocCantInsert occurs when a sub-document operation could not insert.
	// ErrSubDocCantInsert = gocbcore.ErrSubDocCantInsert
	// // ErrSubDocNotJson occurs when a sub-document operation is performed on a
	// // document which is not JSON.
	// ErrSubDocNotJson = gocbcore.ErrSubDocNotJson
	// // ErrSubDocBadRange occurs when a sub-document operation is performed with
	// // a bad range.
	// ErrSubDocBadRange = gocbcore.ErrSubDocBadRange
	// // ErrSubDocBadDelta occurs when a sub-document counter operation is performed
	// // and the specified delta is not valid.
	// ErrSubDocBadDelta = gocbcore.ErrSubDocBadDelta
	// // ErrSubDocPathExists occurs when a sub-document operation expects a path not
	// // to exists, but the path was found in the document.
	// ErrSubDocPathExists = gocbcore.ErrSubDocPathExists
	// // ErrSubDocValueTooDeep occurs when a sub-document operation specifies a value
	// // which is deeper than the depth limits of the sub-document specification.
	// ErrSubDocValueTooDeep = gocbcore.ErrSubDocValueTooDeep
	// // ErrSubDocBadCombo occurs when a multi-operation sub-document operation is
	// // performed and operations within the package of ops conflict with each other.
	// ErrSubDocBadCombo = gocbcore.ErrSubDocBadCombo
	// // ErrSubDocBadMulti occurs when a multi-operation sub-document operation is
	// // performed and operations within the package of ops conflict with each other.
	// ErrSubDocBadMulti = gocbcore.ErrSubDocBadMulti
	// // ErrSubDocSuccessDeleted occurs when a multi-operation sub-document operation
	// // is performed on a soft-deleted document.
	// ErrSubDocSuccessDeleted = gocbcore.ErrSubDocSuccessDeleted

	// ErrSubDocXattrInvalidFlagCombo occurs when an invalid set of
	// extended-attribute flags is passed to a sub-document operation.
	// ErrSubDocXattrInvalidFlagCombo = gocbcore.ErrSubDocXattrInvalidFlagCombo
	// // ErrSubDocXattrInvalidKeyCombo occurs when an invalid set of key operations
	// // are specified for a extended-attribute sub-document operation.
	// ErrSubDocXattrInvalidKeyCombo = gocbcore.ErrSubDocXattrInvalidKeyCombo
	// // ErrSubDocXattrUnknownMacro occurs when an invalid macro value is specified.
	// ErrSubDocXattrUnknownMacro = gocbcore.ErrSubDocXattrUnknownMacro
	// // ErrSubDocXattrUnknownVAttr occurs when an invalid virtual attribute is specified.
	// ErrSubDocXattrUnknownVAttr = gocbcore.ErrSubDocXattrUnknownVAttr
	// // ErrSubDocXattrCannotModifyVAttr occurs when a mutation is attempted upon
	// // a virtual attribute (which are immutable by definition).
	// ErrSubDocXattrCannotModifyVAttr = gocbcore.ErrSubDocXattrCannotModifyVAttr
	// // ErrSubDocMultiPathFailureDeleted occurs when a Multi Path Failure occurs on
	// // a soft-deleted document.
	// ErrSubDocMultiPathFailureDeleted = gocbcore.ErrSubDocMultiPathFailureDeleted
)
