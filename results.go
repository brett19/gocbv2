package gocb

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/couchbase/gocbcore.v7"
)

// GetResult is the return type of Get operations.
type GetResult struct {
	id         string
	flags      uint32
	cas        Cas
	expireAt   int64
	withExpiry bool
	contents   []byte
}

// Cas returns the cas of the result.
func (d *GetResult) Cas() Cas {
	return d.cas
}

// HasExpiry verifies whether or not the result has an expiry value.
func (d *GetResult) HasExpiry() bool {
	return d.withExpiry
}

// Expiry returns the expiry value for the result.
func (d *GetResult) Expiry() time.Time {
	return time.Unix(d.expireAt, 0)
}

// Content assigns the value of the result into the valuePtr using json unmarshalling.
func (d *GetResult) Content(valuePtr interface{}) error {
	return json.Unmarshal(d.contents, valuePtr)
}

// Decode assigns the value of the result into the valuePtr using the decode function
// specified.
func (d *GetResult) Decode(valuePtr interface{}, decode Decode) error {
	if decode == nil {
		decode = DefaultDecode
	}
	return decode(d.contents, d.flags, valuePtr)
}

func (d *GetResult) fromSubDoc(ops []gocbcore.SubDocOp, result *LookupInResult) error {
	content := make(map[string]interface{})
	if len(ops) == 1 && ops[0].Path == "" {
		// This is a special case where the subdoc was a sole fulldoc
		d.contents = result.contents[0].data
		return nil
	}

	for i, op := range ops {
		d.set(strings.Split(op.Path, "."), 0, content, result.contents[i].data)
	}

	bytes, err := json.Marshal(content)
	if err != nil {
		return errors.New("someerror") // TODO
	}
	d.contents = bytes

	return nil
}

func (d *GetResult) set(path []string, i int, content map[string]interface{}, value interface{}) {
	if i == len(path)-1 {
		content[path[i]] = value
		return
	}
	if _, ok := content[path[i]]; !ok {
		content[path[i]] = make(map[string]interface{})
	}
	d.set(path, i+1, content[path[i]].(map[string]interface{}), value)
}

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

// MutationResult is the return type of any store related operations. It contains Cas and mutation tokens.
type MutationResult struct {
	mt  MutationToken
	cas Cas
}

// MutationToken returns the mutation token belonging to an operation.
func (mr MutationResult) MutationToken() MutationToken {
	return mr.mt
}

// Cas returns the Cas value for a document following an operation.
func (mr MutationResult) Cas() Cas {
	return mr.cas
}

// CounterResult is the return type of counter operations.
type CounterResult struct {
	mt      MutationToken
	cas     Cas
	content uint64
}

// MutationToken returns the mutation token belonging to an operation.
func (mr CounterResult) MutationToken() MutationToken {
	return mr.mt
}

// Cas returns the Cas value for a document following an operation.
func (mr CounterResult) Cas() Cas {
	return mr.cas
}

// Content returns the new value for the counter document.
func (mr CounterResult) Content() uint64 {
	return mr.content
}
