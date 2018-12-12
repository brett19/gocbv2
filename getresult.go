package gocb

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"gopkg.in/couchbase/gocbcore.v7"
)

type getPartial struct {
	path  string
	bytes []byte
	err   error
}

type GetResult struct {
	id         string
	flags      uint32
	cas        Cas
	expireAt   uint32
	withExpiry bool
	contents   map[string]interface{}
	pathMap    map[string]int
}

func (d *GetResult) Id() string {
	return d.id
}

func (d *GetResult) Cas() Cas {
	return d.cas
}

func (d *GetResult) HasExpiry() bool {
	return d.withExpiry
}

func (d *GetResult) Expiry() time.Time {
	return time.Unix(int64(d.expireAt), 0)
}

func (d *GetResult) Content(valuePtr interface{}) error {
	return json.Unmarshal(d.contents[""].([]byte), valuePtr)
}

func (d *GetResult) Decode(valuePtr interface{}, decode Decode) error {
	if decode == nil {
		decode = DefaultDecode
	}
	return decode(d.contents[""].([]byte), d.flags, valuePtr)
}

// ContentAt retrieves the value of the operation by its path. The path is the path provided
// to the operation.
func (d *GetResult) ContentAt(path string, valuePtr interface{}) error {
	currentPath := d.contents
	var currentContent interface{}
	var ok bool
	for _, pathPart := range strings.Split(path, ".") {
		currentContent, ok = currentPath[pathPart]
		if !ok {
			return errors.New("Subdoc path does not exist") //TODO: error
		}
		switch currentContent.(type) {
		case map[string]interface{}:
			currentPath = currentPath[pathPart].(map[string]interface{})
		default:
			break
		}
	}

	err := json.Unmarshal(currentContent.([]byte), valuePtr)
	if err != nil {
		return errors.New("")
	}

	return nil
}

func (d *GetResult) fromSubDoc(ops []gocbcore.SubDocOp, result []gocbcore.SubDocResult) {
	content := make(map[string]interface{})
	for i, op := range ops {
		d.set(strings.Split(op.Path, "."), 0, content, result[i].Value)
	}
	d.contents = content
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
