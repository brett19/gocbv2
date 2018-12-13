package gocb

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/couchbase/gocbcore.v7"
)

type GetResult struct {
	id         string
	flags      uint32
	cas        Cas
	expireAt   uint32
	withExpiry bool
	contents   []byte
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
	return json.Unmarshal(d.contents, valuePtr)
}

func (d *GetResult) Decode(valuePtr interface{}, decode Decode) error {
	if decode == nil {
		decode = DefaultDecode
	}
	return decode(d.contents, d.flags, valuePtr)
}

func (d *GetResult) fromSubDoc(ops []gocbcore.SubDocOp, result []projectionResult) error {
	content := make(map[string]interface{})
	for i, op := range ops {
		d.set(strings.Split(op.Path, "."), 0, content, result[i].data)
	}
	bytes, err := json.Marshal(content)
	if err != nil {
		return errors.New("someerror") // TODO
	}
	thing := string(bytes)
	fmt.Println(thing)
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
