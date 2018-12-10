package gocb

import (
	"encoding/json"
	"errors"
	"time"
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
	contents   []getPartial
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
	return json.Unmarshal(d.contents[0].bytes, valuePtr)
}

func (d *GetResult) Decode(valuePtr interface{}, decode Decode) error {
	if decode == nil {
		decode = DefaultDecode
	}

	return decode(d.contents[0].bytes, d.flags, valuePtr)
}

// ContentByIndex retrieves the value of the operation by its index. The index is the position of
// the operation as it was added to the builder.
func (d *GetResult) ContentByIndex(idx int, valuePtr interface{}) error {
	if d.contents[idx].err != nil {
		return d.contents[idx].err
	}

	if valuePtr, ok := valuePtr.(*[]byte); ok {
		*valuePtr = d.contents[idx].bytes
		return nil
	}

	return json.Unmarshal(d.contents[idx].bytes, valuePtr)
}

// ContentAt retrieves the value of the operation by its path. The path is the path provided
// to the operation.
func (d *GetResult) ContentAt(path string, valuePtr interface{}) error {
	if d.pathMap == nil {
		d.pathMap = make(map[string]int)
		for i, v := range d.contents {
			d.pathMap[v.path] = i
		}
	}

	content, ok := d.pathMap[path]
	if !ok {
		return errors.New("Subdoc path does not exist") //TODO: error
	}
	return d.ContentByIndex(content, valuePtr)
}
