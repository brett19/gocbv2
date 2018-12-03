package gocb

import (
	"encoding/json"
	"time"
)

type Document struct {
	id       string
	value    interface{}
	bytes    []byte
	flags    uint32
	cas      Cas
	expireAt uint32
}

func (d *Document) Unmarshal(valuePtr interface{}) error {
	return nil
}

// func NewDocument(value interface{}) (Document, error) {
// 	return Document{
// 		value: value,
// 	}, nil
// }

func (d *Document) Id() string {
	return d.id
}

func (d *Document) Cas() Cas {
	return d.cas
}

func (d *Document) Expiration() time.Time {
	return time.Unix(int64(d.expireAt), 0)
}

func (d *Document) ContentAs(valuePtr interface{}) error {
	return json.Unmarshal(d.bytes, valuePtr)
}

func (d *Document) DecodeAs(valuePtr interface{}, decode Decode) error {
	if decode == nil {
		decode = DefaultDecode
	}

	return decode(d.bytes, d.flags, valuePtr)
}

func (d *Document) WithCas(cas Cas) *Document {
	d.cas = cas

	return d
}
