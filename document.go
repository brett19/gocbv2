package gocb

type Document struct {
	value interface{}
	bytes []byte
	flags uint32
	cas   Cas
}

func (d *Document) Unmarshal(valuePtr interface{}) error {
	return nil
}

func NewDocument(value interface{}) (Document, error) {
	return Document{
		value: value,
	}, nil
}
