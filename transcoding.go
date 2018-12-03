package gocb

import (
	"encoding/json"

	"gopkg.in/couchbase/gocbcore.v7"
)

// Decode retrieved bytes into a Go type.
type Decode func([]byte, uint32, interface{}) error

// Encode a Go type into bytes for storage.
type Encode func(interface{}) ([]byte, uint32, error)

// Decode applies the default Couchbase transcoding behaviour to decode into a Go type.
func DefaultDecode(bytes []byte, flags uint32, out interface{}) error {
	valueType, compression := gocbcore.DecodeCommonFlags(flags)

	// Make sure compression is disabled
	if compression != gocbcore.NoCompression {
		// return clientError{"Unexpected value compression"}
	}

	// Normal types of decoding
	if valueType == gocbcore.BinaryType {
		switch typedOut := out.(type) {
		case *[]byte:
			*typedOut = bytes
			return nil
		case *interface{}:
			*typedOut = bytes
			return nil
		default:
			// return clientError{"You must encode binary in a byte array or interface"}
		}
	} else if valueType == gocbcore.StringType {
		switch typedOut := out.(type) {
		case *string:
			*typedOut = string(bytes)
			return nil
		case *interface{}:
			*typedOut = string(bytes)
			return nil
		default:
			// return clientError{"You must encode a string in a string or interface"}
		}
	} else if valueType == gocbcore.JsonType {
		err := json.Unmarshal(bytes, &out)
		if err != nil {
			return err
		}
		return nil
	}

	// return clientError{"Unexpected flags value"}
	return nil
}

// DefaultEncode applies the default Couchbase transcoding behaviour to encode a Go type.
func DefaultEncode(value interface{}) ([]byte, uint32, error) {
	var bytes []byte
	var flags uint32
	var err error

	switch typeValue := value.(type) {
	case []byte:
		bytes = typeValue
		flags = gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression)
	case *[]byte:
		bytes = *typeValue
		flags = gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression)
	case string:
		bytes = []byte(typeValue)
		flags = gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression)
	case *string:
		bytes = []byte(*typeValue)
		flags = gocbcore.EncodeCommonFlags(gocbcore.StringType, gocbcore.NoCompression)
	case *interface{}:
		return DefaultEncode(*typeValue)
	default:
		bytes, err = json.Marshal(value)
		if err != nil {
			return nil, 0, err
		}
		flags = gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression)
	}

	// No compression supported currently

	return bytes, flags, nil
}
