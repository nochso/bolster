package json

import (
	"encoding/json"

	"github.com/nochso/bolster/codec"
)

// Codec wraps encoding/json.
var Codec codec.Interface = jsonCodec{}

type jsonCodec struct{}

func (jsonCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(b []byte, v interface{}) error {
	return json.Unmarshal(b, v)
}
