package gob

import (
	"bytes"
	"encoding/gob"
	"sync"

	"github.com/nochso/bolster/codec"
)

var (
	// Codec wraps encoding/gob.
	Codec    codec.Interface = gobCodec{}
	bytePool                 = sync.Pool{
		New: func() interface{} { return &bytes.Buffer{} },
	}
)

type gobCodec struct{}

func (gobCodec) Marshal(v interface{}) ([]byte, error) {
	buf := bytePool.Get().(*bytes.Buffer)
	buf.Reset()
	enc := gob.NewEncoder(buf)
	err := enc.Encode(v)
	if err != nil {
		bytePool.Put(buf)
		return nil, err
	}
	b := make([]byte, buf.Len())
	copy(b, buf.Bytes())
	bytePool.Put(buf)
	return b, nil
}

func (gobCodec) Unmarshal(b []byte, v interface{}) error {
	return gob.NewDecoder(bytes.NewReader(b)).Decode(v)
}
