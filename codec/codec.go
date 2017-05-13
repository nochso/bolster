package codec

// Interface is implemented by bolster codecs.
//
// A bolster codec can marshal a struct to bytes and unmarshal bytes back into
// a struct of the same type.
type Interface interface {
	Marshal(v interface{}) ([]byte, error)
	Unmarshal(b []byte, v interface{}) error
}
