// Package bytesort encodes common types as a binary/byte slices that are bytewise sortable.
//
// The output is intended for binary/bytewise comparison and sorting.
// More specifically for creating the keys used in indexes in a bolt DB.
//
// Use bytes.Compare and sort.Slice to sort [][]byte:
//
//	sort.Slice(s, func(i, j int) bool {
//		return bytes.Compare(s[i], s[j]) < 0
//	})
//
// sort.Search might also be of interest.
package bytesort

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// Encode a value as a byte slice that is bytewise/binary-sortable.
//
// Any results for the same type are sortable using a bytewise/binary
// comparison. A correct Sort order is not guaranteed when mixing different
// types.
//
// When err == nil the length of the byte slice is always > 0. The length is
// always the same for values of the same type. Encoded strings are the only
// exception as they vary in length.
// Empty strings are encoded as 0x00 to allow using them as bolt bucket names.
//
// Sortability is the only requirement. None of the encodings retain any type
// information because decoding of binary back into a value is out of scope.
//
// The following types are supported:
//
//	bool
//	float32 float64
//	int int8 int16 int32 int64
//	uint uint8 uint16 uint32 uint64
//	string    (case-sensitive)
//	time.Time (normalised to UTC)
func Encode(v interface{}) (b []byte, err error) {
	switch v.(type) {
	case string:
		b = []byte(v.(string))
		// Special case for empty strings because empty bucket names are not
		// allowed. Use a zero byte to represent an empty string.
		if len(b) == 0 {
			b = append(b, 0)
		}
		return
	case time.Time:
		return encodeTime(v.(time.Time))
	case float64:
		return encodeFloat64(v.(float64)), nil
	case float32:
		return encodeFloat32(v.(float32)), nil
	}

	b, err = encodeInt(v)
	if err != nil {
		return
	}
	return
}

func encodeInt(data interface{}) ([]byte, error) {
	n := intDataSize(data)
	if n == 0 {
		return nil, fmt.Errorf("bytesort.Encode: unsupported type %T", data)
	}
	bs := make([]byte, n)
	switch v := data.(type) {
	case bool:
		if v {
			bs[0] = 1
		} else {
			bs[0] = 0
		}
	case int8:
		bs[0] = byte(v)
	case uint8:
		bs[0] = v
	case int16:
		binary.BigEndian.PutUint16(bs, uint16(v))
	case uint16:
		binary.BigEndian.PutUint16(bs, v)
	case int32:
		binary.BigEndian.PutUint32(bs, uint32(v))
	case uint32:
		binary.BigEndian.PutUint32(bs, v)
	case int64:
		binary.BigEndian.PutUint64(bs, uint64(v))
	case int:
		binary.BigEndian.PutUint64(bs, uint64(int64(v)))
	case uint64:
		binary.BigEndian.PutUint64(bs, v)
	case uint:
		binary.BigEndian.PutUint64(bs, uint64(v))
	}
	switch data.(type) {
	case int64, int32, int16, int8, int:
		// The order is almost correct, however ascending positive numbers are
		// currently before ascending negative numbers. Flip the first bit of
		// the two's complement: both ranges switch places, resulting in
		// consecutively ascending sort order.
		bs[0] ^= 0x80
	}
	return bs, nil
}

// intDataSize returns the size of the data required to represent the data when encoded.
// It returns zero if the type cannot be implemented by the fast path in Read or Write.
func intDataSize(data interface{}) int {
	switch data.(type) {
	case bool, int8, uint8:
		return 1
	case int16, uint16:
		return 2
	case int32, uint32:
		return 4
	case int64, uint64, int, uint:
		return 8
	}
	return 0
}

// http://stereopsis.com/radix.html
func encodeFloat64(v float64) []byte {
	bits := math.Float64bits(v)
	bits ^= -(bits >> 63) | (1 << 63)
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, bits)
	return b
}

func encodeFloat32(v float32) []byte {
	bits := math.Float32bits(v)
	bits ^= -(bits >> 31) | (1 << 31)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, bits)
	return b
}

func encodeTime(v time.Time) ([]byte, error) {
	b, err := v.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("bytesort.Encode: %s", err)
	}
	// Strip version and time zone, leaving only the sort-relevant parts
	return b[1 : len(b)-2], nil
}
