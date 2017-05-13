package bytesort_test

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/kylelemons/godebug/pretty"
	"github.com/nochso/bolster/bytesort"
	"github.com/nochso/bolster/internal"
)

var (
	update   = flag.Bool("update", false, "update golden test files")
	location = time.FixedZone("UTC-4", -4*60*60)
)

func BenchmarkEncode(b *testing.B) {
	for name, values := range sortTests {
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for _, v := range values {
					bytesort.Encode(v)
				}
			}
		})
	}
}

func BenchmarkEncode_parallel(b *testing.B) {
	for typ, values := range sortTests {
		b.Run(typ, func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					for _, v := range values {
						bytesort.Encode(v)
					}
				}
			})
		})
	}
}

var encodeErrorTests = []interface{}{
	nil,
	[]string{},
	map[string]string{},
}

func TestEncode_error(t *testing.T) {
	for _, tc := range encodeErrorTests {
		name := fmt.Sprintf("%T(%v)", tc, tc)
		t.Run(name, func(t *testing.T) {
			_, err := bytesort.Encode(tc)
			if err == nil {
				t.Error("expected error, got nil")
			} else {
				t.Log(err)
			}
		})
	}
}

var sortTests = map[string][]interface{}{
	"uint8": {
		byte(0),
		byte(2),
		byte(8),
		byte(32),
		byte(128),
		byte(255),
	},
	"int": {
		math.MinInt64,
		math.MinInt64 + 1,
		-1,
		0,
		1,
		math.MaxInt64 - 1,
		math.MaxInt64,
	},
	"int8": {
		int8(math.MinInt8),
		int8(math.MinInt8 + 1),
		int8(-1),
		int8(0),
		int8(1),
		int8(math.MaxInt8 - 1),
		int8(math.MaxInt8),
	},
	"int16": {
		int16(math.MinInt16),
		int16(math.MinInt16 + 1),
		int16(-1),
		int16(0),
		int16(1),
		int16(math.MaxInt16 - 1),
		int16(math.MaxInt16),
	},
	"int32": {
		int32(math.MinInt32),
		int32(math.MinInt32 + 1),
		int32(-1),
		int32(0),
		int32(1),
		int32(math.MaxInt32 - 1),
		int32(math.MaxInt32),
	},
	"int64": {
		int64(math.MinInt64),
		int64(math.MinInt64 + 1),
		int64(-1),
		int64(0),
		int64(1),
		int64(math.MaxInt64 - 1),
		int64(math.MaxInt64),
	},
	"uint": {
		uint(0),
		uint(1),
		uint(math.MaxUint64 - 1),
		uint(math.MaxUint64),
	},
	"uint16": {
		uint16(0),
		uint16(1),
		uint16(math.MaxUint16 - 1),
		uint16(math.MaxUint16),
	},
	"uint32": {
		uint32(0),
		uint32(1),
		uint32(math.MaxUint32 - 1),
		uint32(math.MaxUint32),
	},
	"uint64": {
		uint64(0),
		uint64(1),
		uint64(math.MaxUint64 - 1),
		uint64(math.MaxUint64),
	},
	"float32": {
		float32(-math.MaxFloat32),
		float32(-0.1),
		float32(-math.SmallestNonzeroFloat32),
		float32(0.0),
		float32(math.SmallestNonzeroFloat32),
		float32(0.1),
		float32(math.MaxFloat32),
	},
	"float64": {
		-math.MaxFloat64,
		-0.1,
		-math.SmallestNonzeroFloat64,
		0.0,
		math.SmallestNonzeroFloat64,
		0.1,
		math.MaxFloat64,
	},
	"bool": {
		false,
		true,
	},
	"string": {
		"",
		"  ZOO",
		"  zoo",
		" Aaron",
		"!Aaron",
		"Aaron",
		"Abe",
		"Bert",
		"aaron",
		"bert",
		"bä",
		"bö",
	},
	"time.Time": {
		time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1970, time.January, 1, 0, 0, 0, 0, time.UTC).In(location),
		time.Date(1970, time.January, 1, 0, 0, 0, 1, time.UTC),
		time.Date(1970, time.January, 1, 0, 0, 0, 1, time.UTC).In(location),
		time.Date(1970, time.January, 1, 0, 0, 1, 0, time.UTC),
		time.Date(1970, time.January, 1, 0, 0, 1, 0, time.UTC).In(location),
		time.Date(1970, time.January, 1, 0, 1, 0, 0, time.UTC),
		time.Date(1970, time.January, 1, 0, 1, 0, 0, time.UTC).In(location),
		time.Date(1970, time.January, 1, 1, 0, 0, 0, time.UTC),
		time.Date(1970, time.January, 1, 1, 0, 0, 0, time.UTC).In(location),
		time.Date(1970, time.January, 2, 0, 0, 0, 0, time.UTC),
		time.Date(1970, time.January, 2, 0, 0, 0, 0, time.UTC).In(location),
		time.Date(1970, time.February, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1970, time.February, 1, 0, 0, 0, 0, time.UTC).In(location),
		time.Date(1971, time.January, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1971, time.January, 1, 0, 0, 0, 0, time.UTC).In(location),
	},
}

func TestEncode_sortability(t *testing.T) {
	for typ, values := range sortTests {
		t.Run(typ, func(t *testing.T) {
			testEncodeSortability(t, values)
		})
	}
}

func testEncodeSortability(t *testing.T, values []interface{}) {
	exp := make([][]byte, 0, len(values))
	act := make([][]byte, 0, len(values))
	for _, v := range values {
		b, err := bytesort.Encode(v)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			t.Fatal("byte slice must not be nil")
		}
		exp = append(exp, b)
		act = append(act, b)
	}
	if reflect.TypeOf(values[0]).Kind() != reflect.String {
		for i := 1; i < len(act); i++ {
			if len(act[i-1]) == len(act[i]) {
				continue
			}
			t.Errorf(
				"encoded length must stay the same for non-strings: got %d bytes at #%d and %d bytes at #%d",
				len(act[i-1]), i-1, len(act[i]), i,
			)
		}
	}
	sort.Slice(act, func(i, j int) bool {
		return bytes.Compare(act[i], act[j]) < 0
	})
	if !reflect.DeepEqual(act, exp) {
		t.Error(pretty.Compare(fmtBytes(act), fmtBytes(exp)))
	}
}

// fmtBytes helps make prettier diffs of []byte
func fmtBytes(b [][]byte) []string {
	buf := &bytes.Buffer{}
	for i, bb := range b {
		fmt.Fprintf(buf, "%02d. % x\n", i, bb)
	}
	return strings.Split(buf.String(), "\n")
}

func TestEncode(t *testing.T) {
	for typ, values := range sortTests {
		t.Run(typ, func(t *testing.T) {
			testEncode(t, values)
		})
	}
}

func testEncode(t *testing.T, values []interface{}) {
	act := &bytes.Buffer{}
	for _, v := range values {
		b, err := bytesort.Encode(v)
		if err != nil {
			t.Error(err)
		}
		fmt.Fprintf(act, "%v\n%s\n", v, hex.Dump(b))
	}
	internal.Gold(t, act.Bytes(), *update)
}

func TestEncode_fixedLengthExceptForStrings(t *testing.T) {
	for typ, values := range sortTests {
		t.Run(typ, func(t *testing.T) {
			length := -1
			for _, v := range values {
				b, err := bytesort.Encode(v)
				if err != nil {
					t.Error(err)
				}
				if length == -1 {
					length = len(b)
				} else if length != len(b) && typ != "string" {
					t.Errorf(
						"expected fixed length for type %s: length %d of %q is different from the first value's length %d",
						typ,
						len(b),
						v,
						length,
					)
				}
			}
		})
	}
}
