package internal

import (
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/kylelemons/godebug/pretty"
	"github.com/nochso/bolster/codec"
)

type testStruct struct {
	A int
	B uint
	C int8
	D int16
	E int32
	F int64
	G uint8
	H uint16
	I uint32
	J uint64
	K bool
	L float32
	M float64
	N string
	O testStructChild
	P *testStructChild
}

type testStructChild struct {
	A int
	B bool
}

func Roundtrip(t *testing.T, c codec.Interface) {
	r := rand.New(rand.NewSource(0))
	for i := 0; i < 1000; i++ {
		// Create random struct
		expr, ok := quick.Value(reflect.TypeOf(testStruct{}), r)
		if !ok {
			t.Fatal("unable to create random struct")
		}
		exp := expr.Addr().Interface()
		// Encode to bytes
		b, err := c.Marshal(exp)
		if err != nil {
			t.Fatal(err)
		}
		// Decode it back to a new struct
		act := &testStruct{}
		err = c.Unmarshal(b, act)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(act, exp) {
			t.Error(pretty.Compare(act, exp))
		}
		// Encode to bytes again
		b2, err := c.Marshal(act)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(b, b2) {
			t.Error(pretty.Compare(b2, b))
		}
	}
}
