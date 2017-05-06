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
}

func Roundtrip(t *testing.T, c codec.Interface) {
	r := rand.New(rand.NewSource(0))
	for i := 0; i < 1000; i++ {
		expr, ok := quick.Value(reflect.TypeOf(testStruct{}), r)
		if !ok {
			t.Fatal("unable to create random struct")
		}
		exp := expr.Addr().Interface()
		b, err := c.Marshal(exp)
		if err != nil {
			t.Fatal(err)
		}
		var act testStruct
		err = c.Unmarshal(b, &act)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(act, exp) {
			pretty.Compare(act, exp)
		}
	}
}
