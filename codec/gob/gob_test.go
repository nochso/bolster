package gob_test

import (
	"testing"

	"github.com/nochso/bolster/codec/gob"
	"github.com/nochso/bolster/codec/internal"
)

func TestRoundtrip(t *testing.T) {
	internal.Roundtrip(t, gob.Codec)
}
