package gob

import (
	"testing"

	"github.com/nochso/bolster/codec/internal"
)

func TestRoundtrip(t *testing.T) {
	internal.Roundtrip(t, Codec)
}
