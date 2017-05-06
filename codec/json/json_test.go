package json_test

import (
	"testing"

	"github.com/nochso/bolster/codec/internal"
	"github.com/nochso/bolster/codec/json"
)

func TestRoundtrip(t *testing.T) {
	internal.Roundtrip(t, json.Codec)
}
