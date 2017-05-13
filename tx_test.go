package bolster_test

import (
	"flag"
	"testing"

	"github.com/nochso/bolster"
	"github.com/nochso/bolster/internal"
)

var updateGold = flag.Bool("update", false, "update golden test files")

func TestTx_Insert(t *testing.T) {
	st, closer := internal.OpenTestStore(t)
	defer closer()

	err := st.Register(structWithID{})
	if err != nil {
		t.Error(err)
	}
	err = st.Write(func(tx *bolster.Tx) error {
		return tx.Insert(&structWithID{ID: 1})
	})
	if err != nil {
		t.Error(err)
	}
	act := internal.DumpStore(st)
	internal.Gold(t, act, *updateGold)
}
