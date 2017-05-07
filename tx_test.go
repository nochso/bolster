package bolster_test

import (
	"testing"

	"github.com/nochso/bolster"
	"github.com/nochso/bolster/internal"
)

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
}
