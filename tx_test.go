package bolster_test

import (
	"flag"
	"testing"

	"github.com/nochso/bolster"
	"github.com/nochso/bolster/internal"
)

var updateGold = flag.Bool("update", false, "update golden test files")

func TestTx_Insert_withoutAutoincrement(t *testing.T) {
	st, closer := internal.OpenTestStore(t)
	defer closer()

	err := st.Register(structWithID{})
	if err != nil {
		t.Error(err)
	}
	t.Run("first", func(t *testing.T) {
		err := st.Write(func(tx *bolster.Tx) error {
			return tx.Insert(&structWithID{})
		})
		if err != nil {
			t.Error(err)
		}
		internal.GoldStore(t, st, *updateGold)
	})
	t.Run("duplicate", func(t *testing.T) {
		err := st.Write(func(tx *bolster.Tx) error {
			return tx.Insert(&structWithID{})
		})
		if err == nil {
			t.Error("expected error, got nil")
		} else {
			t.Log(err)
		}
		internal.GoldStore(t, st, *updateGold)
	})
	t.Run("duplicateLazy", func(t *testing.T) {
		err := st.Write(func(tx *bolster.Tx) error {
			tx.Insert(&structWithID{})
			tx.Insert(&structWithID{})
			tx.Insert(&structWithID{})
			return nil
		})
		if err == nil {
			t.Error("expected error, got nil")
		} else {
			t.Log(err)
		}
		internal.GoldStore(t, st, *updateGold)
	})
	t.Run("surrounding", func(t *testing.T) {
		err := st.Write(func(tx *bolster.Tx) error {
			for i := -5; i < 6; i++ {
				if i == 0 {
					continue
				}
				err := tx.Insert(&structWithID{ID: i})
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Error(err)
		}
		internal.GoldStore(t, st, *updateGold)
	})
}
