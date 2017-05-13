package bolster_test

import (
	"flag"
	"reflect"
	"testing"

	"github.com/kylelemons/godebug/pretty"
	"github.com/nochso/bolster"
	"github.com/nochso/bolster/internal"
)

var updateGold = flag.Bool("update", false, "update golden test files")

func TestTx_Insert_withoutAutoincrement(t *testing.T) {
	st, closer := internal.OpenTestStore(t)
	defer closer()

	t.Run("withoutRegistration", func(t *testing.T) {
		err := st.Write(func(tx *bolster.Tx) error {
			return tx.Insert(&structWithID{})
		})
		if err == nil {
			t.Error("expected error, got nil")
		} else {
			t.Log(err)
		}
	})
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
	t.Run("withoutPointer", func(t *testing.T) {
		err := st.Write(func(tx *bolster.Tx) error {
			return tx.Insert(structWithID{})
		})
		if err == nil {
			t.Error("expected error, got nil")
		} else {
			t.Log(err)
		}
	})
	t.Run("pointerToNonStruct", func(t *testing.T) {
		err := st.Write(func(tx *bolster.Tx) error {
			return tx.Insert(new(int))
		})
		if err == nil {
			t.Error("expected error, got nil")
		} else {
			t.Log(err)
		}
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

type structWithAutoincrement struct {
	ID uint8 `bolster:"inc"`
}

func TestTx_Insert_withAutoincrement(t *testing.T) {
	st, closer := internal.OpenTestStore(t)
	defer closer()

	err := st.Register(structWithAutoincrement{})
	if err != nil {
		t.Error(err)
	}
	t.Run("single", func(t *testing.T) {
		exp := &structWithAutoincrement{ID: 1}
		act := &structWithAutoincrement{}
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Insert(act)
		})
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(exp, act) {
			t.Error(pretty.Compare(act, exp))
		}
		internal.GoldStore(t, st, *updateGold)
	})
	t.Run("multipleInSingleTransaction", func(t *testing.T) {
		actuals := make([]structWithAutoincrement, 4)
		err = st.Write(func(tx *bolster.Tx) error {
			for i := range actuals {
				tx.Insert(&actuals[i])
			}
			return nil
		})
		if err != nil {
			t.Error(err)
		}
		for _, act := range actuals {
			if act.ID == 0 {
				t.Errorf("expected ID of struct to be set, got %d", act.ID)
			}
		}
		internal.GoldStore(t, st, *updateGold)
	})
	// TODO Add tests with more int/uint types
	t.Run("overflowID", func(t *testing.T) {
		err := st.Write(func(tx *bolster.Tx) error {
			for i := 0; i < 256; i++ {
				err := tx.Insert(&structWithAutoincrement{})
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err == nil {
			t.Error("expected error, got nil")
		} else {
			t.Log(err)
		}
		internal.GoldStore(t, st, *updateGold)
	})
}

func TestTx_Get(t *testing.T) {
	st, closer := internal.OpenTestStore(t)
	defer closer()

	err := st.Register(&structWithID{})
	if err != nil {
		t.Error(err)
	}
	exp := &structWithID{ID: 2}
	err = st.Write(func(tx *bolster.Tx) error {
		return tx.Insert(exp)
	})
	if err != nil {
		t.Error(err)
	}
	t.Run("NotFound", func(t *testing.T) {
		act := &structWithID{}
		err := st.Read(func(tx *bolster.Tx) error {
			return tx.Get(act, 1)
		})
		if err != bolster.ErrNotFound {
			t.Errorf("expected ErrNotFound, got %v", err)
		} else {
			t.Log(err)
		}
	})
	t.Run("Success", func(t *testing.T) {
		act := &structWithID{}
		err = st.Read(func(tx *bolster.Tx) error {
			return tx.Get(act, 2)
		})
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(act, exp) {
			t.Error(pretty.Compare(act, exp))
		}
	})
	t.Run("wrongTypeOfID", func(t *testing.T) {
		err := st.Read(func(tx *bolster.Tx) error {
			return tx.Get(&structWithID{}, "1")
		})
		if err == nil {
			t.Error("expected error, got nil")
		} else {
			t.Log(err)
		}
	})
	t.Run("withoutPointer", func(t *testing.T) {
		err := st.Read(func(tx *bolster.Tx) error {
			return tx.Get(structWithID{}, "1")
		})
		if err == nil {
			t.Error("expected error, got nil")
		} else {
			t.Log(err)
		}
	})
}
