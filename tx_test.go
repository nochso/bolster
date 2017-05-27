package bolster_test

import (
	"flag"
	"reflect"
	"strings"
	"testing"

	"github.com/kylelemons/godebug/pretty"
	"github.com/nochso/bolster"
	"github.com/nochso/bolster/errlist"
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
	t.Run("multiFieldIndex", func(t *testing.T) {
		st, closer := internal.OpenTestStore(t)
		defer closer()
		err := st.Register(structWithMultiFieldIndex{})
		if err != nil {
			t.Error(err)
		}
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Truncate(structWithMultiFieldIndex{})
		})
		if err != nil {
			t.Error(err)
		}
		err = st.Write(func(tx *bolster.Tx) error {
			tx.Insert(&structWithMultiFieldIndex{ID: 1, Name: "foo", Visible: true})
			tx.Insert(&structWithMultiFieldIndex{ID: 2, Name: "bar", Visible: false})
			tx.Insert(&structWithMultiFieldIndex{ID: 3, Name: "foobar", Visible: false})
			return nil
		})
		if err != nil {
			t.Error(err)
		}
		internal.GoldStore(t, st, *updateGold)
	})
}

func TestTx_Insert_NonIntegerID(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		st, closer := internal.OpenTestStore(t)
		defer closer()
		err := st.Register(structWithTaggedID{})
		if err != nil {
			t.Error(err)
		}
		itm := &structWithTaggedID{Name: "foo"}
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Insert(itm)
		})
		if err != nil {
			t.Error(err)
		}
		if itm.Name != "foo" {
			t.Errorf("expected Name = \"foo\", got %q", itm.Name)
		}
		internal.GoldStore(t, st, *updateGold)
	})
	t.Run("multiple", func(t *testing.T) {
		st, closer := internal.OpenTestStore(t)
		defer closer()
		err := st.Register(structWithTaggedID{})
		if err != nil {
			t.Error(err)
		}
		err = st.Write(func(tx *bolster.Tx) error {
			itm := &structWithTaggedID{}
			for i := 0; i < 16; i++ {
				itm.Name = strings.Repeat("z", i)
				err := tx.Insert(itm)
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
	t.Run("duplicate", func(t *testing.T) {
		st, closer := internal.OpenTestStore(t)
		defer closer()
		err := st.Register(structWithTaggedID{})
		if err != nil {
			t.Error(err)
		}
		itm := &structWithTaggedID{Name: "foo"}
		err = st.Write(func(tx *bolster.Tx) error {
			tx.Insert(itm)
			tx.Insert(itm)
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
		actuals := make([]structWithAutoincrement, 5)
		err = st.Write(func(tx *bolster.Tx) error {
			tx.Truncate(actuals[0])
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
	t.Run("overflowIDinSingleTransaction", func(t *testing.T) {
		err := st.Write(func(tx *bolster.Tx) error {
			return tx.Truncate(structWithAutoincrement{})
		})
		if err != nil {
			t.Error(err)
		}
		err = st.Write(func(tx *bolster.Tx) error {
			for i := 0; i < 257; i++ {
				tx.Insert(&structWithAutoincrement{})
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
	t.Run("overflowIDinMultipleTransactions", func(t *testing.T) {
		err := st.Write(func(tx *bolster.Tx) error {
			return tx.Truncate(structWithAutoincrement{})
		})
		if err != nil {
			t.Error(err)
		}
		errs := errlist.New()
		for i := 0; i < 257; i++ {
			err = st.Write(func(tx *bolster.Tx) error {
				return tx.Insert(&structWithAutoincrement{})
			})
			errs.Append(err)
		}
		if errs.ErrorOrNil() == nil {
			t.Error("expected at least one error, got none")
		} else {
			t.Log(errs.Error())
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
		e, ok := err.(bolster.Error)
		if !ok {
			t.Errorf("expected Error, got %T", err)
		}
		if !e.IsNotFound() {
			t.Errorf("expected Error.IsNotFound = true, got false")
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

func TestTx_Delete(t *testing.T) {
	st, closer := internal.OpenTestStore(t)
	defer closer()

	err := st.Register(structWithID{})
	if err != nil {
		t.Error(err)
	}

	internal.GoldStore(t, st, *updateGold)
	t.Run("nonExisting", func(t *testing.T) {
		err := st.Write(func(tx *bolster.Tx) error {
			return tx.Delete(structWithID{})
		})
		if err != nil {
			t.Error(err)
		}
		internal.GoldStore(t, st, *updateGold)
	})
	t.Run("first", func(t *testing.T) {
		err := st.Write(func(tx *bolster.Tx) error {
			tx.Insert(&structWithID{1})
			tx.Insert(&structWithID{2})
			return nil
		})
		if err != nil {
			t.Error(err)
		}
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Delete(&structWithID{1})
		})
		if err != nil {
			t.Error(err)
		}
		internal.GoldStore(t, st, *updateGold)
	})
	t.Run("firstInSameTransaction", func(t *testing.T) {
		err := st.Write(func(tx *bolster.Tx) error {
			tx.Truncate(structWithID{})
			tx.Insert(&structWithID{3})
			tx.Insert(&structWithID{4})
			return tx.Delete(&structWithID{3})
		})
		if err != nil {
			t.Error(err)
		}
		internal.GoldStore(t, st, *updateGold)
	})
	t.Run("multiFieldIndex", func(t *testing.T) {
		st, closer := internal.OpenTestStore(t)
		defer closer()
		err := st.Register(structWithMultiFieldIndex{})
		if err != nil {
			t.Error(err)
		}
		err = st.Write(func(tx *bolster.Tx) error {
			tx.Insert(&structWithMultiFieldIndex{ID: 1, Name: "foo", Visible: true})
			tx.Insert(&structWithMultiFieldIndex{ID: 2, Name: "bar", Visible: false})
			tx.Insert(&structWithMultiFieldIndex{ID: 3, Name: "foobar", Visible: false})
			return nil
		})
		if err != nil {
			t.Error(err)
		}
		// Expect proper clean up of the index even though we only pass the ID.
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Delete(structWithMultiFieldIndex{ID: 2})
		})
		if err != nil {
			t.Error(err)
		}
		internal.GoldStore(t, st, *updateGold)
	})
}

type structWithIDAndField struct {
	ID   int
	Name string
}

func TestTx_Update(t *testing.T) {
	st, closer := internal.OpenTestStore(t)
	defer closer()
	err := st.Register(structWithIDAndField{})
	if err != nil {
		t.Error(err)
	}
	t.Run("existing", func(t *testing.T) {
		exp := &structWithIDAndField{1, "foo"}
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Insert(exp)
		})
		if err != nil {
			t.Error(err)
		}
		exp.Name = "bar"
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Update(exp)
		})
		if err != nil {
			t.Error(err)
		}
		act := &structWithIDAndField{}
		err = st.Read(func(tx *bolster.Tx) error {
			return tx.Get(act, 1)
		})
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(act, exp) {
			t.Error(pretty.Compare(act, exp))
		}
		internal.GoldStore(t, st, *updateGold)
	})
	t.Run("missing", func(t *testing.T) {
		item := &structWithIDAndField{123, "foo"}
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Truncate(item)
		})
		if err != nil {
			t.Error(err)
		}
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Update(item)
		})
		if err == nil {
			t.Error("expected error, got nil")
		} else {
			t.Log(err)
		}
		internal.GoldStore(t, st, *updateGold)
	})
}

type structWithIncrementingIDAndField struct {
	ID   int `bolster:"inc"`
	Name string
}

func TestTx_Upsert_withoutAutoincrement(t *testing.T) {
	st, closer := internal.OpenTestStore(t)
	defer closer()
	err := st.Register(structWithIDAndField{})
	if err != nil {
		t.Error(err)
	}
	t.Run("existing", func(t *testing.T) {
		exp := &structWithIDAndField{1, "foo"}
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Insert(exp)
		})
		if err != nil {
			t.Error(err)
		}
		exp.Name = "bar"
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Upsert(exp)
		})
		if err != nil {
			t.Error(err)
		}
		act := &structWithIDAndField{}
		err = st.Read(func(tx *bolster.Tx) error {
			return tx.Get(act, 1)
		})
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(act, exp) {
			t.Error(pretty.Compare(act, exp))
		}
		internal.GoldStore(t, st, *updateGold)
	})
	t.Run("missing", func(t *testing.T) {
		item := &structWithIDAndField{123, "foo"}
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Truncate(item)
		})
		if err != nil {
			t.Error(err)
		}
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Upsert(item)
		})
		if err != nil {
			t.Error(err)
		}
		internal.GoldStore(t, st, *updateGold)
	})
}

func TestTx_Upsert_withAutoincrement(t *testing.T) {
	st, closer := internal.OpenTestStore(t)
	defer closer()
	err := st.Register(structWithIncrementingIDAndField{})
	if err != nil {
		t.Error(err)
	}
	t.Run("existing", func(t *testing.T) {
		exp := &structWithIncrementingIDAndField{1, "foo"}
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Insert(exp)
		})
		if err != nil {
			t.Error(err)
		}
		exp.Name = "bar"
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Upsert(exp)
		})
		if err != nil {
			t.Error(err)
		}
		internal.GoldStore(t, st, *updateGold)
	})
	t.Run("missing", func(t *testing.T) {
		item := &structWithIncrementingIDAndField{123, "foo"}
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Truncate(item)
		})
		if err != nil {
			t.Error(err)
		}
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Upsert(item)
		})
		if err != nil {
			t.Error(err)
		}
		internal.GoldStore(t, st, *updateGold)
	})
	t.Run("missing_autoincrement", func(t *testing.T) {
		item := &structWithIncrementingIDAndField{0, "foo"}
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Truncate(item)
		})
		if err != nil {
			t.Error(err)
		}
		err = st.Write(func(tx *bolster.Tx) error {
			return tx.Upsert(item)
		})
		if err != nil {
			t.Error(err)
		}
		if item.ID == 0 {
			t.Error("expected item with autoincremented ID, got zero value")
		}
		internal.GoldStore(t, st, *updateGold)
	})
}

func TestTx_Write_errors(t *testing.T) {
	st, closer := internal.OpenTestStore(t)
	defer closer()
	err := st.Write(func(tx *bolster.Tx) error {
		tx.Delete(nil)
		tx.Delete(nil)
		tx.Get(nil, nil)
		tx.Insert(nil)
		tx.Truncate(nil)
		tx.Update(nil)
		tx.Upsert(nil)
		return nil
	})
	if err == nil {
		t.Error("expected error, got nil")
	} else {
		t.Log(err)
	}
}
