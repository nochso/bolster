package bolster_test

import (
	"testing"

	"github.com/nochso/bolster/internal"
)

type structWithoutID struct {
	Name string
}

type structWithID struct {
	ID int
}

type structWithTaggedID struct {
	Name string `bolster:"id"`
}

type structWithMultipleTaggedIDs struct {
	Name string `bolster:"id"`
	ID   int    `bolster:"id"`
}

type structWithInvalidID struct {
	ID map[string]string
}

type structWithInvalidTaggedID struct {
	Name []string `bolster:"id"`
}

func TestStore_Register(t *testing.T) {
	st, closer := internal.OpenTestStore(t)
	defer closer()

	t.Run("structWithoutID", func(t *testing.T) {
		err := st.Register(structWithoutID{})
		if err == nil {
			t.Errorf("expected error, got %v", err)
		} else {
			t.Log(err)
		}
	})
	t.Run("nonStruct", func(t *testing.T) {
		err := st.Register(1)
		if err == nil {
			t.Errorf("expected error, got %v", err)
		} else {
			t.Log(err)
		}
	})
	t.Run("structWithID", func(t *testing.T) {
		err := st.Register(structWithID{})
		if err != nil {
			t.Error(err)
		}
		err = st.Register(structWithID{})
		if err == nil {
			t.Errorf("expected error, got %v", err)
		} else {
			t.Log(err)
		}
		err = st.Register(&structWithID{})
		if err == nil {
			t.Errorf("expected error, got %v", err)
		} else {
			t.Log(err)
		}
		err = st.Register(structWithID{}, structWithID{})
		if err == nil {
			t.Errorf("expected error, got %v", err)
		} else {
			t.Log(err)
		}
	})
	t.Run("structWithTaggedID", func(t *testing.T) {
		err := st.Register(structWithTaggedID{})
		if err != nil {
			t.Error(err)
		}
	})
	t.Run("structWithMultipleTaggedIDs", func(t *testing.T) {
		err := st.Register(structWithMultipleTaggedIDs{})
		if err == nil {
			t.Errorf("expected error, got %v", err)
		} else {
			t.Log(err)
		}
	})
	t.Run("structWithInvalidID", func(t *testing.T) {
		err := st.Register(structWithInvalidID{})
		if err == nil {
			t.Errorf("expected error, got %v", err)
		} else {
			t.Log(err)
		}
	})
	t.Run("structWithInvalidTaggedID", func(t *testing.T) {
		err := st.Register(structWithInvalidTaggedID{})
		if err == nil {
			t.Errorf("expected error, got %v", err)
		} else {
			t.Log(err)
		}
	})
}
