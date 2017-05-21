package bolster

import (
	"errors"
	"fmt"
	"os"
	"reflect"

	"github.com/boltdb/bolt"
	"github.com/nochso/bolster/codec"
	"github.com/nochso/bolster/codec/json"
	"github.com/nochso/bolster/errlist"
)

var (
	bktNameData  = []byte("data")
	bktNameIndex = []byte("index")
)

// Store can store and retrieve structs.
type Store struct {
	codec codec.Interface
	db    *bolt.DB
	types map[reflect.Type]structType
}

// Open creates and opens a Store.
func Open(path string, mode os.FileMode, options *bolt.Options) (*Store, error) {
	db, err := bolt.Open(path, mode, options)
	if err != nil {
		return nil, err
	}
	st := &Store{
		codec: json.Codec,
		db:    db,
		types: make(map[reflect.Type]structType),
	}
	return st, nil
}

// Bolt returns the bolt.DB instance.
func (s *Store) Bolt() *bolt.DB {
	return s.db
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (s *Store) Close() error {
	return s.db.Close()
}

// Read executes a function within the context of a managed read-only transaction.
// Any error that is returned from the function is returned from the View() method.
func (s *Store) Read(fn func(*Tx) error) error {
	return s.db.View(func(btx *bolt.Tx) error {
		tx := &Tx{btx: btx, store: s, errs: errlist.New()}
		err := fn(tx)
		if err != nil {
			return err
		}
		return tx.errs.ErrorOrNil()
	})
}

// Write executes a function within the context of a read-write managed transaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the commit is
// returned from the Write() method.
func (s *Store) Write(fn func(*Tx) error) error {
	return s.db.Update(func(btx *bolt.Tx) error {
		tx := &Tx{btx: btx, store: s, errs: errlist.New()}
		err := fn(tx)
		if err != nil {
			return err
		}
		return tx.errs.ErrorOrNil()
	})
}

// Register validates struct types for later use.
// Structs that have not been registered can not be used.
func (s *Store) Register(v ...interface{}) error {
	errs := errlist.New()
	for _, vv := range v {
		errs.Append(s.register(vv))
	}
	return errs.ErrorOrNil()
}

func (s *Store) register(v interface{}) error {
	e := newErrorFactory(register)
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return e.with(fmt.Errorf("expected struct, got %v", t.Kind()))
	}
	if st, exists := s.types[t]; exists {
		e.structType = st
		return e.with(errors.New("type is already registered"))
	}
	st, err := newStructType(t)
	e.structType = st
	if err != nil {
		return e.with(err)
	}
	s.types[st.Type] = st
	return e.with(s.Write(st.init))
}
