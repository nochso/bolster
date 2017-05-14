package bolster

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/nochso/bolster/bytesort"
	"github.com/nochso/bolster/codec"
	"github.com/nochso/bolster/codec/json"
	"github.com/nochso/bolster/errlist"
)

const (
	tagBolster       = "bolster"
	tagID            = "id"
	tagAutoIncrement = "inc"
)

// Store can store and retrieve structs.
type Store struct {
	codec codec.Interface
	db    *bolt.DB
	types map[reflect.Type]typeInfo
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
		types: make(map[reflect.Type]typeInfo),
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
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return fmt.Errorf("expected struct, got %v", t.Kind())
	}
	if _, exists := s.types[t]; exists {
		return fmt.Errorf("%v: type is already registered", t)
	}
	ti, err := newTypeInfo(t)
	if err != nil {
		return err
	}
	s.types[ti.Type] = ti
	return s.Write(func(tx *Tx) error {
		_, err := tx.btx.CreateBucketIfNotExists(ti.FullName)
		return err
	})
}

type typeInfo struct {
	FullName      []byte
	IDField       int
	AutoIncrement bool
	Type          reflect.Type
}

func newTypeInfo(t reflect.Type) (typeInfo, error) {
	ti := &typeInfo{
		FullName: []byte(t.PkgPath() + "." + t.Name()),
		Type:     t,
		IDField:  -1,
	}
	err := ti.validateIDField()
	if err != nil {
		return *ti, err
	}
	err = ti.validateBytesort()
	return *ti, err
}

func (ti *typeInfo) validateIDField() error {
	tags := newTagList(ti.Type)
	idKeys := tags.filter(tagID)
	if len(idKeys) > 1 {
		return fmt.Errorf("%v: must not have multiple fields with tag %q", ti, tagID)
	} else if len(idKeys) == 1 {
		ti.IDField = idKeys[0]
	} else if idField, ok := ti.Type.FieldByName("ID"); ok {
		ti.IDField = idField.Index[0]
	}
	if ti.IDField != -1 {
		ti.AutoIncrement = tags.contains(ti.IDField, tagAutoIncrement)
		return nil
	}
	return fmt.Errorf("%v: unable to find ID field: field has to be named \"ID\" or tagged with `bolster:\"id\"`", ti)
}

type tagList [][]string

// newTagList returns a list of bolster tags for each struct field.
func newTagList(rt reflect.Type) tagList {
	tl := make([][]string, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		ftags := strings.Split(rt.Field(i).Tag.Get(tagBolster), ",")
		tl = append(tl, ftags)
	}
	return tl
}

// filter returns the positions of fields containing a tag s.
func (tl tagList) filter(s string) []int {
	keys := []int{}
	for i := range tl {
		if tl.contains(i, s) {
			keys = append(keys, i)
		}
	}
	return keys
}

// contains returns true when i'th field contains tag s.
func (tl tagList) contains(i int, s string) bool {
	for _, w := range tl[i] {
		if w == s {
			return true
		}
	}
	return false
}

func (ti *typeInfo) validateBytesort() error {
	f := ti.Type.Field(ti.IDField)
	zv := reflect.Zero(f.Type)
	_, err := bytesort.Encode(zv.Interface())
	if err != nil {
		err = fmt.Errorf("%v: ID field %q is not byte encodable: %s", ti, f.Name, err)
	}
	return err
}

func (ti typeInfo) String() string {
	return string(ti.FullName)
}
