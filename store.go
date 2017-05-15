package bolster

import (
	"errors"
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
	err = s.Write(func(tx *Tx) error {
		_, err = tx.btx.CreateBucketIfNotExists(st.FullName)
		return err
	})
	return e.with(err)
}

type structType struct {
	FullName []byte
	ID       idField
	Type     reflect.Type
}

func newStructType(t reflect.Type) (structType, error) {
	st := &structType{
		FullName: []byte(t.PkgPath() + "." + t.Name()),
		Type:     t,
	}
	var err error
	st.ID, err = newIDField(t)
	if err != nil {
		return *st, err
	}
	err = st.validateBytesort()
	return *st, err
}

func (st *structType) validateBytesort() error {
	zv := reflect.Zero(st.ID.Type)
	_, err := bytesort.Encode(zv.Interface())
	if err != nil {
		err = fmt.Errorf("ID field %q is not byte encodable: %s", st.ID.Name, err)
	}
	return err
}

func (st structType) String() string {
	return string(st.FullName)
}

type idField struct {
	StructPos     int
	AutoIncrement bool
	reflect.StructField
}

func (i idField) isInteger() bool {
	return i.Type.Kind() >= reflect.Int && i.Type.Kind() <= reflect.Uint64
}

func (i idField) encode(v interface{}) ([]byte, error) {
	f := reflect.ValueOf(v)
	// always encode integer IDs with 8 bytes length
	if i.isInteger() && f.Type().Size() < 8 {
		k := f.Type().Kind()
		if k >= reflect.Int && k <= reflect.Int64 {
			f = f.Convert(reflect.TypeOf(int64(0)))
		} else {
			f = f.Convert(reflect.TypeOf(uint64(0)))
		}
	}
	return bytesort.Encode(f.Interface())
}

func (i idField) encodeStruct(structRV reflect.Value) ([]byte, error) {
	return i.encode(structRV.Field(i.StructPos).Interface())
}

func newIDField(t reflect.Type) (idField, error) {
	id := idField{StructPos: -1}
	tags := newTagList(t)
	idKeys := tags.filter(tagID)
	if len(idKeys) > 1 {
		return id, fmt.Errorf("must not have multiple fields with tag %q", tagID)
	} else if len(idKeys) == 1 {
		id.StructPos = idKeys[0]
	} else if idField, ok := t.FieldByName("ID"); ok {
		id.StructPos = idField.Index[0]
	}
	if id.StructPos == -1 {
		return id, errors.New("unable to find ID field: field has to be named \"ID\" or tagged with `bolster:\"id\"`")
	}
	id.StructField = t.Field(id.StructPos)
	id.AutoIncrement = tags.contains(id.StructPos, tagAutoIncrement)
	if !id.isInteger() && id.AutoIncrement {
		return id, fmt.Errorf("autoincremented IDs must be integer, got %s", id.Type.Kind())
	}
	return id, nil
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
