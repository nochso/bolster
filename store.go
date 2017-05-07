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
	tagBolster = "bolster"
	tagID      = "id"
)

type Store struct {
	codec codec.Interface
	db    *bolt.DB
	types map[reflect.Type]typeInfo
}

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

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Read(fn func(*Tx) error) error {
	return s.db.View(func(btx *bolt.Tx) error {
		tx := &Tx{btx: btx, store: s}
		err := fn(tx)
		if err != nil {
			return err
		}
		return tx.errs.ErrorOrNil()
	})
}

func (s *Store) Write(fn func(*Tx) error) error {
	return s.db.Update(func(btx *bolt.Tx) error {
		tx := &Tx{btx: btx, store: s}
		err := fn(tx)
		if err != nil {
			return err
		}
		return tx.errs.ErrorOrNil()
	})
}

func (s *Store) Register(v ...interface{}) error {
	errs := errlist.New()
	for _, vv := range v {
		errs = errs.Append(s.register(vv))
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
	FullName []byte
	IDField  int
	Type     reflect.Type
}

func newTypeInfo(t reflect.Type) (typeInfo, error) {
	ti := &typeInfo{
		FullName: []byte(t.PkgPath() + "." + t.Name()),
		Type:     t,
	}
	err := ti.validateIDField()
	if err != nil {
		return *ti, err
	}
	err = ti.validateBytesort()
	return *ti, err
}

func (ti *typeInfo) validateIDField() error {
	for i := 0; i < ti.Type.NumField(); i++ {
		f := ti.Type.Field(i)
		tags := f.Tag.Get(tagBolster)
		for _, tag := range strings.Split(tags, ",") {
			if tag == tagID {
				ti.IDField = i
				return nil
			}
		}
	}
	if idField, ok := ti.Type.FieldByName("ID"); ok {
		ti.IDField = idField.Index[0]
		return nil
	}
	return fmt.Errorf("%v: unable to find ID field: field has to be named \"ID\" or tagged with `bolster:\"id\"`", ti)
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
