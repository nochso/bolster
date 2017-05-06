package bolster

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/boltdb/bolt"
)

const (
	tagBolster = "bolster"
	tagID      = "id"
)

type Store struct {
	db    *bolt.DB
	types map[reflect.Type]typeInfo
}

func Open(path string, mode os.FileMode, options *bolt.Options) (*Store, error) {
	db, err := bolt.Open(path, mode, options)
	if err != nil {
		return nil, err
	}
	st := &Store{
		db:    db,
		types: make(map[reflect.Type]typeInfo),
	}
	return st, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Register(v ...interface{}) error {
	for _, vv := range v {
		err := s.register(vv)
		if err != nil {
			return err
		}
	}
	return nil
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
	return nil
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

func (ti typeInfo) String() string {
	return string(ti.FullName)
}
