package bolster

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/boltdb/bolt"
	"github.com/nochso/bolster/bytesort"
	"github.com/nochso/bolster/errlist"
)

// Tx is a read-only or read-write transaction.
type Tx struct {
	store *Store
	btx   *bolt.Tx
	errs  errlist.Errors
}

type txAction int

const (
	insert txAction = iota
	update
	upsert
	delete
	get
)

var (
	ErrNotFound = errors.New("item not found")
)

func (tx *Tx) validateStruct(v interface{}, action txAction) (typeInfo, reflect.Value, error) {
	rv := reflect.ValueOf(v)
	rt := rv.Type()
	if action == insert || action == update || action == upsert || action == get {
		if rt.Kind() != reflect.Ptr {
			return typeInfo{}, rv, fmt.Errorf("expected pointer to struct, got %v", rt.Kind())
		}
		rv = rv.Elem()
		rt = rv.Type()
	}
	if rt.Kind() != reflect.Struct {
		return typeInfo{}, rv, fmt.Errorf("expected pointer to struct, got pointer to %v", rt.Kind())
	}
	ti, ok := tx.store.types[rt]
	if !ok {
		return ti, rv, fmt.Errorf("unregistered struct: %v", rt)
	}
	return ti, rv, nil
}

// Insert saves a new item.
func (tx *Tx) Insert(v interface{}) error {
	ti, rv, err := tx.validateStruct(v, insert)
	if err != nil {
		tx.errs = tx.errs.Append(err)
		return err
	}

	idBytes, err := bytesort.Encode(rv.Field(ti.IDField).Interface())
	if err != nil {
		tx.errs = tx.errs.Append(err)
		return err
	}

	bkt := tx.btx.Bucket(ti.FullName)
	if bkt.Get(idBytes) != nil {
		err = fmt.Errorf("Insert: %s: item with ID %q already exists", ti, fmt.Sprintf("%v", rv.Field(ti.IDField).Interface()))
		tx.errs = tx.errs.Append(err)
		return err
	}

	structBytes, err := tx.store.codec.Marshal(v)
	if err != nil {
		tx.errs = tx.errs.Append(err)
		return err
	}
	err = bkt.Put(idBytes, structBytes)
	tx.errs = tx.errs.Append(err)
	return err
}

func (tx *Tx) Get(v interface{}, id interface{}) error {
	ti, _, err := tx.validateStruct(v, get)
	if err != nil {
		return err
	}
	// TODO Check type of id for compatibility
	idBytes, err := bytesort.Encode(id)
	if err != nil {
		return err
	}
	bkt := tx.btx.Bucket(ti.FullName)
	b := bkt.Get(idBytes)
	if b == nil {
		return ErrNotFound
	}
	return tx.store.codec.Unmarshal(b, v)
}
