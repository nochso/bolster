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
	// ErrNotFound is returned when a specific item could not be found.
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

	bkt := tx.btx.Bucket(ti.FullName)
	if ti.AutoIncrement {
		idType := rv.Field(ti.IDField).Type()
		zero := reflect.Zero(idType).Interface()
		if zero == rv.Field(ti.IDField).Interface() {
			seq, err := bkt.NextSequence()
			if err != nil {
				tx.errs = tx.errs.Append(err)
				return err
			}
			seqRV := reflect.ValueOf(seq)
			if !seqRV.Type().ConvertibleTo(idType) {
				err = fmt.Errorf("Insert: %s: unable to convert autoincremented ID of type %s to %s", ti, seqRV.Type(), idType)
				tx.errs = tx.errs.Append(err)
				return err
			}
			var overflows bool
			switch idType.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				signedSeq := int64(seq)
				overflows = rv.Field(ti.IDField).OverflowInt(signedSeq)
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				overflows = rv.Field(ti.IDField).OverflowUint(seq)
			}
			if overflows {
				err = fmt.Errorf("Insert: %s: next bucket sequence %d overflows ID field of type %s", ti, seq, idType)
				tx.errs = tx.errs.Append(err)
				return err
			}
			rv.Field(ti.IDField).Set(seqRV.Convert(idType))
		}
	}

	idBytes, err := bytesort.Encode(rv.Field(ti.IDField).Interface())
	if err != nil {
		tx.errs = tx.errs.Append(err)
		return err
	}

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

// Get fetches v by ID.
func (tx *Tx) Get(v interface{}, id interface{}) error {
	ti, _, err := tx.validateStruct(v, get)
	if err != nil {
		return err
	}
	actTypeID := reflect.TypeOf(id)
	expTypeID := ti.Type.Field(ti.IDField).Type
	if actTypeID != expTypeID {
		return fmt.Errorf("Get: %s: incompatible type of ID: expected %v, got %v", ti, expTypeID, actTypeID)
	}
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
