package bolster

import (
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
	errs  *errlist.Errors // list of all errors so far
	errf  Error           // error factory with context
}

type txAction int

var txActionNames = [7]string{"insert", "update", "upsert", "get", "delete", "truncate", "register"}

const (
	insert txAction = iota
	update
	upsert
	get
	delete
	truncate
	register
)

func (a txAction) needsPointer() bool {
	return a >= insert && a <= get
}

func (a txAction) String() string {
	if int(a) >= len(txActionNames) || a < 0 {
		return "[unknown txAction]"
	}
	return txActionNames[a]
}

func (tx *Tx) validateStruct(v interface{}, action txAction) (typeInfo, reflect.Value, error) {
	rv := reflect.ValueOf(v)
	rt := rv.Type()
	if action.needsPointer() && rt.Kind() != reflect.Ptr {
		return typeInfo{}, rv, fmt.Errorf("expected pointer to struct, got %v", rt.Kind())
	}
	if rt.Kind() == reflect.Ptr {
		rv = rv.Elem()
		rt = rv.Type()
	}
	if rt.Kind() != reflect.Struct {
		return typeInfo{}, rv, fmt.Errorf("expected struct, got %v", rt.Kind())
	}
	ti, ok := tx.store.types[rt]
	if !ok {
		return ti, rv, fmt.Errorf("unregistered struct: %v", rt)
	}
	return ti, rv, nil
}

func (tx *Tx) addErr(err error) error {
	return tx.errs.Append(tx.errf.with(err))
}

// Truncate deletes all items of v's type.
func (tx *Tx) Truncate(v interface{}) error {
	ti, _, err := tx.validateStruct(v, truncate)
	tx.errf = newErrorFactory(truncate, ti)
	if tx.errs.HasError() {
		return tx.addErr(ErrBadTransaction)
	}
	if err != nil {
		return tx.addErr(err)
	}
	tx.addErr(tx.btx.DeleteBucket(ti.FullName))
	_, err = tx.btx.CreateBucket(ti.FullName)
	return tx.addErr(err)
}

// Delete removes the given item.
//
// If the item does not exist then nothing is done and a nil error is returned.
func (tx *Tx) Delete(v interface{}) error {
	ti, rv, err := tx.validateStruct(v, delete)
	tx.errf = newErrorFactory(delete, ti)
	if tx.errs.HasError() {
		return tx.addErr(ErrBadTransaction)
	}
	if err != nil {
		return tx.addErr(err)
	}
	id := rv.Field(ti.IDField)
	idBytes, err := bytesort.Encode(id.Interface())
	if err != nil {
		return tx.addErr(err)
	}
	bkt := tx.btx.Bucket(ti.FullName)
	return tx.addErr(bkt.Delete(idBytes))
}

// Insert saves a new item.
func (tx *Tx) Insert(v interface{}) error {
	ti, rv, err := tx.validateStruct(v, insert)
	tx.errf = newErrorFactory(insert, ti)
	if tx.errs.HasError() {
		return tx.addErr(ErrBadTransaction)
	}
	if err != nil {
		return tx.addErr(err)
	}

	bkt := tx.btx.Bucket(ti.FullName)
	id := rv.Field(ti.IDField)
	if ti.AutoIncrement {
		err = tx.autoincrement(id, bkt, ti)
		if err != nil {
			return tx.addErr(err)
		}
	}
	idBytes, err := bytesort.Encode(id.Interface())
	if err != nil {
		return tx.addErr(err)
	}

	if bkt.Get(idBytes) != nil {
		err = fmt.Errorf("item with ID %q already exists", fmt.Sprintf("%v", id.Interface()))
		return tx.addErr(err)
	}

	structBytes, err := tx.store.codec.Marshal(v)
	if err != nil {
		return tx.addErr(err)
	}
	err = bkt.Put(idBytes, structBytes)
	return tx.addErr(err)
}

func (tx *Tx) autoincrement(id reflect.Value, bkt *bolt.Bucket, ti typeInfo) error {
	idType := id.Type()
	zero := reflect.Zero(idType).Interface()
	if id.Interface() != zero {
		return nil
	}
	seq, err := bkt.NextSequence()
	if err != nil {
		return err
	}
	seqRV := reflect.ValueOf(seq)
	if !seqRV.Type().ConvertibleTo(idType) {
		return fmt.Errorf("unable to convert autoincremented ID of type %s to %s", seqRV.Type(), idType)
	}
	var overflows bool
	if idType.Kind() >= reflect.Int && idType.Kind() <= reflect.Int64 {
		signedSeq := int64(seq)
		overflows = id.OverflowInt(signedSeq)
	} else if idType.Kind() >= reflect.Uint && idType.Kind() <= reflect.Uint64 {
		overflows = id.OverflowUint(seq)
	}
	if overflows {
		return fmt.Errorf("next bucket sequence %d overflows ID field of type %s", seq, idType)
	}
	id.Set(seqRV.Convert(idType))
	return nil
}

// Get fetches v by ID.
func (tx *Tx) Get(v interface{}, id interface{}) error {
	ti, _, err := tx.validateStruct(v, get)
	tx.errf = newErrorFactory(get, ti)
	if err != nil {
		return tx.errf.with(err)
	}
	actTypeID := reflect.TypeOf(id)
	expTypeID := ti.Type.Field(ti.IDField).Type
	if actTypeID != expTypeID {
		return tx.errf.with(fmt.Errorf("incompatible type of ID: expected %v, got %v", expTypeID, actTypeID))
	}
	idBytes, err := bytesort.Encode(id)
	if err != nil {
		return tx.errf.with(err)
	}
	bkt := tx.btx.Bucket(ti.FullName)
	b := bkt.Get(idBytes)
	if b == nil {
		return tx.errf.with(ErrNotFound)
	}
	return tx.errf.with(tx.store.codec.Unmarshal(b, v))
}
