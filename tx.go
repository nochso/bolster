package bolster

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/boltdb/bolt"
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

var txActionIndex = [...]uint8{0, 6, 12, 18, 21, 27, 35, 43}

const (
	insert txAction = iota
	update
	upsert
	get
	delete
	truncate
	register
	txActionNames = "insertupdateupsertgetdeletetruncateregister"
)

func (a txAction) needsPointer() bool {
	return a >= insert && a <= get
}

func (a txAction) canAutoIncrement() bool {
	return a == insert || a == upsert
}

func (a txAction) String() string {
	if a < 0 || a >= txAction(len(txActionIndex)-1) {
		return fmt.Sprintf("txAction(%d)", a)
	}
	return txActionNames[txActionIndex[a]:txActionIndex[a+1]]
}

func (tx *Tx) validateStruct(v interface{}, action txAction) (structType, reflect.Value, error) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return structType{}, rv, errors.New("invalid interface")
	}
	rt := rv.Type()
	if action.needsPointer() && rt.Kind() != reflect.Ptr {
		return structType{}, rv, fmt.Errorf("expected pointer to struct, got %v", rt.Kind())
	}
	if rt.Kind() == reflect.Ptr {
		rv = rv.Elem()
		rt = rv.Type()
	}
	if rt.Kind() != reflect.Struct {
		return structType{}, rv, fmt.Errorf("expected struct, got %v", rt.Kind())
	}
	st, ok := tx.store.types[rt]
	if !ok {
		return st, rv, fmt.Errorf("unregistered struct: %v", rt)
	}
	return st, rv, nil
}

func (tx *Tx) addErr(err error) error {
	return tx.errs.Append(tx.errf.with(err))
}

// Truncate deletes all items of v's type.
func (tx *Tx) Truncate(v interface{}) error {
	st, _, err := tx.validateStruct(v, truncate)
	tx.errf = newErrorFactory(truncate, st)
	if tx.errs.HasError() {
		return tx.addErr(ErrBadTransaction)
	}
	if err != nil {
		return tx.addErr(err)
	}
	tx.addErr(tx.btx.DeleteBucket(st.FullName))
	return tx.addErr(st.init(tx))
}

// Delete removes the given item.
//
// If the item does not exist a nil error is returned.
func (tx *Tx) Delete(v interface{}) error {
	return tx.put(v, delete)
}

// Insert saves a new item.
// If an item with the same ID exists an error is returned.
func (tx *Tx) Insert(v interface{}) error {
	return tx.put(v, insert)
}

// Update overwrites an existing item.
//
// If the item does not exist an error is returned.
func (tx *Tx) Update(v interface{}) error {
	return tx.put(v, update)
}

// Upsert either updates or inserts an item.
func (tx *Tx) Upsert(v interface{}) error {
	return tx.put(v, upsert)
}

func (tx *Tx) put(v interface{}, action txAction) error {
	st, rv, err := tx.validateStruct(v, action)
	tx.errf = newErrorFactory(action, st)
	if tx.errs.HasError() {
		return tx.addErr(ErrBadTransaction)
	}
	if err != nil {
		return tx.addErr(err)
	}
	bktData := tx.btx.Bucket(st.FullName).Bucket(bktNameData)
	id := rv.Field(st.ID.StructPos)
	if action.canAutoIncrement() && st.ID.AutoIncrement {
		err = tx.autoincrement(id, bktData, st)
		if err != nil {
			return tx.addErr(err)
		}
	}
	idBytes, err := st.ID.encodeStruct(rv)
	if err != nil {
		return tx.addErr(err)
	}
	oldStructBytes := bktData.Get(idBytes)
	exists := oldStructBytes != nil
	if action == insert && exists {
		err = fmt.Errorf("item with ID %q already exists", fmt.Sprintf("%v", id.Interface()))
		return tx.addErr(err)
	} else if action == update && !exists {
		err = fmt.Errorf("item with ID %q does not exist", fmt.Sprintf("%v", id.Interface()))
		return tx.addErr(err)
	} else if action == delete && !exists {
		return nil
	}
	// struct data needs inserting or updating
	if action == insert || action == update || action == upsert {
		structBytes, err := tx.store.codec.Marshal(v)
		if err != nil {
			return tx.addErr(err)
		}
		err = bktData.Put(idBytes, structBytes)
		if err != nil {
			return tx.addErr(err)
		}
	} else if action == delete {
		// or simple deletion
		err = bktData.Delete(idBytes)
		if err != nil {
			return tx.addErr(err)
		}
	}
	// when updating or deleting items stale index data needs to be removed
	if action == update || (action == upsert && exists) || action == delete {
		// index data currently reflects the data of the struct in the database,
		// not the one being saved. we need to decode the old bytes into a
		// struct and use it to delete the old index data.
		old := reflect.New(rv.Type())
		err = tx.store.codec.Unmarshal(oldStructBytes, old.Interface())
		if err != nil {
			return tx.addErr(err)
		}
		old = reflect.Indirect(old)
		err = st.deleteIndexes(tx.btx.Bucket(st.FullName).Bucket(bktNameIndex), old, idBytes)
		if err != nil {
			return tx.addErr(err)
		}
	}
	// update the index unless deleting
	if action != delete {
		err = st.putIndexes(tx.btx.Bucket(st.FullName).Bucket(bktNameIndex), rv, idBytes)
		return tx.addErr(err)
	}
	return nil
}

func (tx *Tx) autoincrement(id reflect.Value, bkt *bolt.Bucket, st structType) error {
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

// Get fetches an item of v's type by its ID.
// v must be a pointer to a struct.
func (tx *Tx) Get(v interface{}, id interface{}) error {
	st, _, err := tx.validateStruct(v, get)
	tx.errf = newErrorFactory(get, st)
	if err != nil {
		return tx.errf.with(err)
	}
	actTypeID := reflect.TypeOf(id)
	expTypeID := st.ID.Type
	if actTypeID != expTypeID {
		return tx.errf.with(fmt.Errorf("incompatible type of ID: expected %v, got %v", expTypeID, actTypeID))
	}
	idBytes, err := st.ID.encode(id)
	if err != nil {
		return tx.errf.with(err)
	}
	bkt := tx.btx.Bucket(st.FullName).Bucket(bktNameData)
	b := bkt.Get(idBytes)
	if b == nil {
		return tx.errf.with(ErrNotFound)
	}
	return tx.errf.with(tx.store.codec.Unmarshal(b, v))
}
