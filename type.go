package bolster

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/nochso/bolster/bytesort"
)

type structType struct {
	FullName []byte
	ID       idField
	Type     reflect.Type
	Indexes  []index
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
	st.Indexes, err = newIndexSlice(t)
	if err != nil {
		return *st, err
	}
	if !st.ID.isInteger() {
		// non-integer IDs need to be uniquely mapped to uint64 IDs
		idx := index{
			Unique: true,
			Fields: []indexField{{st.ID.StructPos, st.ID.StructField}},
		}
		idx.FullName = idx.getFullName()
		st.ID.IntIndex = idx
		st.Indexes = append(st.Indexes, idx)
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

func (st structType) init(tx *Tx) error {
	bkt, err := tx.btx.CreateBucketIfNotExists(st.FullName)
	if err != nil {
		return err
	}
	_, err = bkt.CreateBucketIfNotExists(bktNameData)
	if err != nil {
		return err
	}
	idxBkt, err := bkt.CreateBucketIfNotExists(bktNameIndex)
	if err != nil {
		return err
	}
	for _, idx := range st.Indexes {
		_, err = idxBkt.CreateBucketIfNotExists(idx.FullName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (st structType) putIndexes(bkt *bolt.Bucket, rv reflect.Value, id []byte) error {
	for _, idx := range st.Indexes {
		err := idx.put(bkt, rv, id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (st structType) deleteIndexes(bkt *bolt.Bucket, rv reflect.Value, id []byte) error {
	for _, idx := range st.Indexes {
		err := idx.delete(bkt, rv, id)
		if err != nil {
			return err
		}
	}
	return nil
}

type idField struct {
	StructPos int
	reflect.StructField
	AutoIncrement bool
	IntIndex      index // non-integer type mapping to uint64
}

func (i idField) isInteger() bool {
	return i.Type.Kind() >= reflect.Int && i.Type.Kind() <= reflect.Uint64
}

func (i idField) encode(v interface{}, bkt *bolt.Bucket, a txAction) ([]byte, error) {
	f := reflect.ValueOf(v)
	if i.isInteger() {
		// always encode integer IDs with 8 bytes length
		if f.Type().Size() < 8 {
			k := f.Type().Kind()
			if k >= reflect.Int && k <= reflect.Int64 {
				f = f.Convert(reflect.TypeOf(int64(0)))
			} else {
				f = f.Convert(reflect.TypeOf(uint64(0)))
			}
		}
		return bytesort.Encode(f.Interface())
	}
	// non-integer IDs need to be mapped to uint64
	b, err := i.IntIndex.get(bkt, v)
	if err == ErrNotFound {
		if a != insert && a != upsert {
			return nil, err
		}
		// ID is unknown but we're inserting or upserting so get the next ID
		var id uint64
		id, err = bkt.NextSequence()
		if err != nil {
			return nil, err
		}
		return bytesort.Encode(id)
	}
	if a == insert {
		return nil, fmt.Errorf("item with ID %q already exists", fmt.Sprintf("%v", v))
	}
	return b, err
}

func (i idField) encodeStruct(structRV reflect.Value, bkt *bolt.Bucket, a txAction) ([]byte, error) {
	return i.encode(structRV.Field(i.StructPos).Interface(), bkt, a)
}

func newIDField(t reflect.Type) (idField, error) {
	id := idField{StructPos: -1}
	stl := newStructTagList(t)
	idKeys := stl.filter(tagID)
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
	id.AutoIncrement = stl.contains(id.StructPos, tagAutoIncrement)
	if !id.isInteger() && id.AutoIncrement {
		return id, fmt.Errorf("autoincremented IDs must be integer, got %s", id.Type.Kind())
	}
	return id, nil
}

func newIndexSlice(t reflect.Type) ([]index, error) {
	stl := newStructTagList(t)
	var is []index
	mfis := make(map[string]map[int]int)
	for fieldPos, tags := range stl {
		for _, tag := range tags {
			words := strings.Fields(tag)
			if len(words) == 0 {
				continue
			}
			if words[0] == tagIndex {
				if len(words) == 1 {
					idx := index{Fields: []indexField{{fieldPos, t.Field(fieldPos)}}}
					idx.FullName = idx.getFullName()
					is = append(is, idx)
				} else if len(words) == 3 {
					// index <index name> <position of field in index>
					// 0      1            2
					idxFieldPos, err := strconv.Atoi(words[2])
					if err != nil {
						return nil, err
					}
					if _, ok := mfis[words[1]]; !ok {
						mfis[words[1]] = make(map[int]int)
					}
					mfis[words[1]][idxFieldPos] = fieldPos
				}
			}
		}
	}
	for idxID, positions := range mfis {
		idx := index{}
		for i := 0; i < len(positions); i++ {
			fieldPos, ok := positions[i]
			if !ok {
				err := fmt.Errorf("index %q has %d field(s) and its field order must be 0..%d: field %d is missing", idxID, len(positions), len(positions)-1, i)
				return nil, err
			}
			f := indexField{fieldPos, t.Field(fieldPos)}
			idx.Fields = append(idx.Fields, f)
		}
		idx.FullName = idx.getFullName()
		is = append(is, idx)
	}
	return is, nil
}

type index struct {
	FullName []byte
	Unique   bool
	Fields   []indexField
}

func (i index) getFullName() []byte {
	buf := &bytes.Buffer{}
	if i.Unique {
		buf.WriteByte('u')
	} else {
		buf.WriteByte('i')
	}
	for _, field := range i.Fields {
		fmt.Fprintf(buf, ",%s %s %s", field.Type.PkgPath(), field.Type, field.Name)
	}
	return buf.Bytes()
}

func (i index) get(bkt *bolt.Bucket, v ...interface{}) ([]byte, error) {
	if !i.Unique {
		return nil, errors.New("index.get only works on unique indexes")
	}
	if len(v) != len(i.Fields) {
		return nil, errors.New("amount of values does not match count of index fields")
	}
	bkt = bkt.Bucket(i.FullName)
	key := &bytes.Buffer{}
	for n, field := range i.Fields {
		b, err := bytesort.Encode(v[n])
		if err != nil {
			return nil, err
		}
		key.Write(b)
		if field.Type.Kind() == reflect.String && n < len(i.Fields)-1 {
			bkt = bkt.Bucket(key.Bytes())
			if bkt == nil {
				return nil, ErrNotFound
			}
			key.Reset()
		}
	}
	b := bkt.Get(key.Bytes())
	if b == nil {
		return nil, ErrNotFound
	}
	return b, nil
}

func (i index) put(bkt *bolt.Bucket, rv reflect.Value, id []byte) error {
	bkt = bkt.Bucket(i.FullName)
	key := &bytes.Buffer{}
	for n, field := range i.Fields {
		b, err := bytesort.Encode(rv.Field(field.StructPos).Interface())
		if err != nil {
			return err
		}
		key.Write(b)
		if field.Type.Kind() == reflect.String && n < len(i.Fields)-1 {
			bkt, err = bkt.CreateBucketIfNotExists(key.Bytes())
			if err != nil {
				return err
			}
			key.Reset()
		}
	}
	if i.Unique {
		// Key -> value (value being the primary ID)
		return bkt.Put(key.Bytes(), id)
	}
	key.Write(id)
	return bkt.Put(key.Bytes(), nil)
}

func (i index) delete(bkt *bolt.Bucket, rv reflect.Value, id []byte) error {
	bkt = bkt.Bucket(i.FullName)
	key := &bytes.Buffer{}
	for n, field := range i.Fields {
		b, err := bytesort.Encode(rv.Field(field.StructPos).Interface())
		if err != nil {
			return err
		}
		key.Write(b)
		if field.Type.Kind() == reflect.String && n < len(i.Fields)-1 {
			bkt = bkt.Bucket(key.Bytes())
			if bkt == nil {
				// odd, the index is out of sync. still fulfills the delete though.
				return nil
			}
			key.Reset()
		}
	}
	// TODO Delete empty buckets
	if i.Unique {
		// Key -> value (value being the primary ID)
		return bkt.Delete(key.Bytes())
	}
	key.Write(id)
	return bkt.Delete(key.Bytes())
}

type indexField struct {
	StructPos int
	reflect.StructField
}
