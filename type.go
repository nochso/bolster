package bolster

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/nochso/bolster/bytesort"
)

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
	StructPos int
	reflect.StructField
	AutoIncrement bool
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
