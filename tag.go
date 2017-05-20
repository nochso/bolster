package bolster

import (
	"reflect"
	"strings"
)

const (
	tagBolster       = "bolster"
	tagID            = "id"
	tagAutoIncrement = "inc"
)

type structTagList [][]string

// newStructTagList returns a list of bolster tags for each struct field.
func newStructTagList(rt reflect.Type) structTagList {
	tl := make([][]string, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		ftags := strings.Split(rt.Field(i).Tag.Get(tagBolster), ",")
		tl = append(tl, ftags)
	}
	return tl
}

// filter returns the positions of fields containing a tag s.
func (stl structTagList) filter(s string) []int {
	keys := []int{}
	for i := range stl {
		if stl.contains(i, s) {
			keys = append(keys, i)
		}
	}
	return keys
}

// contains returns true when i'th field contains tag s.
func (stl structTagList) contains(i int, s string) bool {
	for _, w := range stl[i] {
		if w == s {
			return true
		}
	}
	return false
}
