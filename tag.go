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
