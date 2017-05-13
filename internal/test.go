package internal

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/kylelemons/godebug/diff"
	"github.com/nochso/bolster"
)

func Gold(t *testing.T, actual []byte, update bool) {
	name := strings.TrimPrefix(t.Name(), "Test") + ".golden"
	name = strings.Replace(name, "_", "/", -1)
	path := filepath.Join("test-fixtures", name)
	err := os.MkdirAll(filepath.Dir(path), 0755)
	if err != nil {
		t.Error(err)
	}
	if update {
		t.Log("updating golden test file")
		err = ioutil.WriteFile(path, actual, 0644)
		if err != nil {
			t.Error(err)
		}
		return
	}
	exp, err := ioutil.ReadFile(path)
	if err != nil {
		t.Log(err)
	}
	if !bytes.Equal(exp, actual) {
		t.Error("-Actual +Expected\n" + diff.Diff(string(actual), string(exp)))
	}
}

// OpenTestStore returns a fresh store for testing and a function to close and delete it.
func OpenTestStore(t *testing.T) (*bolster.Store, func()) {
	dir, err := ioutil.TempDir("", "bolster_test")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "bolster.db")
	st, err := bolster.Open(path, 0644, nil)
	if err != nil {
		t.Fatal(err)
	}
	return st, func() {
		err := st.Close()
		if err != nil {
			t.Log(err)
		}
		err = os.RemoveAll(dir)
		if err != nil {
			t.Log(err)
		}
	}
}

func GoldStore(t *testing.T, st *bolster.Store, update bool) {
	Gold(t, DumpStore(st), update)
}

// DumpStore returns a dump containing all buckets and items.
func DumpStore(st *bolster.Store) []byte {
	buf := &bytes.Buffer{}
	st.Bolt().View(func(tx *bolt.Tx) error {
		dumpCursor(buf, tx, tx.Cursor(), 0)
		return nil
	})
	return buf.Bytes()
}

func dumpCursor(w io.Writer, tx *bolt.Tx, c *bolt.Cursor, indent int) {
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if v == nil {
			// bucket name
			bkt := hexDump(k, indent, "bkt")
			fmt.Fprintf(w, "%s\n", bkt)
			newCursor := c.Bucket().Bucket(k).Cursor()
			dumpCursor(w, tx, newCursor, indent+1)
		} else {
			// key + value
			key := hexDump(k, indent, "key")
			val := hexDump(v, indent+1, "val")
			fmt.Fprintf(w, "%s\n%s\n", key, val)
		}
	}
}

func indent(s string, depth int, prefix string) string {
	idt := strings.Repeat("    ", depth) + prefix + " "
	return idt + strings.Replace(s, "\n", "\n"+idt, -1)
}

func hexDump(b []byte, depth int, prefix string) string {
	if len(b) == 0 {
		return indent(fmt.Sprintf("%#v", b), depth, prefix)
	}
	return indent(strings.TrimSuffix(hex.Dump(b), "\n"), depth, prefix)
}
