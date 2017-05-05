package internal

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kylelemons/godebug/diff"
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
