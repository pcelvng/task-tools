package dedup

import (
	"bytes"
	"strings"
	"sync"

	"github.com/json-iterator/go"
)

var json = jsoniter.ConfigFastest

func New() *Dedup {
	return &Dedup{
		linesMp: make(map[string][]byte),
	}
}

// Dedup will dedup lines added to AddLine by the provided key.
// Newer lines replace older lines. If order is important then consider
// sorting all the lines first.
type Dedup struct {
	linesMp map[string][]byte // unique list of linesMp by key

	mu sync.Mutex
}

// AddLine will add the bytes record b to the pool of deduped linesMp.
// As a help, basic csv and json key generators are provided as standalone
// functions in this package.
func (w *Dedup) Add(key string, b []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.linesMp[key] = b
}

// Lines returns the deduped lines.
func (w *Dedup) Lines() [][]byte {
	lines := make([][]byte, len(w.linesMp))
	i := 0
	for _, ln := range w.linesMp {
		lines[i] = ln
		i++
	}

	return lines
}

// KeyFromJSON generates a 'key' of fields values by concatenating
// the field values in the order received in fields.
//
// If returned string is empty then the fields were not found.
func KeyFromJSON(b []byte, fields []string) string {
	var key string
	for _, field := range fields {
		s := json.Get(b, field).ToString()
		key += s + "|"
	}
	key = strings.TrimRight(key, "|")
	return key
}

// KeyFromCSV generates a 'key' of fields values by concatenating
// the field values in the order received in fields.
//
// If returned string is empty then the fields were not found.
//
// If a fields index value is out of range then that field is ignored
// and not included in the key.
func KeyFromCSV(b []byte, fields []int, sep []byte) string {
	pieces := bytes.Split(b, sep)
	key := ""
	for _, fieldIndex := range fields {
		if fieldIndex < len(pieces) {
			key = key + string(pieces[fieldIndex])
		}
	}

	return key
}
