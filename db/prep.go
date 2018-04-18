package db

import (
	"reflect"
	"strings"

	"github.com/pkg/errors"
)

var knownStructs = make(map[reflect.Type]*prepare)

// CheckColumns will return a non-nil error if one or more
// columns from cols is missing from the underlying struct.
func CheckColumns(v interface{}, cols ...string) error {
	missing := MissingColumns(v, cols...)
	if len(missing) > 0 {
		return errors.Errorf("columns not found: %s", strings.Join(missing, ", "))
	}

	return nil
}

// MissingColumns returns a list of columns from cols that were not found
// in the provided struct.
func MissingColumns(v interface{}, cols ...string) (missing []string) {
	return prep(v).missingColumns(cols...)
}

// Columns returns all the struct table columns according
// to the 'db' and 'json' meta tag values.
func Columns(v interface{}) (cols []string) {
	return prep(v).columns()
}

// Values prepares a row insert for struct v for the provided columns.
func Values(v interface{}, cols ...string) (row []interface{}) {
	return prep(v).values(v, cols...)
}

// prepare a custom struct for insertion into a database using the batchloader.
// prepare using the 'db' struct tags to correctly identify which field to use.
// If the 'db' tag is absent the 'json' tag will then be used. If neither tag is provided
// or if 'db' is set to "-" that field is ignored
type prepare struct {
	// lookup is a cache of reflected column names (key) and it's corresponding cardinality - order (value)
	lookup map[string]data
}

type data struct {
	index    int
	nullable bool
}

// prep prepares a struct for database insertion. Will return nil if a non-struct is received
// This is optimised to return a cached prepare object if
// the custom struct has already been prepared.
func prep(v interface{}) *prepare {
	p := &prepare{
		lookup: make(map[string]data),
	}
	vStruct := reflect.ValueOf(v)
	if vStruct.Kind() == reflect.Ptr {
		vStruct = vStruct.Elem()
	}
	if vStruct.Kind() != reflect.Struct {
		return nil
	}

	if p, found := knownStructs[vStruct.Type()]; found {
		return p
	}

	// reflect on public field tags to discover column name.
	// 1. Use 'db' tag value if exists
	// 2. Ignore if 'db' tag == "-"
	// 2. Use 'json' tag value (if exists) as column name
	for i := 0; i < vStruct.NumField(); i++ {
		s := strings.Split(vStruct.Type().Field(i).Tag.Get("db"), ",")
		tag := s[0]
		nullzero := len(s) > 1 && s[1] == "nullzero"

		if tag == "-" {
			continue
		}
		if tag == "" {
			tag = vStruct.Type().Field(i).Tag.Get("json")
			if tag == "" {
				continue
			}
			tag = strings.Split(tag, ",")[0]
		}
		p.lookup[tag] = data{i, nullzero}
	}
	knownStructs[vStruct.Type()] = p
	return p
}

// missingColumns returns a list of columns from cols that were not found
// in the provided struct.
func (p *prepare) missingColumns(columns ...string) (missing []string) {
	for _, c := range columns {
		if _, found := p.lookup[c]; !found {
			missing = append(missing, c)
		}
	}

	return missing
}

// columns returns a list of all columns specified in the underlying
// struct 'db' and 'json' tags.
func (p *prepare) columns() (cols []string) {
	for name := range p.lookup {
		cols = append(cols, name)
	}
	return cols
}

// values returns the corresponding row values in same order as cols.
func (p *prepare) values(i interface{}, cols ...string) (args []interface{}) {
	vStruct := reflect.ValueOf(i)
	if vStruct.Kind() == reflect.Ptr {
		vStruct = vStruct.Elem()
	}
	for _, col := range cols {
		d, found := p.lookup[col]
		if found {
			v := vStruct.Field(d.index).Interface()
			if d.nullable && isZero(v) {
				v = nil
			}
			args = append(args, v)
		}
	}
	return args
}

func isZero(i interface{}) bool {
	z := reflect.Zero(reflect.TypeOf(i))
	return i == z.Interface()
}
