package prep

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jbsmith7741/go-tools/appenderr"
	"github.com/pkg/errors"
)

var knownStructs map[reflect.Type]*Prepare

func init() {
	knownStructs = make(map[reflect.Type]*Prepare)
}

// Prepare a custom struct for insertion into a database using the batchloader.
// Prepare using the 'db' struct tags to correctly identify which field to use.
// If the 'db' tag is absent the 'json' tag will then be used. If neither tag is provided
// or if 'db' is set to "-" that field is ignored
type Prepare struct {
	lookup map[string]int
}

// New prepares a struct for database insertion. Will return nil if a non-struct is received
// This is optimised to return a cached Prepare object if
// the custom struct has already been prepared.
func New(v interface{}) *Prepare {
	p := &Prepare{
		lookup: make(map[string]int),
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

	for i := 0; i < vStruct.NumField(); i++ {
		tag := vStruct.Type().Field(i).Tag.Get("db")
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
		p.lookup[tag] = i
	}
	knownStructs[vStruct.Type()] = p
	return p
}

// Check if the given column names are in the prepared struct
func (p *Prepare) Check(columns ...string) error {
	errs := appenderr.New()
	for _, c := range columns {
		if _, found := p.lookup[c]; !found {
			errs.Add(fmt.Errorf("%v", c))
		}
	}
	if errs.ErrOrNil() != nil {
		return errors.Wrap(errs, "columns not found")
	}
	return errs.ErrOrNil()
}

// Columns found in the prepared struct
func (p *Prepare) Columns() (cols []string) {
	for name := range p.lookup {
		cols = append(cols, name)
	}
	return cols
}

// Row prepares a row insert for struct v for the provided columns.
func (p *Prepare) Row(v interface{}, cols ...string) (args []interface{}) {
	vStruct := reflect.ValueOf(v)
	if vStruct.Kind() == reflect.Ptr {
		vStruct = vStruct.Elem()
	}
	for _, v := range cols {
		i, found := p.lookup[v]
		if found {
			args = append(args, vStruct.Field(i).Interface())
		}
	}
	return args
}

// Row prepares a row insert for struct v for the provided columns.
func Row(v interface{}, cols ...string) (row []interface{}) {
	return New(v).Row(v, cols...)
}
