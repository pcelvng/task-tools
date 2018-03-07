package prep

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/jbsmith7741/go-tools/appenderr"
)

var knownStructs map[reflect.Type]*Prepare

func init() {
	knownStructs = make(map[reflect.Type]*Prepare)
}

type Prepare struct {
	lookup map[string]int
}

func New(v interface{}) *Prepare {
	p := &Prepare{
		lookup: make(map[string]int),
	}
	vStruct := reflect.ValueOf(v)
	if vStruct.Kind() == reflect.Ptr {
		vStruct = vStruct.Elem()
	}
	if vStruct.Kind() != reflect.Struct {
		return nil //, fmt.Errorf("Must pass a struct not %v", vStruct.Kind())
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

func (p *Prepare) Check(columns ...string) error {
	errs := appenderr.New()
	for _, c := range columns {
		if _, found := p.lookup[c]; !found {
			errs.Add(fmt.Errorf("%v", c))
		}
	}
	return errs.ErrOrNil()
}

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

func Row(v interface{}, cols ...string) (row []interface{}) {
	p := New(v)
	return p.Row(v, cols...)
}
