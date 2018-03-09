package prep

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	cases := []struct {
		msg       string
		v         interface{}
		shouldErr bool
		expected  map[string]int
	}{
		{
			msg:       "no struct",
			v:         "Hello",
			shouldErr: true,
		},
		{
			msg:      "pointer struct",
			v:        &testStruct{},
			expected: map[string]int{"Name": 0, "Int1": 1, "count": 4},
		},
		{
			msg:      "normal struct",
			v:        &testStruct{},
			expected: map[string]int{"Name": 0, "Int1": 1, "count": 4},
		},
	}
	for _, test := range cases {
		p := New(test.v)
		if test.shouldErr && p != nil {
			t.Errorf("FAIL: %q expected error not found", test.msg)
		} else if !test.shouldErr && p == nil {
			t.Errorf("FAIL: %q error not expected", test.msg)
		} else if !test.shouldErr && !cmp.Equal(test.expected, p.lookup) {
			t.Errorf("FAIL: %q result mismatch %s", test.msg, cmp.Diff(test.expected, p.lookup))
		} else {
			t.Logf("PASS: %q", test.msg)
		}
	}
}

func TestPrepare_Check(t *testing.T) {
	cases := []struct {
		msg       string
		v         interface{}
		cols      []string
		shouldErr bool
	}{
		{
			msg:       "all valid columns",
			cols:      []string{"Name", "Int1", "count"},
			v:         &testStruct{},
			shouldErr: false,
		},
		{
			msg:       "field without db or json tag",
			cols:      []string{"Int3"},
			v:         &testStruct{},
			shouldErr: true,
		},
		{
			msg:       "db tag take precedence over json",
			cols:      []string{"Int4"},
			v:         &testStruct{},
			shouldErr: true,
		},
	}
	for _, test := range cases {
		p := New(test.v)
		err := p.Check(test.cols...)
		if test.shouldErr != (err != nil) {
			t.Errorf("FAIL: %q", test.msg)
		} else {
			t.Logf("PASS: %q", test.msg)
		}
	}
}
func TestPrepare_Row(t *testing.T) {
	assert.Equal(t, []interface{}{"Hello world", 42, 10}, Row(testStruct{
		Name: "Hello world",
		Int1: 10,
		Int4: 42,
	}, "Name", "count", "Int1"))

	assert.Equal(t, []interface{}{"Hello world", 42, 10}, Row(&testStruct{
		Name: "Hello world",
		Int1: 10,
		Int4: 42,
	}, "Name", "count", "Int1"))
}

func BenchmarkPrepare_RowSmall(b *testing.B) {
	p := New(testStruct{})
	for i := 0; i < b.N; i++ {
		p.Row(testStruct{
			Name: "Hello world",
			Int1: 10,
			Int4: 42,
		}, "Name", "count", "Int1")
	}
}

func BenchmarkPrepRowSmall(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Row(testStruct{
			Name: "Hello world",
			Int1: 10,
			Int4: 42,
		}, "Name", "count", "Int1")
	}
}

func BenchmarkPrepare_RowLarge(b *testing.B) {
	p := New(large{})
	for i := 0; i < b.N; i++ {
		p.Row(large{
			String1:  "hello",
			String10: "world",
			Int30:    30,
			String5:  "5",
			Float17:  17.0,
			Int7:     7,
		}, "String1", "String10", "Int30", "String5", "Float17", "Int7")
	}
}

func BenchmarkPrepRowLarge(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Row(large{
			String1:  "hello",
			String10: "world",
			Int30:    30,
			String5:  "5",
			Float17:  17.0,
			Int7:     7,
		}, "String1", "String10", "Int30", "String5", "Float17", "Int7")
	}
}

type testStruct struct {
	Name string `json:"Name"`
	Int1 int    `json:"Int1"`
	Int2 int    `json:"Int2" db:"-"` // ignore -
	Int3 int    // ignore no tags
	Int4 int    `json:"Int4" db:"count"`
}

type large struct {
	Int1  int `json:"Int1"`
	Int2  int `json:"Int2"`
	Int3  int `json:"Int3"`
	Int4  int `json:"Int4"`
	Int5  int `json:"Int5"`
	Int6  int `json:"Int6"`
	Int7  int `json:"Int7"`
	Int8  int `json:"Int8"`
	Int9  int `json:"Int9"`
	Int10 int `json:"Int10"`
	Int11 int `json:"Int11"`
	Int12 int `json:"Int12"`
	Int13 int `json:"Int13"`
	Int14 int `json:"Int14"`
	Int15 int `json:"Int15"`
	Int16 int `json:"Int16"`
	Int17 int `json:"Int17"`
	Int18 int `json:"Int18"`
	Int19 int `json:"Int19"`
	Int20 int `json:"Int20"`
	Int21 int `json:"Int21"`
	Int22 int `json:"Int22"`
	Int23 int `json:"Int23"`
	Int24 int `json:"Int24"`
	Int25 int `json:"Int25"`
	Int26 int `json:"Int26"`
	Int27 int `json:"Int27"`
	Int28 int `json:"Int28"`
	Int29 int `json:"Int29"`
	Int30 int `json:"Int30"`

	String1  string `json:"String1"`
	String2  string `json:"String2"`
	String3  string `json:"String3"`
	String4  string `json:"String4"`
	String5  string `json:"String5"`
	String6  string `json:"String6"`
	String7  string `json:"String7"`
	String8  string `json:"String8"`
	String9  string `json:"String9"`
	String10 string `json:"String10"`
	String11 string `json:"String11"`
	String12 string `json:"String12"`
	String13 string `json:"String13"`
	String14 string `json:"String14"`
	String15 string `json:"String15"`
	String16 string `json:"String16"`
	String17 string `json:"String17"`
	String18 string `json:"String18"`
	String19 string `json:"String19"`
	String20 string `json:"String20"`
	String21 string `json:"String21"`
	String22 string `json:"String22"`
	String23 string `json:"String23"`
	String24 string `json:"String24"`
	String25 string `json:"String25"`
	String26 string `json:"String26"`
	String27 string `json:"String27"`
	String28 string `json:"String28"`
	String29 string `json:"String29"`
	String30 string `json:"String30"`

	Float1  float32 `json:"Float1"`
	Float2  float32 `json:"Float2"`
	Float3  float32 `json:"Float3"`
	Float4  float32 `json:"Float4"`
	Float5  float32 `json:"Float5"`
	Float6  float32 `json:"Float6"`
	Float7  float32 `json:"Float7"`
	Float8  float32 `json:"Float8"`
	Float9  float32 `json:"Float9"`
	Float10 float32 `json:"Float10"`
	Float11 float32 `json:"Float11"`
	Float12 float32 `json:"Float12"`
	Float13 float32 `json:"Float13"`
	Float14 float32 `json:"Float14"`
	Float15 float32 `json:"Float15"`
	Float16 float32 `json:"Float16"`
	Float17 float32 `json:"Float17"`
	Float18 float32 `json:"Float18"`
	Float19 float32 `json:"Float19"`
	Float20 float32 `json:"Float20"`
	Float21 float32 `json:"Float21"`
	Float22 float32 `json:"Float22"`
	Float23 float32 `json:"Float23"`
	Float24 float32 `json:"Float24"`
	Float25 float32 `json:"Float25"`
	Float26 float32 `json:"Float26"`
	Float27 float32 `json:"Float27"`
	Float28 float32 `json:"Float28"`
	Float29 float32 `json:"Float29"`
	Float30 float32 `json:"Float30"`
}
