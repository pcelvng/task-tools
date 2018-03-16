package file

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestScanner_Text(t *testing.T) {

	cases := []struct {
		msg       string
		file      string
		shouldErr bool
		expected  []string
	}{
		{
			msg:      "blank file",
			expected: []string{},
		},
		{
			msg:      "file ends in \n",
			file:     "line1\nline2\nline3\n",
			expected: []string{"line1", "line2", "line3"},
		},
		{
			msg:      "file ends without \n",
			file:     "line1\nline2\nline3",
			expected: []string{"line1", "line2", "line3"},
		},
		{
			msg:      "file contains blank line",
			file:     "line1\nline2\n\nline3",
			expected: []string{"line1", "line2", "line3"},
		},
		{
			msg:      "file ends in blank lines",
			file:     "line1\nline2\nline3\n\n\n",
			expected: []string{"line1", "line2", "line3"},
		},
	}
	defer os.Remove("test.txt")
	for _, test := range cases {
		ioutil.WriteFile("test.txt", []byte(test.file), 0666)
		reader, _ := NewReader("test.txt", nil)
		scanner := NewScanner(reader)
		var result = make([]string, 0, len(test.expected))
		for scanner.Scan() {
			result = append(result, scanner.Text())
		}
		if test.shouldErr {
			if scanner.Err() == nil {
				t.Errorf("FAIL: %q expected error", test.msg)
			}
		} else if err := scanner.Err(); err != nil {
			t.Errorf("FAIL: %q unexpected error %v", test.msg, err)
		} else if !cmp.Equal(result, test.expected) {
			t.Errorf("FAIL: %q %v", test.msg, cmp.Diff(test.expected, result))
		} else {
			t.Logf("PASS: %q", test.msg)
		}
	}
}
