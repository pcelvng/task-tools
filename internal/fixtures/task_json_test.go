package fixtures

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
)

func TestTaskJSONGolden(t *testing.T) {
	fn := func(name string) (string, error) {
		switch name {
		case "minimal":
			return string((&task.Task{Type: "test-type", Info: "test-info"}).JSONBytes()), nil
		case "full_in":
			return string((&task.Task{
				Type:    "nop",
				Job:     "job-1",
				Info:    `{"x":1}`,
				Created: "2000-01-01T01:01:00Z",
				ID:      "550e8400-e29b-41d4-a716-446655440000",
				Meta:    "a=1&b=two",
			}).JSONBytes()), nil
		case "meta_special":
			return string((&task.Task{
				Type: "t",
				Info: "i",
				Meta: "q=a%26b&key=hello%20world",
			}).JSONBytes()), nil
		case "completed":
			tsk, err := task.NewFromBytes([]byte(`{"type":"test-type","info":"test-info","created":"2000-01-01T00:00:00Z","result":"complete","msg":"test msg","started":"2000-01-01T00:00:00Z","ended":"2000-01-01T00:00:00Z"}`))
			if err != nil {
				return "", err
			}
			return string(tsk.JSONBytes()), nil
		case "lifecycle":
			tsk := task.NewWithID("nop", "info", "id-1")
			tsk.Start()
			tsk.End(task.CompleteResult, "ok")
			return string(tsk.JSONBytes()), nil
		default:
			t.Fatalf("unknown case %q", name)
			return "", nil
		}
	}

	cases := trial.Cases[string, string]{
		"minimal":      {Input: "minimal", Expected: `{"type":"test-type","info":"test-info"}`},
		"full_in":      {Input: "full_in", Expected: `{"type":"nop","job":"job-1","info":"{\"x\":1}","created":"2000-01-01T01:01:00Z","id":"550e8400-e29b-41d4-a716-446655440000","meta":"a=1&b=two"}`},
		"meta_special": {Input: "meta_special", Expected: `{"type":"t","info":"i","meta":"q=a%26b&key=hello%20world"}`},
	}

	trial.New(fn, cases).Test(t)

	dir := filepath.Join("..", "..", "python", "tests", "fixtures")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"minimal", "full_in", "meta_special", "completed", "lifecycle"} {
		got, err := fn(name)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, name+".json")
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}