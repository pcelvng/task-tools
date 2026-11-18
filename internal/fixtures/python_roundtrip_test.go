package fixtures

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hydronica/trial"
)

func TestPythonTaskCodecGolden(t *testing.T) {
	pyRoot := filepath.Join("..", "..", "python")
	fn := func(name string) (string, error) {
		fixture := filepath.Join(pyRoot, "tests", "fixtures", name+".json")
		raw, err := os.ReadFile(fixture)
		if err != nil {
			return "", err
		}
		script := `
import sys
from task_tools.task import new_from_bytes
raw = sys.stdin.buffer.read()
sys.stdout.buffer.write(new_from_bytes(raw).json_bytes())
`
		cmd := exec.Command("python3", "-c", script)
		cmd.Dir = pyRoot
		cmd.Env = append(os.Environ(), "PYTHONPATH="+pyRoot)
		cmd.Stdin = strings.NewReader(string(raw))
		out, err := cmd.Output()
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				return "", ee
			}
			return "", err
		}
		return string(out), nil
	}

	cases := trial.Cases[string, string]{
		"minimal":      {Input: "minimal", Expected: `{"type":"test-type","info":"test-info"}`},
		"full_in":      {Input: "full_in", Expected: `{"type":"nop","job":"job-1","info":"{\"x\":1}","created":"2000-01-01T01:01:00Z","id":"550e8400-e29b-41d4-a716-446655440000","meta":"a=1&b=two"}`},
		"meta_special": {Input: "meta_special", Expected: `{"type":"t","info":"i","meta":"q=a%26b&key=hello%20world"}`},
		"completed": {
			Input:    "completed",
			Expected: `{"type":"test-type","info":"test-info","created":"2000-01-01T00:00:00Z","result":"complete","msg":"test msg","started":"2000-01-01T00:00:00Z","ended":"2000-01-01T00:00:00Z"}`,
		},
	}

	trial.New(fn, cases).Test(t)
}