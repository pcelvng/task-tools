package fixtures

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pcelvng/task"
)

func TestPythonLauncherDoneShape(t *testing.T) {
	pyRoot, err := filepath.Abs(filepath.Join("..", "..", "python"))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	in := filepath.Join(dir, "in.jsonl")
	done := filepath.Join(dir, "done.jsonl")
	inTsk := task.New("echo", "x")
	if err := os.WriteFile(in, append(inTsk.JSONBytes(), '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("python3", "tests/launcher_once.py", in, done)
	cmd.Dir = pyRoot
	cmd.Env = append(os.Environ(), "PYTHONPATH="+pyRoot)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("launcher: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(done)
	if err != nil {
		t.Fatal(err)
	}
	doneTsk, err := task.NewFromBytes([]byte(strings.TrimSpace(string(raw))))
	if err != nil {
		t.Fatal(err)
	}
	if doneTsk.Result != task.CompleteResult {
		t.Fatalf("result: %s", doneTsk.Result)
	}
	if doneTsk.Msg != "ok" {
		t.Fatalf("msg: %s", doneTsk.Msg)
	}
	if doneTsk.Started == "" || doneTsk.Ended == "" {
		t.Fatal("missing started/ended")
	}
}