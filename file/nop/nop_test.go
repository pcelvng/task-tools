package nop

import (
	"testing"
	"time"

	"github.com/hydronica/trial"

	"github.com/pcelvng/task-tools/file/stat"
)

func TestStat(t *testing.T) {
	tm := time.Now().UTC().Truncate(24 * time.Hour).Format(time.RFC3339)
	cases := trial.Cases[string, stat.Stats]{
		"file.txt": {
			Input: "nop://file.txt",
			Expected: stat.Stats{
				LineCnt:  10,
				Size:     123,
				Checksum: "28130874f9b9eb9711de4606399b7231",
				Created:  tm,
				Path:     "nop://file.txt"},
		},
		"error": {
			Input:     "nop://err",
			ShouldErr: true,
		},
		"directory": {
			Input: "nop://path/to/dir?stat_dir",
			Expected: stat.Stats{
				LineCnt:  10,
				Size:     123,
				Checksum: "0df5b11bdb0296d6f0646c840d64e738",
				Created:  tm,
				IsDir:    true,
				Path:     "nop://path/to/dir?stat_dir"},
		},
	}
	trial.New(Stat, cases).SubTest(t)
}

func TestNewReader(t *testing.T) {
	fn := func(path string) (string, error) {
		r, err := NewReader(path)
		if err != nil {
			return "", err
		}
		return r.sts.Path(), nil
	}

	cases := trial.Cases[string, string]{
		"happy path": {
			Input:    "nop://file.txt",
			Expected: "nop://file.txt",
		},
		"init error": {
			Input:     "nop://init_err",
			ShouldErr: true,
		},
	}

	trial.New(fn, cases).SubTest(t)
}

func TestNewWriter(t *testing.T) {
	fn := func(path string) (string, error) {
		w, err := NewWriter(path)
		if err != nil {
			return "", err
		}
		return w.sts.Path(), nil
	}

	cases := trial.Cases[string, string]{
		"happy path": {
			Input:    "nop://file.txt",
			Expected: "nop://file.txt",
		},
		"init error": {
			Input:     "nop://init_err",
			ShouldErr: true,
		},
	}

	trial.New(fn, cases).SubTest(t)
}

