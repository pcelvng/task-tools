package main

import (
	"errors"
	"sort"
	"testing"

	"github.com/hydronica/trial"
	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/workflow"
	"github.com/pcelvng/task/bus/nop"
)

func TestUnmarshalStat(t *testing.T) {
	fn := func(i trial.Input) (interface{}, error) {
		return unmarshalStat([]byte(i.String())), nil
	}
	cases := trial.Cases{
		"invalid": {
			Input:    "",
			Expected: stat.Stats{},
		},
		"GCP": {
			Input: `{
  "kind": "storage#object",
  "id": "gcp-project/task/path/2021/11/17/21/16/20211117T2116-0015bf8f.json.gz/1637191065933407",
  "selfLink": "https://www.googleapis.com/storage/v1/b/task/o/task%path%2F2021%2F11%2F17%2F21%2F16%2F20211117T2116-0015bf8f.json.gz",
  "name": "task/path/2021/11/17/21/16/20211117T2116-0015bf8f.json.gz",
  "bucket": "gcp-project",
  "generation": "1637191065933407",
  "metageneration": "1",
  "contentType": "application/octet-stream",
  "timeCreated": "2021-11-17T23:17:45.997Z",
  "updated": "2021-11-17T23:17:45.997Z",
  "storageClass": "STANDARD",
  "timeStorageClassUpdated": "2021-11-17T23:17:45.997Z",
  "size": "14725896",
  "md5Hash": "wDJDr49QUgrsBzXoUCBqpQ==",
  "mediaLink": "https://www.googleapis.com/download/storage/v1/b/gcp-project/o/task%path%2F2021%2F11%2F17%2F21%2F16%2F20211117T2116-0015bf8f.json.gz?generation=1637191065933407&alt=media",
  "crc32c": "9gY7HQ==",
  "etag": "CN+sqP/DoPQCEAE="
}`,
			Expected: stat.Stats{
				Size:     14725896,
				Checksum: "wDJDr49QUgrsBzXoUCBqpQ==",
				Path:     "gs://gcp-project/task/path/2021/11/17/21/16/20211117T2116-0015bf8f.json.gz",
				Created:  "2021-11-17T23:17:45Z",
			},
		},
		"stats": {
			Input: `{
  "linecnt": 21225,
  "bytecnt": 1972214,
  "size": 211710,
  "checksum": "e11ba5bc480441876a7aeb2b8d655fd4",
  "path": "s3://task/path/current.json.gz",
  "created": "2021-11-24T23:00:01Z"
}`,
			Expected: stat.Stats{
				LineCnt:  21225,
				ByteCnt:  1972214,
				Size:     211710,
				Checksum: "e11ba5bc480441876a7aeb2b8d655fd4",
				Path:     "s3://task/path/current.json.gz",
				Created:  "2021-11-24T23:00:01Z",
			},
		},
	}
	trial.New(fn, cases).Test(t)
}

func TestTaskMaster_MatchFile(t *testing.T) {
	tm := taskMaster{
		files: []fileRule{
			{
				SrcPattern:   "gs://*/*/*.txt",
				workflowFile: "basic.toml",
				Phase: workflow.Phase{
					Task: "basic",
				},
			},
			{
				SrcPattern:   "gs://*/*/data.txt",
				workflowFile: "data.toml",
				Phase: workflow.Phase{
					Task: "data",
					Rule: "job=1",
				},
			},
		},
	}

	fn := func(i trial.Input) (interface{}, error) {

		tm.producer, _ = nop.NewProducer("")
		mock := tm.producer.(*nop.Producer)
		err := tm.matchFile(i.Interface().(stat.Stats))
		msg := make([]task.Task, 0)
		for _, v := range mock.Messages {
			for _, s := range v {
				t, err := task.NewFromBytes([]byte(s))
				if err != nil {
					return nil, err
				}
				msg = append(msg, *t)
			}
		}

		sort.Slice(msg, func(i, j int) bool {
			return msg[i].Type < msg[j].Type
		})
		return msg, err
	}
	cases := trial.Cases{
		"match": {
			Input: stat.Stats{Path: "gs://bucket/path/file.txt"},
			Expected: []task.Task{
				{Type: "basic", Meta: "file=gs://bucket/path/file.txt&filename=file.txt&workflow=basic.toml"},
			},
		},
		"no match": {
			Input:       stat.Stats{Path: "/path/to/file.txt"},
			ExpectedErr: errors.New("no match found"),
		},
		"match 2": {
			Input: stat.Stats{Path: "gs://bucket/group/data.txt"},
			Expected: []task.Task{
				{Type: "basic", Meta: "file=gs://bucket/group/data.txt&filename=data.txt&workflow=basic.toml"},
				{Type: "data", Meta: "file=gs://bucket/group/data.txt&filename=data.txt&job=1&workflow=data.toml"},
			},
		},
	}

	trial.New(fn, cases).Comparer(
		trial.EqualOpt(
			trial.IgnoreAllUnexported,
			trial.IgnoreFields("ID", "Created"),
		)).Test(t)
}
