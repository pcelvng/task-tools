package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pcelvng/task"
	"github.com/pcelvng/task-tools/file/stat"
	"github.com/pcelvng/task-tools/tmpl"
	"github.com/pcelvng/task-tools/workflow"
)

// gcp_files is a struct populated by https://cloud.google.com/storage/docs/json_api/v1/objects#resource

// GCSEvent is the payload of a GCS event.
type gcsEvent struct {
	Kind           string    `json:"kind"`
	ID             string    `json:"id"`
	SelfLink       string    `json:"selfLink"`
	Path           string    `json:"name"`
	Bucket         string    `json:"bucket"`
	Generation     string    `json:"generation"`
	Metageneration string    `json:"metageneration"`
	ContentType    string    `json:"contentType"`
	TimeCreated    time.Time `json:"timeCreated"`
	Updated        time.Time `json:"updated"`
	TemporaryHold  bool      `json:"temporaryHold"`
	//EventBasedHold          bool                   `json:"eventBasedHold"`
	//RetentionExpirationTime time.Time              `json:"retentionExpirationTime"`
	StorageClass            string    `json:"storageClass"`
	TimeStorageClassUpdated time.Time `json:"timeStorageClassUpdated"`
	Size                    string    `json:"size"`
	MD5Hash                 string    `json:"md5Hash"`
	MediaLink               string    `json:"mediaLink"`
	//ContentEncoding         string                 `json:"contentEncoding"`
	//ContentDisposition      string                 `json:"contentDisposition"`
	//CacheControl            string                 `json:"cacheControl"`
	//Metadata                map[string]interface{} `json:"metadata"`
	CRC32C string `json:"crc32c"`
	//ComponentCount          int                    `json:"componentCount"`
	Etag string `json:"etag"`
	/*CustomerEncryption      struct {
		EncryptionAlgorithm string `json:"encryptionAlgorithm"`
		KeySha256           string `json:"keySha256"`
	}
	KMSKeyName    string `json:"kmsKeyName"`
	ResourceState string `json:"resourceState"`*/
}

type fileRule struct {
	SrcPattern string `uri:"file"` // source file path pattern to match (supports glob style matching)

	workflowFile string

	workflow.Phase

	// checks for rules that checks on groups of files instead of responding
	// immediately to an individual file.
	CronCheck  string `uri:"cron"`  // optional cron parsable string representing when to check src pattern matching files
	CountCheck int    `uri:"count"` // optional int representing how many files matching that rule to wait for until the rule is exercised
}

func (e gcsEvent) Stat() stat.Stats {
	size, _ := strconv.Atoi(e.Size)
	return stat.Stats{
		LineCnt:  0,
		ByteCnt:  0,
		Size:     int64(size),
		Checksum: e.MD5Hash,
		Path:     "gs://" + e.Bucket + "/" + e.Path,
		Created:  e.TimeCreated.In(time.UTC).Format(time.RFC3339),
		IsDir:    false,
		Files:    0,
	}
}

func unmarshalStat(b []byte) (sts stat.Stats) {
	e := gcsEvent{}
	json.Unmarshal(b, &e)
	if e.ID != "" && e.Path != "" { // this is a valid gcsEvent
		return e.Stat()
	}

	json.Unmarshal(b, &sts)
	return sts
}

// matchFile checks the sts.Path with all file Rules that are registered with flowlord
// if a match is found it will create a task and send it out
func (tm *taskMaster) matchFile(sts stat.Stats) error {
	matches := 0
	for _, f := range tm.files {
		if isMatch, _ := filepath.Match(f.SrcPattern, sts.Path); !isMatch {
			continue
		}
		matches++

		// setup task
		t := tmpl.PathTime(sts.Path) // get time from path
		// setup custom files values from rules
		meta, _ := url.ParseQuery(f.Rule)
		meta.Set("file", sts.Path)
		meta.Set("filename", filepath.Base(sts.Path))
		meta.Set("workflow", f.workflowFile)
		// todo: add job if provided in task name ex -> task:job

		// populate the info string
		info := tmpl.Parse(f.Template, t)
		info = tmpl.Meta(info, meta)

		tsk := task.New(f.Topic(), info)
		tsk.Meta, _ = url.QueryUnescape(meta.Encode())

		if err := tm.producer.Send(tsk.Type, tsk.JSONBytes()); err != nil {
			return err
		}
	}
	if matches == 0 {
		return fmt.Errorf("no match found for %q", sts.Path)
	}
	return nil

}

/*
func (tm *taskMaster) match(sts *stat.Stats, rule *Rule) {
	if isMatch, _ := filepath.Match(rule.SrcPattern, sts.Path); !isMatch {
		return
	}

	// goes to a rule bucket?
	if rule.CountCheck > 0 || rule.CronCheck != "" {
		tm.addSts(rule, sts)

		// count check - send tsk if count is full
		if rule.CountCheck > 0 {
			tm.countCheck(rule)
		}
	} else {
		// does not go to a rule bucket so
		// create task and send immediately
		info := genInfo(rule.InfoTemplate, sts)
		tsk := task.New(rule.TaskType, info)
		tm.sendTsk(tsk, rule)
	}
}
*/
