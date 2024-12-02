package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/dustin/go-humanize"
	"github.com/jbsmith7741/uri"
	"github.com/pcelvng/task"
	"google.golang.org/api/option"

	"github.com/pcelvng/task-tools/file"
)

type worker struct {
	task.Meta
	options

	DestTable Destination `uri:"dest_table"` // BQ table to load data into

	File      string            `uri:"origin" required:"true"`      // if not GCS ref must be file, can be folder (for GCS)
	FromGCS   bool              `uri:"direct_load" default:"false"` // load directly from GCS ref, can use wildcards *
	Truncate  bool              `uri:"truncate"`                    //remove all data in table before insert
	DeleteMap map[string]string `uri:"delete"`                      // map of fields with value to check and delete

	// Read options
	Interactive bool   `uri:"interactive"` // makes queries run faster for local development
	DestPath    string `uri:"dest_path"`

	writeToFile bool
	delete      bool
}

// TODO: Add bq read support
// run a query that can export data into another table
// Run a query that can export data into a local file
// run a query that Bigquery will export to GCS
// Allow using Query Params. Prefer BQ native

func (o *options) NewWorker(info string) task.Worker {
	w := &worker{
		Meta:    task.NewMeta(),
		options: *o,
	}
	err := uri.Unmarshal(info, w)
	if err != nil {
		return task.InvalidWorker(err.Error())
	}

	// verify options
	w.delete = len(w.DeleteMap) > 0
	if w.delete && w.Truncate {
		return task.InvalidWorker("truncate and delete options must be selected independently")
	}

	//TODO: verify destination is required for all loading, but only for reading in the dest_path is empty
	return w
}

// ExportToFile will add meta data to the end of the query that will cause the results query to be exported to a file
func prepQuery(query []byte, destPath string, format bigquery.DataFormat) string {
	var w bytes.Buffer
	w.WriteString("EXPORT DATA OPTIONS(\n")
	w.WriteString("overwrite=true,\n")
	switch format {
	case bigquery.JSON:
		w.WriteString("format=JSON,\n")
	case bigquery.CSV:
		w.WriteString("format=CSV,\n")
	}
	w.WriteString("uri='" + destPath + "') AS \n")
	w.Write(query)
	return w.String()
}

func (w *worker) DoTask(ctx context.Context) (task.Result, string) {
	opts := make([]option.ClientOption, 0)
	if w.BqAuth != "" {
		opts = append(opts, option.WithCredentialsFile(w.BqAuth))
	}
	client, err := bigquery.NewClient(ctx, w.Project, opts...)
	defer client.Close()
	if err != nil {
		return task.Failf("bigquery client init %s", err)
	}

	var format bigquery.DataFormat
	switch filepath.Ext(w.File) {
	case ".sql":
		f, err := file.NewReader(w.File, &w.Fopts)
		if err != nil {
			return task.Failf("read error: %v %v", w.File, err)
		}
		b, _ := io.ReadAll(f)
		query := string(b)
		if isGCSExport(w.DestPath) {
			query = prepQuery(b, w.DestPath, bigquery.CSV)
		} else if w.DestPath != "" {
			w.writeToFile = true
		}
		return w.Query(ctx, client, query)
	case ".json":
		format = bigquery.JSON
		return w.Load(ctx, client, format)
	case ".csv":
		format = bigquery.CSV
		return w.Load(ctx, client, format)
	}

	return task.Failf("unsupported file format %v, expected:sql|json|csv", filepath.Ext(w.File))
}

func (w *worker) Query(ctx context.Context, client *bigquery.Client, query string) (task.Result, string) {
	bq := client.Query(query)
	// TODO: add query params with @name

	if w.Interactive {
		bq.Priority = bigquery.InteractivePriority
	} else {
		bq.Priority = bigquery.BatchPriority
	}

	if w.DestTable.String() != "" && w.DestPath == "" {
		// project is defined in the client
		bq.Dst = w.DestTable.BqTable(client)
		bq.WriteDisposition = bigquery.WriteAppend
	}
	job, err := bq.Run(ctx)
	if err != nil {
		return task.Failf("bq build: %v", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return task.Failf("wait: %v", err)
	}

	if bqStats, ok := status.Statistics.Details.(*bigquery.QueryStatistics); ok {

		w.SetMeta("bq_bytes_billed", strconv.FormatInt(bqStats.TotalBytesBilled, 10))
		w.SetMeta("bq_query_time", status.Statistics.EndTime.Sub(status.Statistics.StartTime).String())
	}

	// process query and save to file
	if w.writeToFile {
		writer, err := file.NewWriter(w.DestPath, &w.Fopts)
		if err != nil {
			return task.Failf("write to %v: %v", w.DestPath, err)
		}
		format := strings.Trim(filepath.Ext(w.DestPath), ".")
		if err := writeToFile(ctx, job, writer, format); err != nil {
			return task.Failed(err)
		}
	}
	return task.Completed("BQ byte processed: %v", humanize.Bytes(uint64(status.Statistics.TotalBytesProcessed)))
}

func (w *worker) Load(ctx context.Context, client *bigquery.Client, format bigquery.DataFormat) (task.Result, string) {
	var loader *bigquery.Loader
	if w.FromGCS { // load from Google Cloud Storage
		gcsRef := bigquery.NewGCSReference(w.File)
		gcsRef.SourceFormat = format
		loader = w.DestTable.BqTable(client).LoaderFrom(gcsRef)
	} else { // load from file reader
		r, err := file.NewReader(w.File, &w.Fopts)
		if err != nil {
			return task.Failf("problem with file: %s", err)
		}
		bqRef := bigquery.NewReaderSource(r)
		bqRef.SourceFormat = format
		bqRef.MaxBadRecords = 1
		loader = w.DestTable.BqTable(client).LoaderFrom(bqRef)
	}

	loader.WriteDisposition = bigquery.WriteAppend
	if len(w.DeleteMap) > 0 {
		q := delStatement(w.DeleteMap, w.DestTable)
		j, err := client.Query(q).Run(ctx)
		if err != nil {
			return task.Failf("delete statement: %s", err)
		}
		status, err := j.Wait(ctx)
		if err != nil {
			return task.Failf("delete wait: %s", err)
		}
		if status.Err() != nil {
			return task.Failf("delete: %s", err)
		}
		status = j.LastStatus()
		if qSts, ok := status.Statistics.Details.(*bigquery.QueryStatistics); ok {
			w.SetMeta("rows_del", strconv.FormatInt(qSts.NumDMLAffectedRows, 10))
		}
	}

	if w.Truncate {
		loader.WriteDisposition = bigquery.WriteTruncate
	}

	job, err := loader.Run(ctx)
	if err != nil {
		return task.Failf("loader run: %s", err)
	}
	status, err := job.Wait(ctx)
	if err == nil {
		if status.Err() != nil {
			return task.Failf("job completed with error: %v", status.Errors)
		}
		if sts, ok := status.Statistics.Details.(*bigquery.LoadStatistics); ok {
			w.SetMeta("rows_insert", strconv.FormatInt(sts.OutputRows, 10))
			return task.Completed("%d rows (%s) loaded", sts.OutputRows, humanize.Bytes(uint64(sts.OutputBytes)))
		}
	}

	return task.Completed("completed")
}

func delStatement(m map[string]string, d Destination) string {
	s := make([]string, 0)
	for k, v := range m {
		s = append(s, k+" = "+v)
	}
	sort.Strings(s)
	return fmt.Sprintf("delete from `%s` where %s", d.String(), strings.Join(s, " and "))
}

// isGCSExport checks in the url path will work for exporting to GCS
// - starts with gs://
// - must contain a single asterisk * in the filename
func isGCSExport(s string) bool {
	if !strings.HasPrefix(s, "gs://") {
		return false
	}
	d, f := filepath.Split(s)
	return !strings.Contains(d, "*") && strings.Count(f, "*") == 1
}
