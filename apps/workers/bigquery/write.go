package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/pcelvng/task-tools/file"
)

// bqValueLoader implements bigquery.ValueLoader and can be marshaled to JSON
type bqValueLoader struct {
	data   map[string]any
	Header []string
}

// Load implements bigquery.ValueLoader
func (b *bqValueLoader) Load(vs []bigquery.Value, schema bigquery.Schema) error {
	if b.data == nil {
		b.data = make(map[string]any)
	}

	for i, field := range schema {
		if i >= len(vs) {
			break
		}
		val := vs[i]
		b.data[field.Name] = convertBQValue(val)
	}
	return nil
}

// MarshalJSON implements json.Marshaler
func (b *bqValueLoader) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.data)
}

// ToCSV converts the loaded data to a CSV string based on the Header order
func (b *bqValueLoader) ToCSV() string {
	if len(b.Header) == 0 {
		return ""
	}

	values := make([]string, len(b.Header))
	for i, field := range b.Header {
		val := b.data[field]
		values[i] = convertToString(val)
	}
	return strings.Join(values, ",")
}

// convertToString converts a value to its string representation for CSV
func convertToString(v interface{}) string {
	if v == nil {
		return ""
	}

	switch x := v.(type) {
	case string:
		// Escape quotes and wrap in quotes if contains comma or quote
		escaped := strings.ReplaceAll(x, `"`, `""`)
		if strings.ContainsAny(x, ",\"") {
			return `"` + escaped + `"`
		}
		return escaped
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case []interface{}:
		// Convert array to JSON string and wrap in quotes
		b, _ := json.Marshal(x)
		return `"` + string(b) + `"`
	case map[string]interface{}:
		// Convert map to JSON string and wrap in quotes
		b, _ := json.Marshal(x)
		return `"` + string(b) + `"`
	default:
		// Convert any other type to string using standard conversion
		return fmt.Sprintf("%v", v)
	}
}

// convertBQValue converts BigQuery types to JSON-compatible types
func convertBQValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch x := v.(type) {
	case time.Time:
		return x.Format(time.RFC3339Nano)
	case []byte:
		return string(x)
	case *big.Rat:
		if x == nil {
			return nil
		}
		// Convert to float64 with high precision
		f, _ := x.Float64()
		return f
	case map[string]bigquery.Value:
		m := make(map[string]interface{})
		for k, v := range x {
			m[k] = convertBQValue(v)
		}
		return m
	case []bigquery.Value:
		arr := make([]interface{}, len(x))
		for i, v := range x {
			arr[i] = convertBQValue(v)
		}
		return arr
	default:
		return v
	}
}

func writeToFile(ctx context.Context, j *bigquery.Job, w file.Writer, format string) error {
	rows, err := j.Read(ctx)
	if err != nil {
		return fmt.Errorf("row: %w", err)
	}

	loader := &bqValueLoader{}

	// Set header for CSV format
	if format == "csv" {
		schema := rows.Schema
		header := make([]string, len(schema))
		for i, field := range schema {
			header[i] = field.Name
		}
		loader.Header = header

		// Write header row
		err = w.WriteLine([]byte(strings.Join(header, ",")))
		if err != nil {
			return fmt.Errorf("write header: %w", err)
		}
	}

	for err = rows.Next(loader); err == nil; err = rows.Next(loader) {
		var line []byte
		if format == "csv" {
			line = []byte(loader.ToCSV())
		} else {
			line, _ = json.Marshal(loader)
		}

		err = w.WriteLine(line)
		if err != nil {
			return fmt.Errorf("write: %w", err)
		}
	}
	return w.Close()
}
