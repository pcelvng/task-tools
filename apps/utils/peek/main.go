package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/pcelvng/task-tools"
	"github.com/pcelvng/task-tools/bootstrap"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/timeframe"
	"github.com/pcelvng/task-tools/tmpl"
)

var (
	from = flag.String("from", "now", "format 'yyyy-mm-ddThh' (example: '2017-01-03T01'). Allows a special keyword 'now'.")
	to   = flag.String("to", "", "same format as 'from'; if not specified, will run the one hour specified by from. Allows special keyword 'now'.")
)

const (
	name = "peek"
	desc = ""
)

type config struct {
	File *file.Options `toml:"file"`
	Path string
}

func main() {
	c := &config{
		File: file.NewOptions(),
		Path: "s3://path/done/{yyyy}/{mm}/{dd}/{hh}.gz",
	}
	bootstrap.NewUtility(name, c).
		Version(tools.String()).Description(desc).Initialize()
	start, err := timeframe.NewHour(*from)
	if err != nil {
		log.Fatalf("from %s not valid format (2006-01-02T15)", *from)
	}
	end, err := timeframe.NewHour(*to)
	if err != nil {
		log.Fatalf("to %s not valid format (2006-01-02T15)", *to)
	}
	tf := timeframe.TimeFrame{Start: start, End: end}

	for _, t := range tf.Generate() {
		s := tmpl.Parse(c.Path, t)
		sts, err := file.Glob(s, c.File)
		if err != nil {
			log.Println(err)
			continue
		}
		for _, f := range sts {
			reader, err := file.NewReader(f.Path, c.File)
			if err != nil {
				log.Printf("reader init: %s %s", s, err)
				continue
			}
			scanner := file.NewScanner(reader)
			for scanner.Scan() {
				fmt.Println(scanner.Text())
			}
			reader.Close()
		}
	}
}
