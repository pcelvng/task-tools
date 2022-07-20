package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hydronica/go-config"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/stat"
)

const usage = `Usage: fz [command [opts ...] args ...]
Available commands
  ls  <path>        List files and directories within a path
  cat <path>        Concat file content to stdout
  cp  <from> <to>   Copy a file from a location to another`

func main() {
	conf := file.Options{}
	config.New(&conf).Description(usage).LoadOrDie()

	args := getCMDArgs()
	if len(args) <= 1 {
		log.Fatal(usage)
	}
	cmd, f1 := args[0], args[1]
	var f2 string
	if len(args) > 2 {
		f2 = args[2]
	}
	var err error
	switch strings.ToLower(cmd) {
	case "ls":
		err = ls(f1, &conf)
	case "cat":
		err = cat(f1, &conf)
	case "cp":
		err = cp(f1, f2, &conf)
	default:
		log.Fatalf("Unknown command %s\n%s", cmd, usage)
	}
	if err != nil {
		log.Fatalf("%s:\t%s", cmd, err)
	}
}

// getCMDArgs returns just the needed file commands
// by removing the flag variables and file name from the os args slice
func getCMDArgs() (args []string) {
	var skip bool
	for _, v := range os.Args[1:] {
		if skip {
			skip = false
			continue
		}
		// skip flag vars
		if v[0] == '-' {
			if !strings.Contains(v, "=") {
				// skip the set variable with the flag
				skip = true
			}
			continue
		}
		args = append(args, v)
	}
	return args
}

func format(sts stat.Stats) string {
	t, _ := time.Parse(time.RFC3339, sts.Created)
	if sts.IsDir {
		sts.Size = 4096
	}
	return fmt.Sprintf("%4s %s %v",
		fByte(sts.Size), t.Format("Jan 02 15:04"), sts.Path)

}
func fByte(i int64) string {
	count, prev := 0, i
	for ; i > 4096; count++ {
		prev = i
		i >>= 10
	}
	v := fmt.Sprintf("%.1f", float64(prev)/1024.0)
	switch count {
	case 0:
		return strconv.FormatInt(i, 10)
	case 1:
		return v + "K"
	case 2:
		return v + "M"
	case 3:
		return v + "G"
	case 4:
		return v + "T"
	default:
		return v + "u" + strconv.Itoa(count)
	}
}

func ls(path string, opt *file.Options) error {
	sts, err := file.Stat(path, opt)
	if err != nil {
		return err
	}
	if sts.IsDir {
		sts, err := file.List(path, opt)
		if err != nil {
			return err
		}
		for _, s := range sts {
			fmt.Println(format(s))
		}
		return nil
	}
	fmt.Println(format(sts))
	return nil
}

func cat(path string, opt *file.Options) error {
	r, err := file.NewReader(path, opt)
	if err != nil {
		return err
	}
	s := file.NewScanner(r)
	for s.Scan() {
		fmt.Println(s.Text())
	}

	return nil
}

func cp(from, to string, opt *file.Options) error {
	if to == "" || from == "" {
		return fmt.Errorf(usage)
	}
	sts, _ := file.Stat(to, opt)
	if sts.IsDir {
		_, fName := filepath.Split(from)
		to = strings.TrimRight(to, "/") + "/" + fName
	}
	r, err := file.NewReader(from, opt)
	if err != nil {
		return fmt.Errorf("reader init for %s %w", from, err)
	}
	w, err := file.NewWriter(to, opt)
	if err != nil {
		return fmt.Errorf("writer init for %s %w", to, err)
	}

	s := file.NewScanner(r)
	for s.Scan() {
		if err := w.WriteLine(s.Bytes()); err != nil {
			w.Abort()
			return fmt.Errorf("write error: %w", err)
		}
	}
	return w.Close()
}
