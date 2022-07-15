package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hydronica/go-config"
	"github.com/pcelvng/task-tools/file"
	"github.com/pcelvng/task-tools/file/stat"
)

const usage = "Usage: mc <command> <Options>"

func main() {
	conf := file.Options{}
	config.LoadOrDie(&conf)

	args := getCMDArgs()
	if len(args) < 1 {
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
	case "cp":
		err = cp(f1, f2, &conf)
	case "rm":
		err = rm(f1, &conf)
	default:
		log.Fatalf("Unknown command %s\n%s", cmd, usage)
	}
	if err != nil {
		log.Fatalf("%s: %s", cmd, err)
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
	return fmt.Sprintf("%4s %v %v",
		fByte(sts.Size), t.Format("Jan 02 15:04"), sts.Path)

}
func fByte(i int64) string {
	count, prev := 0, i
	for ; i > 1024; count++ {
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

func cp(from, to string, opt *file.Options) error {
	return nil
}

func rm(path string, opt *file.Options) error {
	return nil
}
