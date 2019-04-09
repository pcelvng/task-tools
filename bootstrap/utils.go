package bootstrap

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"reflect"
	"strconv"

	btoml "gopkg.in/BurntSushi/toml.v0"
	ptoml "github.com/pelletier/go-toml"
)

type Utility struct {
	name        string
	description string
	version     string
	config      interface{}
}

func NewUtility(name string, config interface{}) *Utility {
	return &Utility{
		name:   name,
		config: config,
	}
}

func (u *Utility) Initialize() {
	setHelpOutput(u.name, u.description)
	u.checkFlags()
}

func (u *Utility) Version(version string) *Utility {
	u.version = version
	return u
}

func (u *Utility) Description(description string) *Utility {
	u.description = description
	return u
}

func (u *Utility) AddInfo(info func() interface{}, port int) *Utility {
	if port == 0 {
		log.Println("http status server has been disabled")
		return u
	}
	fn := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")

		b, err := json.MarshalIndent(info(), "", "  ")
		if b != nil && err == nil {
			// Replace the first { in the json string with the { + application name
			b = bytes.Replace(b, []byte(`{`), []byte(`{
"app_name":"`+u.name+`",`), 1)
		}
		w.Write(b)
	}

	log.Printf("starting http status server on port %d", port)

	http.HandleFunc("/", fn)
	go func() {
		err := http.ListenAndServe(":"+strconv.Itoa(port), nil)
		log.Fatal("http health service failed", err)
	}()
	return u
}

func setHelpOutput(name, description string) {
	// custom help screen
	flag.Usage = func() {
		if name != "" {
			fmt.Fprintln(os.Stderr, name)
			fmt.Fprintln(os.Stderr, "")
		}
		if description != "" {
			fmt.Fprintln(os.Stderr, description)
			fmt.Fprintln(os.Stderr, "")
		}
		fmt.Fprintln(os.Stderr, "Flag options:")
		flag.PrintDefaults()
	}
}

func (u *Utility) checkFlags() {
	if !flag.Parsed() {
		flag.Parse()
	}

	if *showVersion || *ver && u.version != "" {
		fmt.Println(u.version)
		os.Exit(0)
	}

	// gen config (sent to stdout)
	if *genConfig || *g {
		cfg := u.config
		if v := reflect.ValueOf(cfg); v.Kind() == reflect.Ptr {
			cfg = v.Elem().Interface()
		}
		b, _ := ptoml.Marshal(cfg)
		fmt.Println(string(b))
		os.Exit(0)
	}

	var path string
	// configPth required
	if *configPth == "" && *c == "" {
		log.Fatal("-config (-c) config file path required")
	} else if *configPth != "" {
		path = *configPth
	} else {
		path = *c
	}

	_, err := btoml.DecodeFile(path, u.config)
	if err != nil {
		log.Fatal("Error parsing config file", err)
	}

}
