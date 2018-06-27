package bootstrap

import (
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"

	btoml "gopkg.in/BurntSushi/toml.v0"
	ptoml "gopkg.in/pelletier/go-toml.v1"
)

type Helper struct {
	name        string
	description string
	version     string
	config      interface{}
}

func NewHelper(name string, config interface{}) *Helper {
	return &Helper{
		name:   name,
		config: config,
	}
}

func (h *Helper) Initialize() {
	setHelpOutput(h.name, h.description)
	h.checkFlags()
}

func (h *Helper) Version(version string) *Helper {
	h.version = version
	return h
}

func (h *Helper) Description(description string) *Helper {
	h.description = description
	return h
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

func (h *Helper) checkFlags() {
	if !flag.Parsed() {
		flag.Parse()
	}

	if *showVersion || *ver && h.version != "" {
		fmt.Println(h.version)
		os.Exit(0)
	}

	// gen config (sent to stdout)
	if *genConfig || *g {
		cfg := h.config
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

	_, err := btoml.DecodeFile(path, h.config)
	if err != nil {
		log.Fatal("Error parsing config file", err)
	}

}
