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
