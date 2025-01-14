package bootstrap

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/davecgh/go-spew/spew"
	"github.com/hydronica/go-config"
	"github.com/hydronica/toml"
)

type Utility struct {
	name        string
	description string
	version     string
	options     Validator `options:"-"`
}

func NewUtility(name string, config Validator) *Utility {
	return &Utility{
		name:    name,
		options: config,
	}
}

func (u *Utility) Initialize() *Utility {
	var genConf bool
	var showConf bool
	flag.BoolVar(&genConf, "g", false, "generate options file")
	flag.BoolVar(&showConf, "show", false, "show current options values")
	config.New(u).
		Version(u.version).Disable(config.OptGenConf | config.OptShow).
		Description(u.description).
		LoadOrDie()

	if genConf {
		enc := toml.NewEncoder(os.Stdout)
		if err := enc.Encode(u.options); err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}
	if showConf {
		spew.Dump(u.options)
		os.Exit(0)
	}

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

func (u *Utility) Version(version string) *Utility {
	u.version = version
	return u
}

func (u *Utility) Description(description string) *Utility {
	u.description = description
	return u
}
