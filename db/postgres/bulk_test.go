package postgres

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/user"
	"testing"

	_ "github.com/lib/pq"
)

var pgConnStr = "user=%s password=%s host=%s dbname=%s sslmode=disable"

func TestMain(m *testing.M) {
	// postgres user (default is current user)
	usr, _ := user.Current()
	log.Println(usr.Username)

	// setup postgres test db
	pgDB, err := sql.Open("postgres", fmt.Sprintf(pgConnStr, usr.Username, "", "", "postgres"))
	if err != nil {
		log.Fatalln(err)
	}
	_, err = pgDB.Exec("CREATE DATABASE ci_test;")
	if err != nil {
		log.Fatalln(err)
	}

	// run tests
	code := m.Run()

	// remove postgres test db
	_, err = pgDB.Exec("DROP DATABASE ci_test;")
	if err != nil {
		log.Fatalln(err)
	}

	os.Exit(code)
}
