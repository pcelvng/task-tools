package batch

import (
	"fmt"
	"database/sql"

	_ "github.com/lib/pq"
)

func ExampleNewBatchLoader() {
	un := "root"
	pass := ""
	host := "127.0.0.1:5432"
	dbName := "ci-test"

	connStr := fmt.Sprintf("user=%s password=%s host=%s dbname=%s", un, pass, host, dbName)
	_, err := sql.Open("postgres", connStr)

	fmt.Println(err)

	// Output:
	// <nil>
}

