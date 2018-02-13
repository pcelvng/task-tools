package postgres

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

func ExampleGenInsert() {
	cols := []string{"col1", "col2"}
	numRows := 3
	tableName := "testTable"
	insQuery := GenInsert(cols, numRows, tableName)

	fmt.Println(insQuery)

	// Output:
	// INSERT INTO testTable (col1,col2) VALUES ($1,$2),($3,$4),($5,$6);
}
