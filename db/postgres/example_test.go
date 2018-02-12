package postgres

import "fmt"

func ExampleGenInsert() {
	cols := []string{"col1", "col2"}
	numRows := 3
	tableName := "testTable"
	insQuery := GenInsert(cols, numRows, tableName)

	fmt.Println(insQuery)

	// Output:
	// INSERT INTO testTable (col1,col2) VALUES ($1,$2),($3,$4),($5,$6);
}

