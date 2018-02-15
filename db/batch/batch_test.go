package batch

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"os/user"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var (
	// postgres
	pgConn, pgDB *sql.DB
	pgConnStr    = "user=%s password=%s host=%s dbname=%s sslmode=disable"

	// mysql
	msqlConn, msqlDB *sql.DB
	msqlConnStr      = "%s:%s@tcp(%s)/%s?parseTime=true"
)

func TestBatchLoader_Delete(t *testing.T) {
	bl := NewBatchLoader("postgres", pgConn)

	bl.Delete("delete query", []interface{}{"one", "two"}...)

	got := bl.delQuery
	expected := "delete query"
	if got != expected {
		t.Errorf("got %v but expected %v", got, expected)
	}

	gotV := len(bl.delVals)
	expectedV := 2
	if gotV != expectedV {
		t.Errorf("got %v but expected %v", gotV, expectedV)
	}
}

func TestBatchLoader_AddRow(t *testing.T) {
	bl := NewBatchLoader("postgres", pgConn)

	bl.AddRow([]interface{}{"one", "two", "three"})
	bl.AddRow([]interface{}{"one", "two", "three"})
	bl.AddRow([]interface{}{"one", "two", "three"})

	got := len(bl.fRows)
	expected := 9
	if got != expected {
		t.Errorf("got %v but expected %v", got, expected)
	}
}

func TestBatchLoader_Commit(t *testing.T) {
	createTable(tableTestCommit)
	defer dropTable("test.test_commit")

	bl := NewBatchLoader("postgres", pgConn)

	dt := time.Date(2017, 02, 03, 04, 05, 06, 0, time.UTC)
	bl.fRows = []interface{}{"one", 2, dt}

	sts, err := bl.Commit(context.Background(), "test.test_commit", []string{"f1", "f2", "f3"}...)
	if err != nil {
		t.Fatal(err)
	}

	if sts.Started == "" {
		t.Error("expected value for sts.Started")
	}
}

func TestBatchLoader_MySQL(t *testing.T) {
	createTableMySQL(tableTestMySQL)
	defer dropTableMySQL("test_mysql")

	bl := NewBatchLoader("mysql", msqlConn)

	dt := time.Date(2017, 02, 03, 04, 05, 06, 0, time.UTC)
	bl.fRows = []interface{}{"one", 2, dt}

	sts, err := bl.Commit(context.Background(), "test_mysql", []string{"f1", "f2", "f3"}...)
	if err != nil {
		t.Fatal(err)
	}

	if sts.Started == "" {
		t.Error("expected value for sts.Started")
	}
}

func TestBatchLoader_DeleteCommit(t *testing.T) {
	createTable(tableTestCommit)
	defer dropTable("test.test_commit")

	bl := NewBatchLoader("postgres", pgConn)

	dt1 := time.Date(2017, 02, 03, 04, 05, 06, 0, time.UTC)
	dt2 := time.Date(2017, 03, 03, 04, 05, 06, 0, time.UTC)
	dt3 := time.Date(2017, 04, 03, 04, 05, 06, 0, time.UTC)
	bl.AddRow([]interface{}{"one", 2, dt1})
	bl.AddRow([]interface{}{"two", 3, dt2})
	bl.AddRow([]interface{}{"three", 4, dt3})
	cols := []string{"f1", "f2", "f3"}

	// first commit
	bl.Commit(context.Background(), "test.test_commit", cols...)

	// now add delete query
	bl.Delete(`DELETE FROM test.test_commit WHERE f3 >= $1`, dt3)
	sts, err := bl.Commit(context.Background(), "test.test_commit", cols...)
	if err != nil {
		t.Error(err)
	}

	if sts.Removed != 1 {
		t.Errorf("got %v but expected %v\n", sts.Removed, 1)
	}
}

func TestBatchLoader_MultipleBatches(t *testing.T) {
	// Test multiple batches where the first batches and
	// the last batch have a different number of rows.

	createTable(tableTestCommit)
	defer dropTable("test.test_commit")

	maxBatchSize = 2
	bl := NewBatchLoader("postgres", pgConn)

	dt1 := time.Date(2017, 02, 03, 04, 05, 06, 0, time.UTC)
	dt2 := time.Date(2017, 03, 03, 04, 05, 06, 0, time.UTC)
	dt3 := time.Date(2017, 04, 03, 04, 05, 06, 0, time.UTC)
	// three batches: 2 rows, 2 rows and 1 row
	bl.AddRow([]interface{}{"one", 2, dt1})
	bl.AddRow([]interface{}{"two", 3, dt2})
	bl.AddRow([]interface{}{"three", 4, dt3})
	bl.AddRow([]interface{}{"one", 2, dt1})
	bl.AddRow([]interface{}{"two", 3, dt2})

	cols := []string{"f1", "f2", "f3"}
	sts, err := bl.Commit(context.Background(), "test.test_commit", cols...)
	if err != nil {
		t.Error(err)
	}

	if sts.Inserted != 5 {
		t.Errorf("got %v but expected %v\n", sts.Inserted, 5)
	}

	if sts.Rows != 5 {
		t.Errorf("got %v but expected %v\n", sts.Rows, 5)
	}

	// reset maxBatchSize
	maxBatchSize = 200
}

func TestBatchLoader_MultipleSameBatches(t *testing.T) {
	// Test multiple batches where all batches have the same
	// number of rows.

	createTable(tableTestCommit)
	defer dropTable("test.test_commit")

	maxBatchSize = 2
	bl := NewBatchLoader("postgres", pgConn)

	dt1 := time.Date(2017, 02, 03, 04, 05, 06, 0, time.UTC)
	dt2 := time.Date(2017, 03, 03, 04, 05, 06, 0, time.UTC)
	dt3 := time.Date(2017, 04, 03, 04, 05, 06, 0, time.UTC)
	// three batches: 2 rows, 2 rows and 1 row
	bl.AddRow([]interface{}{"one", 2, dt1})
	bl.AddRow([]interface{}{"two", 3, dt2})
	bl.AddRow([]interface{}{"three", 4, dt3})
	bl.AddRow([]interface{}{"one", 2, dt1})
	bl.AddRow([]interface{}{"two", 3, dt2})
	bl.AddRow([]interface{}{"three", 4, dt3})

	cols := []string{"f1", "f2", "f3"}
	sts, err := bl.Commit(context.Background(), "test.test_commit", cols...)
	if err != nil {
		t.Error(err)
	}

	if sts.Inserted != 6 {
		t.Errorf("got %v but expected %v\n", sts.Inserted, 6)
	}

	if sts.Rows != 6 {
		t.Errorf("got %v but expected %v\n", sts.Rows, 6)
	}

	// reset maxBatchSize
	maxBatchSize = 200
}

func TestBatchLoader_CommitNoCols(t *testing.T) {
	// Test that num of cols is validated. Must be greater
	// than zero.

	bl := NewBatchLoader("postgres", pgConn)

	dt1 := time.Date(2017, 02, 03, 04, 05, 06, 0, time.UTC)
	bl.AddRow([]interface{}{"one", 2, dt1})

	cols := make([]string, 0)
	_, err := bl.Commit(context.Background(), "test.test_commit", cols...)
	expected := "columns not provided"
	if err == nil {
		t.Errorf("expected %v got %v", expected, err)
	}
}

func TestBatchLoader_DoTx(t *testing.T) {
	createTable(tableTestCommit)
	defer dropTable("test.test_commit")

	type scenario struct {
		bl        *BatchLoader
		ctx       context.Context
		nRows     int
		nBatches  int
		bSize     int
		lbSize    int
		tableName string
		err       error
	}
	ctx, _ := context.WithCancel(context.Background())
	ctxCncld, cncl := context.WithCancel(ctx)
	cncl() // call cancel

	bl1 := NewBatchLoader("postgres", pgConn) // vanilla

	bl2 := NewBatchLoader("postgres", pgConn)
	bl2.cols = []string{"f2"}            // with cols
	bl2.fRows = []interface{}{"badval1"} // one row

	bl3 := NewBatchLoader("postgres", pgConn) // bad del query
	bl3.Delete("bad")

	scenarios := []scenario{
		{bl1, ctxCncld, 0, 0, 0, 0, "test.table", errors.New("context canceled")},
		{bl2, ctx, 1, 0, 1, 1, "test.does_not_exist", errors.New(`pq: relation "test.does_not_exist" does not exist`)},
		{bl3, ctx, 1, 0, 1, 2, "test.test_commit", errors.New(`pq: syntax error at or near "bad"`)},
		{bl2, ctx, 1, 1, 1, 1, "test.test_commit", errors.New(`pq: invalid input syntax for integer: "badval1"`)},
	}

	for i, s := range scenarios {
		_, err := s.bl.doTx(s.ctx, s.nRows, s.nBatches, s.bSize, s.lbSize, s.tableName)
		if err == nil {
			err = errors.New("") // so no nil panic
		}

		if s.err == nil {
			s.err = errors.New("")
		}

		if s.err.Error() != err.Error() {
			t.Errorf("for scenario[%v] expected '%v' but got '%v'\n", i, s.err, err)
		}
	}
}

func TestNumBatches(t *testing.T) {
	type scenario struct {
		maxBatchSize     int
		numRows          int
		expBatches       int
		expBatchSize     int
		expLastBatchSize int
	}

	scenarios := []scenario{
		{0, 0, 0, 0, 0},
		{0, 1, 0, 0, 0},
		{1, 1, 1, 1, 1},
		{2, 1, 1, 1, 1},
		{3, 2, 1, 2, 2},
		{3, 3, 1, 3, 3},
		{3, 6, 2, 3, 3},
		{3, 7, 3, 3, 1},
	}

	for _, s := range scenarios {
		bs, bSize, lbSize := numBatches(s.maxBatchSize, s.numRows)

		if bs != s.expBatches {
			t.Errorf("for maxBatchSize %v and numRows %v expected batches of %v but got %v\n", s.maxBatchSize, s.numRows, s.expBatches, bs)
		}

		if bSize != s.expBatchSize {
			t.Errorf("for maxBatchSize %v and numRows %v expected batchSize of %v but got %v\n", s.maxBatchSize, s.numRows, s.expBatchSize, bSize)
		}

		if lbSize != s.expLastBatchSize {
			t.Errorf("for maxBatchSize %v and numRows %v expected lastBatchSize of %v but got %v\n", s.maxBatchSize, s.numRows, s.expLastBatchSize, lbSize)
		}
	}
}

func BenchmarkBatchLoader_CommitNoIndexSmallTable(b *testing.B) {
	// Benchmark a small table with no extra indexes
	// with batch size 200 and 10,000 rows per commit.
	// Not deleting in this benchmark.

	createTable(tableTestCommit)
	defer dropTable("test.test_commit")

	maxBatchSize = 200
	bl := NewBatchLoader("postgres", pgConn)

	dt1 := time.Date(2017, 02, 03, 04, 05, 06, 0, time.UTC)
	dt2 := time.Date(2017, 03, 03, 04, 05, 06, 0, time.UTC)
	dt3 := time.Date(2017, 04, 03, 04, 05, 06, 0, time.UTC)

	// prep with 10,000 rows
	for i := 0; i < 2000; i++ {
		bl.AddRow([]interface{}{"one", 2, dt1})
		bl.AddRow([]interface{}{"two", 3, dt2})
		bl.AddRow([]interface{}{"three", 4, dt3})
		bl.AddRow([]interface{}{"four", 5, dt3})
		bl.AddRow([]interface{}{"five", 6, dt3})
	}
	cols := []string{"f1", "f2", "f3"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bl.Commit(context.Background(), "test.test_commit", cols...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchLoader_Commit1IndexSmallTable(b *testing.B) {
	// Benchmark a small table with one index on the created field
	// with batch size 200 and 10,000 rows per commit.
	// Not deleting in this benchmark.

	createTable(tableTestCommit)
	createTable(tableIdxCreated)
	defer dropTable("test.test_commit")

	maxBatchSize = 200
	bl := NewBatchLoader("postgres", pgConn)

	dt1 := time.Date(2017, 02, 03, 04, 05, 06, 0, time.UTC)
	dt2 := time.Date(2017, 03, 03, 04, 05, 06, 0, time.UTC)
	dt3 := time.Date(2017, 04, 03, 04, 05, 06, 0, time.UTC)

	// prep with 10,000 rows
	for i := 0; i < 2000; i++ {
		bl.AddRow([]interface{}{"one", 2, dt1})
		bl.AddRow([]interface{}{"two", 3, dt2})
		bl.AddRow([]interface{}{"three", 4, dt3})
		bl.AddRow([]interface{}{"four", 5, dt3})
		bl.AddRow([]interface{}{"five", 6, dt3})
	}
	cols := []string{"f1", "f2", "f3"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bl.Commit(context.Background(), "test.test_commit", cols...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchLoader_Commit2IndexSmallTable(b *testing.B) {
	// Benchmark a small table with one index on the created field
	// with batch size 200 and 10,000 rows per commit.
	// Not deleting in this benchmark.

	createTable(tableTestCommit)
	createTable(tableIdxCreated)
	createTable(tableIdxF3)
	defer dropTable("test.test_commit")

	maxBatchSize = 200
	bl := NewBatchLoader("postgres", pgConn)

	dt1 := time.Date(2017, 02, 03, 04, 05, 06, 0, time.UTC)
	dt2 := time.Date(2017, 03, 03, 04, 05, 06, 0, time.UTC)
	dt3 := time.Date(2017, 04, 03, 04, 05, 06, 0, time.UTC)

	// prep with 10,000 rows
	for i := 0; i < 2000; i++ {
		bl.AddRow([]interface{}{"one", 2, dt1})
		bl.AddRow([]interface{}{"two", 3, dt2})
		bl.AddRow([]interface{}{"three", 4, dt3})
		bl.AddRow([]interface{}{"four", 5, dt3})
		bl.AddRow([]interface{}{"five", 6, dt3})
	}
	cols := []string{"f1", "f2", "f3"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bl.Commit(context.Background(), "test.test_commit", cols...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchLoader_CommitNoIndexSmallTableWithDeletes(b *testing.B) {
	// Benchmark a small table with no extra indexes
	// with batch size 200 and 10,000 rows per commit.

	createTable(tableTestCommit)
	defer dropTable("test.test_commit")

	maxBatchSize = 200
	bl := NewBatchLoader("postgres", pgConn)

	dt1 := time.Date(2017, 02, 03, 04, 05, 06, 0, time.UTC)
	dt2 := time.Date(2017, 03, 03, 04, 05, 06, 0, time.UTC)
	dt3 := time.Date(2017, 04, 03, 04, 05, 06, 0, time.UTC)

	// prep with 10,000 rows
	for i := 0; i < 2000; i++ {
		bl.AddRow([]interface{}{"one", 2, dt1})
		bl.AddRow([]interface{}{"two", 3, dt2})
		bl.AddRow([]interface{}{"three", 4, dt3})
		bl.AddRow([]interface{}{"four", 5, dt3})
		bl.AddRow([]interface{}{"five", 6, dt3})
	}
	cols := []string{"f1", "f2", "f3"}
	bl.Delete(`DELETE FROM test.test_commit WHERE f3 >= $1`, dt3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bl.Commit(context.Background(), "test.test_commit", cols...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchLoader_Commit1IndexSmallTableWithDeletesOnIndex(b *testing.B) {
	// Benchmark a small table with no extra indexes
	// with batch size 200 and 10,000 rows per commit.

	createTable(tableTestCommit)
	createTable(tableIdxF3)
	defer dropTable("test.test_commit")

	maxBatchSize = 200
	bl := NewBatchLoader("postgres", pgConn)

	dt1 := time.Date(2017, 02, 03, 04, 05, 06, 0, time.UTC)
	dt2 := time.Date(2017, 03, 03, 04, 05, 06, 0, time.UTC)
	dt3 := time.Date(2017, 04, 03, 04, 05, 06, 0, time.UTC)

	// prep with 10,000 rows
	for i := 0; i < 2000; i++ {
		bl.AddRow([]interface{}{"one", 2, dt1})
		bl.AddRow([]interface{}{"two", 3, dt2})
		bl.AddRow([]interface{}{"three", 4, dt3})
		bl.AddRow([]interface{}{"four", 5, dt3})
		bl.AddRow([]interface{}{"five", 6, dt3})
	}
	cols := []string{"f1", "f2", "f3"}
	bl.Delete(`DELETE FROM test.test_commit WHERE f3 >= $1`, dt3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bl.Commit(context.Background(), "test.test_commit", cols...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchLoader_Commit1IndexSmallTableWithDeletesOffIndex(b *testing.B) {
	// Benchmark a small table with no extra indexes
	// with batch size 200 and 10,000 rows per commit.

	createTable(tableTestCommit)
	createTable(tableIdxCreated)
	defer dropTable("test.test_commit")

	maxBatchSize = 200
	bl := NewBatchLoader("postgres", pgConn)

	dt1 := time.Date(2017, 02, 03, 04, 05, 06, 0, time.UTC)
	dt2 := time.Date(2017, 03, 03, 04, 05, 06, 0, time.UTC)
	dt3 := time.Date(2017, 04, 03, 04, 05, 06, 0, time.UTC)

	// prep with 10,000 rows
	for i := 0; i < 2000; i++ {
		bl.AddRow([]interface{}{"one", 2, dt1})
		bl.AddRow([]interface{}{"two", 3, dt2})
		bl.AddRow([]interface{}{"three", 4, dt3})
		bl.AddRow([]interface{}{"four", 5, dt3})
		bl.AddRow([]interface{}{"five", 6, dt3})
	}
	cols := []string{"f1", "f2", "f3"}
	bl.Delete(`DELETE FROM test.test_commit WHERE f3 >= $1`, dt3)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bl.Commit(context.Background(), "test.test_commit", cols...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchLoader_CommitNoIndexLargeTable(b *testing.B) {
	// Benchmark a large table with no extra indexes
	// with batch size 200 and 10,000 rows per commit.
	// Not deleting in this benchmark.

	createTable(tableTestLarge)
	defer dropTable("test.test_large")

	maxBatchSize = 200
	bl := NewBatchLoader("postgres", pgConn)

	// prep with 10,000 rows
	for i := 0; i < 10000; i++ {
		bl.AddRow(largeRowVals)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bl.Commit(context.Background(), "test.test_large", largeCols...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchLoader_CommitNoIndexLargeTableSmallBatch(b *testing.B) {
	// Benchmark a large table with no extra indexes
	// with batch size 200 and 10,000 rows per commit.
	// Not deleting in this benchmark.

	createTable(tableTestLarge)
	defer dropTable("test.test_large")

	maxBatchSize = 1
	bl := NewBatchLoader("postgres", pgConn)

	// prep with 10,000 rows
	for i := 0; i < 10000; i++ {
		bl.AddRow(largeRowVals)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bl.Commit(context.Background(), "test.test_large", largeCols...)
		if err != nil {
			b.Fatal(err)
		}
	}

	// revert batch size
	maxBatchSize = 200
}

func BenchmarkBatchLoader_Commit1IndexLargeTable(b *testing.B) {
	// Benchmark a large table with 1 index
	// with batch size 200 and 10,000 rows per commit.
	// Not deleting in this benchmark.

	createTable(tableTestLarge)
	createTable(tableIdxTs1Large)
	defer dropTable("test.test_large")

	maxBatchSize = 200
	bl := NewBatchLoader("postgres", pgConn)

	// prep with 10,000 rows
	for i := 0; i < 10000; i++ {
		bl.AddRow(largeRowVals)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bl.Commit(context.Background(), "test.test_large", largeCols...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchLoader_Commit2IndexLargeTable(b *testing.B) {
	// Benchmark a large table with 2 indexes
	// with batch size 200 and 10,000 rows per commit.
	// Not deleting in this benchmark.

	createTable(tableTestLarge)
	createTable(tableIdxTs1Large)
	createTable(tableIdxCreatedLarge)
	defer dropTable("test.test_large")

	maxBatchSize = 200
	bl := NewBatchLoader("postgres", pgConn)

	// prep with 10,000 rows
	for i := 0; i < 10000; i++ {
		bl.AddRow(largeRowVals)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bl.Commit(context.Background(), "test.test_large", largeCols...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchLoader_Commit2IndexLargeTableWithDeleteNotIndex(b *testing.B) {
	// Benchmark a large table with 2 indexes and deleting
	// with batch size 200 and 10,000 rows per commit.

	createTable(tableTestLarge)
	createTable(tableIdxTs1Large)
	createTable(tableIdxCreatedLarge)
	defer dropTable("test.test_large")

	maxBatchSize = 200
	bl := NewBatchLoader("postgres", pgConn)
	bl.Delete(`DELETE FROM test.test_large WHERE ts2 >= '2017-04-03 04:05:06'`)

	// prep with 10,000 rows
	for i := 0; i < 10000; i++ {
		bl.AddRow(largeRowVals)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bl.Commit(context.Background(), "test.test_large", largeCols...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchLoader_Commit2IndexLargeTableWithDeleteOnIndex(b *testing.B) {
	// Benchmark a large table with 2 indexes and deleting
	// with batch size 200 and 10,000 rows per commit.

	createTable(tableTestLarge)
	createTable(tableIdxTs1Large)
	createTable(tableIdxCreatedLarge)
	defer dropTable("test.test_large")

	maxBatchSize = 200
	bl := NewBatchLoader("postgres", pgConn)
	bl.Delete(`DELETE FROM test.test_large WHERE ts1 >= '2017-02-03 04:05:06'`)

	// prep with 10,000 rows
	for i := 0; i < 10000; i++ {
		bl.AddRow(largeRowVals)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bl.Commit(context.Background(), "test.test_large", largeCols...)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func createTable(query string) {
	_, err := pgConn.Exec(query)
	if err != nil {
		log.Fatalln(err)
	}
}

func dropTable(tableName string) {
	_, err := pgConn.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName))
	if err != nil {
		log.Fatalln(err)
	}
}

func createTableMySQL(query string) {
	_, err := msqlConn.Exec(query)
	if err != nil {
		log.Fatalln(err)
	}
}

func dropTableMySQL(tableName string) {
	_, err := msqlConn.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, tableName))
	if err != nil {
		log.Fatalln(err)
	}
}

var tableTestMySQL = `
	CREATE TABLE IF NOT EXISTS test_mysql (
		id int UNSIGNED AUTO_INCREMENT PRIMARY KEY,
		f1 varchar(10) NOT NULL,
		f2 int NOT NULL,
		f3 timestamp NOT NULL,
		created timestamp DEFAULT NOW()
	);
`

var tableTestCommit = `
	CREATE TABLE IF NOT EXISTS test.test_commit (
		id bigserial PRIMARY KEY NOT NULL,
		f1 varchar(10) NOT NULL,
		f2 int NOT NULL,
		f3 timestamp NOT NULL,
		created timestamp DEFAULT NOW()
	);
`

var tableIdxCreated = `
	CREATE INDEX idx_test_commit_created
		on test.test_commit (created);
`

var tableIdxF3 = `
	CREATE INDEX idx_test_commit_f3
		on test.test_commit (f3);
`

var tableTestLarge = `
CREATE TABLE IF NOT EXISTS test.test_large (
  id bigserial primary key not null,

  nf1 numeric,
  nf2 numeric,
  nf3 numeric,
  nf4 numeric,
  nf5 numeric,
  nf6 numeric,
  nf7 numeric,
  nf8 numeric,
  nf9 numeric,
  nf10 numeric,
  nf11 numeric,
  nf12 numeric,
  nf13 numeric,
  nf14 numeric,
  nf15 numeric,
  nf16 numeric,
  nf17 numeric,
  nf18 numeric,
  nf19 numeric,
  nf20 numeric,
  nf21 numeric,
  nf22 numeric,
  nf23 numeric,
  nf24 numeric,
  nf25 numeric,
  nf26 numeric,
  nf27 numeric,
  nf28 numeric,
  nf29 numeric,
  nf30 numeric,
  nf31 numeric,
  nf32 numeric,
  nf33 numeric,
  nf34 numeric,
  nf35 numeric,
  nf36 numeric,
  nf37 numeric,
  nf38 numeric,
  nf39 numeric,
  nf40 numeric,
  nf41 numeric,
  nf42 numeric,
  nf43 numeric,
  nf44 numeric,
  nf45 numeric,
  nf46 numeric,
  nf47 numeric,
  nf48 numeric,
  nf49 numeric,

  if1 integer,
  if2 integer,
  if3 integer,
  if4 integer,
  if5 integer,
  if6 integer,
  if7 integer,
  if8 integer,
  if9 integer,
  if10 integer,
  if11 integer,
  if12 integer,
  if13 integer,
  if14 integer,
  if15 integer,
  if16 integer,
  if17 integer,
  if18 integer,
  if19 integer,
  if20 integer,
  if21 integer,
  if22 integer,
  if23 integer,
  if24 integer,
  if25 integer,
  if26 integer,
  if27 integer,
  if28 integer,
  if29 integer,
  if30 integer,
  if31 integer,
  if32 integer,
  if33 integer,
  if34 integer,
  if35 integer,
  if36 integer,
  if37 integer,
  if38 integer,
  if39 integer,
  if40 integer,
  if41 integer,
  if42 integer,
  if43 integer,
  if44 integer,
  if45 integer,
  if46 integer,
  if47 integer,
  if48 integer,
  if49 integer,
  if50 integer,
  if51 integer,
  if52 integer,
  if53 integer,
  if54 integer,
  if55 integer,
  if56 integer,
  if57 integer,
  if58 integer,
  if59 integer,
  if60 integer,
  if61 integer,
  if62 integer,
  if63 integer,
  if64 integer,
  if65 integer,
  if66 integer,
  if67 integer,
  if68 integer,
  if69 integer,
  if70 integer,
  if71 integer,
  if72 integer,
  if73 integer,
  if74 integer,
  if75 integer,
  if76 integer,
  if77 integer,
  if78 integer,
  if79 integer,

  bif1 bigint,
  bif2 bigint,
  bif3 bigint,
  bif4 bigint,
  bif5 bigint,
    
  vc1 varchar(256) default ''::character varying,
  vc2 varchar(256) default ''::character varying,
  vc3 varchar(256) default ''::character varying,
  vc4 varchar(256) default ''::character varying,
  vc5 varchar(256) default ''::character varying,
  vc6 varchar(256) default ''::character varying,

  ts1 timestamp not null,
  ts2 timestamp not null,
  created timestamp default now() not null
);
`

var tableIdxTs1Large = `
	CREATE INDEX idx_test_large_ts1
		on test.test_large (ts1);
`

var tableIdxCreatedLarge = `
	CREATE INDEX idx_test_large_created
		on test.test_large (created);
`

var largeCols = []string{
	"nf1",
	"nf2",
	"nf3",
	"nf4",
	"nf5",
	"nf6",
	"nf7",
	"nf8",
	"nf9",
	"nf10",
	"nf11",
	"nf12",
	"nf13",
	"nf14",
	"nf15",
	"nf16",
	"nf17",
	"nf18",
	"nf19",
	"nf20",
	"nf21",
	"nf22",
	"nf23",
	"nf24",
	"nf25",
	"nf26",
	"nf27",
	"nf28",
	"nf29",
	"nf30",
	"nf31",
	"nf32",
	"nf33",
	"nf34",
	"nf35",
	"nf36",
	"nf37",
	"nf38",
	"nf39",
	"nf40",
	"nf41",
	"nf42",
	"nf43",
	"nf44",
	"nf45",
	"nf46",
	"nf47",
	"nf48",
	"nf49",
	"if1",
	"if2",
	"if3",
	"if4",
	"if5",
	"if6",
	"if7",
	"if8",
	"if9",
	"if10",
	"if11",
	"if12",
	"if13",
	"if14",
	"if15",
	"if16",
	"if17",
	"if18",
	"if19",
	"if20",
	"if21",
	"if22",
	"if23",
	"if24",
	"if25",
	"if26",
	"if27",
	"if28",
	"if29",
	"if30",
	"if31",
	"if32",
	"if33",
	"if34",
	"if35",
	"if36",
	"if37",
	"if38",
	"if39",
	"if40",
	"if41",
	"if42",
	"if43",
	"if44",
	"if45",
	"if46",
	"if47",
	"if48",
	"if49",
	"if50",
	"if51",
	"if52",
	"if53",
	"if54",
	"if55",
	"if56",
	"if57",
	"if58",
	"if59",
	"if60",
	"if61",
	"if62",
	"if63",
	"if64",
	"if65",
	"if66",
	"if67",
	"if68",
	"if69",
	"if70",
	"if71",
	"if72",
	"if73",
	"if74",
	"if75",
	"if76",
	"if77",
	"if78",
	"if79",
	"bif1",
	"bif2",
	"bif3",
	"bif4",
	"bif5",
	"vc1",
	"vc2",
	"vc3",
	"vc4",
	"vc5",
	"vc6",
	"ts1",
	"ts2",
}

var largeRowVals = []interface{}{
	"1.234",
	"2.234",
	"3.234",
	"4.234",
	"5.234",
	"6.234",
	"7.234",
	"8.234",
	"9.234",
	"10.456",
	"11.456",
	"12.456",
	"13.456",
	"14.456",
	"15.456",
	"16.456",
	"17.456",
	"18.456",
	"19.456",
	"20.456",
	"21.456",
	"22.456",
	"23.456",
	"24.456",
	"25.456",
	"26.456",
	"27.456",
	"28.456",
	"29.456",
	"30.456",
	"31.456",
	"32.456",
	"33.456",
	"34.456",
	"35.456",
	"36.456",
	"37.456",
	"38.456",
	"39.456",
	"40.456",
	"41.456",
	"42.456",
	"43.456",
	"44.456",
	"45.456",
	"46.456",
	"47.456",
	"48.456",
	"49.456",
	1,
	2,
	3,
	4,
	5,
	6,
	7,
	8,
	9,
	10,
	11,
	12,
	13,
	14,
	15,
	16,
	17,
	18,
	19,
	20,
	21,
	22,
	23,
	24,
	25,
	26,
	27,
	28,
	29,
	30,
	31,
	32,
	33,
	34,
	35,
	36,
	37,
	38,
	39,
	40,
	41,
	42,
	43,
	44,
	45,
	46,
	47,
	48,
	49,
	50,
	51,
	52,
	53,
	54,
	55,
	56,
	57,
	58,
	59,
	60,
	61,
	62,
	63,
	64,
	65,
	66,
	67,
	68,
	69,
	70,
	71,
	72,
	73,
	74,
	75,
	76,
	77,
	78,
	79,
	1,
	2,
	3,
	4,
	5,
	"1 hundred years",
	"2 hundred years",
	"3 hundred years",
	"4 hundred years",
	"5 hundred years",
	"6 hundred years",
	"2017-02-03 04:05:06",
	"2017-04-03 04:05:06",
}

func TestMain(m *testing.M) {
	err := setupPG()
	if err != nil {
		log.Fatal(err)
	}

	err = setupMySQL()
	if err != nil {
		teardownPG()
		log.Fatal(err)
	}

	// run tests
	code := m.Run()

	// teardown
	teardownPG()
	teardownMySQL()

	os.Exit(code)
}

func setupPG() error {
	// postgres user (default is current user)
	usr, _ := user.Current()
	un := usr.Username

	// setup postgres test db
	var err error
	pgDB, err = sql.Open("postgres", fmt.Sprintf(pgConnStr, un, "", "", "postgres"))
	if err != nil {
		return err
	}
	_, err = pgDB.Exec("CREATE DATABASE ci_test;")
	if err != nil {
		if err.Error() != `pq: database "ci_test" already exists` {
			return err
		}
	}

	// pg conn for all tests to use
	pgConn, err = sql.Open("postgres", fmt.Sprintf(pgConnStr, un, "", "", "ci_test"))
	if err != nil {
		return err
	}

	// create test schema
	_, err = pgConn.Exec(`Create SCHEMA IF NOT EXISTS test;`)
	if err != nil {
		return err
	}

	return nil
}

func teardownPG() {
	// close ci_test database session so it can be dropped
	pgConn.Close()

	// remove postgres test db
	pgDB.Exec("DROP DATABASE ci_test;")
	pgDB.Close()
}

func setupMySQL() error {
	// setup mysql test db
	var err error
	msqlDB, err = sql.Open("mysql", fmt.Sprintf(msqlConnStr, "root", "", "", ""))
	if err != nil {
		return err
	}
	_, err = msqlDB.Exec("CREATE DATABASE IF NOT EXISTS ci_test;")
	if err != nil {
		return err

	}

	// mysql conn for all tests to use
	msqlConn, err = sql.Open("mysql", fmt.Sprintf(msqlConnStr, "root", "", "", "ci_test"))
	if err != nil {
		return err
	}

	return nil
}

func teardownMySQL() {
	// close ci_test database session so it can be dropped
	msqlConn.Close()

	// remove mysql test db
	msqlDB.Exec("DROP DATABASE ci_test;")
	msqlDB.Close()
}
