package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/pcelvng/task-tools/db/stat"
)

/*
`
START TRANSACTION;

INSERT INTO table_name (column1, column2, column3,...)
values (value1, value2, value3,...);

INSERT INTO table_name (column1, column2, column3,...)
values (value1, value2, value3,...);

INSERT INTO table_name (column1, column2, column3,...)
values (value1, value2, value3,...);

INSERT INTO table_name (column1, column2, column3,...)
values (value1, value2, value3,...);

END TRANSACTION;
`
*/

var (
	insertTmpl = "INSERT INTO %s (%s) VALUES (%s);\n"
)

func New(conn *sql.DB, table string, fields []string) (*Loader, error) {
	fieldCnt := len(fields)
	if fieldCnt == 0 {
		return nil, errors.New("w")
	}

	return &Loader{
		conn:      conn,
		tableName: table,
		fields:    strings.Join(fields, ","),
		fieldCnt:  fieldCnt,
		dbSts:     stat.New(),
	}, nil
}

type Loader struct {
	conn      *sql.DB
	fields    string
	fieldCnt  int
	tableName string
	dbSts     stat.Stats
	inserts   string
}

func (l *Loader) Add(values string) {
	l.inserts += fmt.Sprintf(insertTmpl, l.tableName, l.fields, values)
	l.dbSts.AddRow()
}

func (l *Loader) Commit(ctx context.Context) error {
	tx, err := l.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	tx.Commit()
	//tx.Prepare(l.inserts)
	return nil
}

func (l *Loader) Stats() stat.Stats {
	return l.dbSts.Clone()
}
