package mysql

import (
	"database/sql/driver"
	"fmt"
	"math"
	"testing"
	"time"
)

import (
	"github.com/pingcap/parser"
	"github.com/stretchr/testify/assert"
)

import (
	"github.com/gotrx/mysql/schema"
)

var (
	DBName     = "gotest"
	deleteSQLs = []string{
		"DELETE FROM s1 WHERE id = 1",
		"DELETE FROM s1 WHERE id = 2",
		"DELETE FROM s1",
	}

	tableName      = "table_update_executor_test"
	multiUpdateSQL = "update table_update_executor_test set name = 'WILL' where id = 1;\nupdate table_update_executor_test set name = 'WILL2' where id = 2"
)

func init() {
	InitTableMetaCache(DBName)
	tableMeta := schema.TableMeta{
		TableName: tableName,
		Columns:   []string{"id", "name", "age"},
		AllColumns: map[string]schema.ColumnMeta{
			"id": {
				TableName:  DBName,
				ColumnName: "id",
			},
		},
		AllIndexes: map[string]schema.IndexMeta{
			"id": {
				ColumnName: "id",
				IndexType:  schema.IndexType_PRIMARY,
			},
		},
	}
	GetTableMetaCache(DBName).addCache(fmt.Sprintf("%s.%s", DBName, tableName), tableMeta)
}

//Multi
func TestMultiExecutor(t *testing.T) {
	parser := parser.New()
	acts, _, _ := parser.Parse(multiUpdateSQL, "", "")

	exec := multiExecutor{
		acts: acts,
		args: []driver.Value{
			int64(42424242),
			float64(math.Pi),
			false,
			time.Unix(1423411542, 807015000),
			[]byte("bytes containing special chars ' \" \a \x00"),
			"string containing special chars ' \" \a \x00",
		},
	}

	tableRes, _ := exec.BeforeImage()
	assert.NotEmpty(t, tableRes)
}
