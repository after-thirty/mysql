package mysql

import (
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/stretchr/testify/assert"
	"testing"
)

import (
	"github.com/gotrx/mysql/schema"
)

var (
	DBName         = "gotest"
	deleteSQL      = "DELETE FROM s1 WHERE id = 1\nDELETE FROM s1 WHERE id = 2\nDELETE FROM s1"
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

	var stmts []*ast.DMLNode

	for _, act := range acts {
		node := act.(ast.DMLNode)
		stmts = append(stmts, &node)
	}

	exec := NewBaseExecutor(&multiExecutor{stmts: stmts}, nil, multiUpdateSQL, nil)

	tableRes, _ := exec.BeforeImage()
	assert.NotEmpty(t, tableRes)
}
