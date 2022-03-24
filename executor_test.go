package mysql

import (
	"fmt"
	"github.com/gotrx/mysql/schema"
	"testing"
)

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/stretchr/testify/assert"
)

var DBName = "gotest"

func TestInsertExecutor_GetTableName(t *testing.T) {
	var sourceSQL = "INSERT INTO s1(name,age) values('碉堡',10)"

	parser := parser.New()
	act, _ := parser.ParseOneStmt(sourceSQL, "", "")

	insertStmt, _ := act.(*ast.InsertStmt)

	exec := insertExecutor{
		originalSQL: sourceSQL,
		mc:          nil,
		stmt:        insertStmt,
		args:        nil,
	}

	tableName := exec.GetTableName()
	assert.NotEmpty(t, tableName)
}

// batch delete
func TestMultiDeleteExecutor(t *testing.T) {
	var sourceSQLs = []string{
		"DELETE FROM s1 WHERE id = 1",
		"DELETE FROM s1 WHERE id = 2",
		"DELETE FROM s1",
	}

	var insertStmts []*ast.DeleteStmt
	for _, sourceSQL := range sourceSQLs {
		parser := parser.New()
		act, _ := parser.ParseOneStmt(sourceSQL, "", "")
		insertStmt, _ := act.(*ast.DeleteStmt)
		insertStmts = append(insertStmts, insertStmt)
	}

	conn := new(mockConn)
	mc := &mysqlConn{
		buf: newBuffer(conn),
		cfg: &Config{
			DBName: DBName,
		},
	}

	InitTableMetaCache(DBName)
	tableMeta := schema.TableMeta{
		TableName: "s1",
		Columns:   []string{"id", "name", "age"},
	}
	GetTableMetaCache(DBName).addCache(fmt.Sprintf("%s.s1", DBName), tableMeta)

	exec := multiDeleteExecutor{
		originalSQLs: sourceSQLs,
		mc:           mc,
		stmts:        insertStmts,
		args:         nil,
	}

	//tableRes, _ := exec.BeforeImage()
	assert.NotEmpty(t, exec)
}

//batch update
func TestMultiUpdateExecutor(t *testing.T) {
	tableName := "table_update_executor_test"
	var sourceSQLs = []string{
		"update table_update_executor_test set name = 'WILL' where id = 1;",
		"update table_update_executor_test set name = 'WILL2' where id = 2",
	}

	var stmts []*ast.UpdateStmt
	for _, sourceSQL := range sourceSQLs {
		parser := parser.New()
		act, _ := parser.ParseOneStmt(sourceSQL, "", "")
		stmt, _ := act.(*ast.UpdateStmt)
		stmts = append(stmts, stmt)
	}

	conn := new(mockConn)
	mc := &mysqlConn{
		buf: newBuffer(conn),
		cfg: &Config{
			DBName: DBName,
		},
	}

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

	exec := multiUpdateExecutor{
		originalSQLs: sourceSQLs,
		mc:           mc,
		stmts:        stmts,
		args:         nil,
	}

	tableRes, _ := exec.BeforeImage()
	assert.NotEmpty(t, tableRes)

}
