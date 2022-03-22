package mysql

import (
	"testing"
)

import (
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/stretchr/testify/assert"
)

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
			DBName: "bigtest",
		},
	}

	InitTableMetaCache("bigtest")

	exec := multiDeleteExecutor{
		originalSQLs: sourceSQLs,
		mc:           mc,
		stmts:        insertStmts,
		args:         nil,
	}

	tableName := exec.GetTableName()
	assert.NotEmpty(t, tableName)

	whereCondition := exec.GetWhereCondition()
	assert.NotEmpty(t, whereCondition)

	tableRes, _ := exec.beforeImage()
	assert.NotEmpty(t, tableRes)
}
