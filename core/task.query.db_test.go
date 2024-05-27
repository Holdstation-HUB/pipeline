package core_test

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/magiconair/properties/assert"
	"pipeline/core"
	"pipeline/test"
	"testing"
)

func TestQueryDBTaskWithNoParam(t *testing.T) {
	zapLog := test.NewMockZapLog()
	db, mock := test.NewMockDB()

	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "John Doe").
		AddRow(2, "Duong NT")

	mock.ExpectQuery("^SELECT (.+) FROM users$").
		WillReturnRows(rows)

	runner := core.NewRunner(core.NewDefaultConfig(), zapLog, nil, nil, db)

	specs := core.Spec{
		DotDagSource: `
			users [type="querydb" query="SELECT * FROM users" params=<[]>]
		`,
	}
	_, trrs, err := runner.ExecuteRun(context.TODO(), specs, core.NewVarsFrom(nil), zapLog)
	if err != nil {
		t.Fatal(err)
	}

	finalResult, err := trrs.FinalResult(nil).SingularResult()
	if err != nil {
		t.Fatal(err)
	}

	users := finalResult.Value.([]map[string]any)
	assert.Equal(t, 2, len(users))

	johnDoe := users[0]
	assert.Equal(t, int(johnDoe["id"].(int64)), 1)
	assert.Equal(t, johnDoe["name"], "John Doe")

	duong := users[1]
	assert.Equal(t, int(duong["id"].(int64)), 2)
	assert.Equal(t, duong["name"], "Duong NT")
}

func TestQueryDBTaskWithFixedParams(t *testing.T) {
	zapLog := test.NewMockZapLog()
	db, mock := test.NewMockDB()
	runner := core.NewRunner(core.NewDefaultConfig(), zapLog, nil, nil, db)

	query := `SELECT id, name FROM users WHERE`

	// one param
	mock.ExpectQuery(query).
		WithArgs("1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John Doe"))

	specs1 := core.Spec{
		DotDagSource: `
			users [type="querydb" query="SELECT id, name FROM users WHERE id = ?" params=<["1"]>]
		`,
	}
	_, trrs, err := runner.ExecuteRun(context.TODO(), specs1, core.NewVarsFrom(nil), zapLog)
	if err != nil {
		t.Fatal(err)
	}

	finalResult, err := trrs.FinalResult(zapLog).SingularResult()
	if err != nil {
		t.Fatal(err)
	}

	users := finalResult.Value.([]map[string]any)
	assert.Equal(t, len(users), 1)

	johnDoe := users[0]
	assert.Equal(t, int(johnDoe["id"].(int64)), 1)
	assert.Equal(t, johnDoe["name"], "John Doe")

	// multiple params
	mock.ExpectQuery(query).
		WithArgs("1", "2").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
			AddRow(1, "John Doe").
			AddRow(2, "Duong NT"))

	specs2 := core.Spec{
		DotDagSource: `
			users [type="querydb" query="SELECT id, name FROM users WHERE id = ? OR id = ?" params=<["1","2"]>]
		`,
	}
	_, trrs, err = runner.ExecuteRun(context.TODO(), specs2, core.NewVarsFrom(nil), zapLog)
	if err != nil {
		t.Fatal(err)
	}

	finalResult, err = trrs.FinalResult(zapLog).SingularResult()
	if err != nil {
		t.Fatal(err)
	}

	users = finalResult.Value.([]map[string]any)
	assert.Equal(t, len(users), 2)

	johnDoe = users[0]
	assert.Equal(t, int(johnDoe["id"].(int64)), 1)
	assert.Equal(t, johnDoe["name"], "John Doe")

	duong := users[1]
	assert.Equal(t, int(duong["id"].(int64)), 2)
	assert.Equal(t, duong["name"], "Duong NT")
}

func TestQueryDBTaskWithPassingParams(t *testing.T) {
	zapLog := test.NewMockZapLog()
	db, mock := test.NewMockDB()
	runner := core.NewRunner(core.NewDefaultConfig(), zapLog, nil, nil, db)

	query := `SELECT id, name FROM users WHERE`

	// one param
	mock.ExpectQuery(query).
		WithArgs("1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "John Doe"))

	specs := core.Spec{
		DotDagSource: `
			users [type="querydb" query="SELECT id, name FROM users WHERE id = ?" params=<[$(user.id)]>]
		`,
	}
	params := map[string]any{
		"user": map[string]any{
			"id": int64(1),
		},
	}
	_, trrs, err := runner.ExecuteRun(context.TODO(), specs, core.NewVarsFrom(params), zapLog)
	if err != nil {
		t.Fatal(err)
	}

	finalResult, err := trrs.FinalResult(zapLog).SingularResult()
	if err != nil {
		t.Fatal(err)
	}

	users := finalResult.Value.([]map[string]any)
	assert.Equal(t, len(users), 1)

	johnDoe := users[0]
	assert.Equal(t, int(johnDoe["id"].(int64)), 1)
	assert.Equal(t, johnDoe["name"], "John Doe")

	// multiple params
	mock.ExpectQuery(query).
		WithArgs("1", "2").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).
			AddRow(1, "John Doe").
			AddRow(2, "Duong NT"))

	specs = core.Spec{
		DotDagSource: `
			users [type="querydb" query="SELECT id, name FROM users WHERE id = ? OR id = ?" params=<[$(users.0.id), $(users.1.id)]>]
		`,
	}
	params = map[string]any{
		"users": []map[string]any{
			{
				"id": int64(1),
			},
			{
				"id": int64(2),
			},
		},
	}
	_, trrs, err = runner.ExecuteRun(context.TODO(), specs, core.NewVarsFrom(params), zapLog)
	if err != nil {
		t.Fatal(err)
	}

	finalResult, err = trrs.FinalResult(zapLog).SingularResult()
	if err != nil {
		t.Fatal(err)
	}

	users = finalResult.Value.([]map[string]any)
	assert.Equal(t, len(users), 2)

	johnDoe = users[0]
	assert.Equal(t, int(johnDoe["id"].(int64)), 1)
	assert.Equal(t, johnDoe["name"], "John Doe")

	duong := users[1]
	assert.Equal(t, int(duong["id"].(int64)), 2)
	assert.Equal(t, duong["name"], "Duong NT")
}
