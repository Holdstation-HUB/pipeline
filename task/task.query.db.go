package task

import (
	"context"
	"github.com/Holdstation-HUB/pipeline/core"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const TaskTypeQueryDB core.TaskType = "querydb"

type QueryDBTask struct {
	core.BaseTask `mapstructure:",squash"`
	Query         string `json:"query"`
	Params        string `json:"params"`
	Db            *gorm.DB
}

var _ core.Task = (*QueryDBTask)(nil)

func (q QueryDBTask) Type() core.TaskType {
	return TaskTypeQueryDB
}

func (q QueryDBTask) Run(ctx context.Context, _ *zap.Logger, vars core.Vars, inputs []core.Result) (core.Result, core.RunInfo) {
	var (
		valuesAndErrs core.SliceParam
		stringValues  core.StringSliceParam
	)
	err := errors.Wrap(core.ResolveParam(&valuesAndErrs, core.From(core.VarExpr(q.Params, vars), core.JSONWithVarExprs(q.Params, vars, true), core.Inputs(inputs))), "params")
	if err != nil {
		return core.Result{Error: err}, core.RunInfo{}
	}

	params, faults := valuesAndErrs.FilterErrors()
	if faults > 0 {
		return core.Result{Error: errors.Wrapf(core.ErrTooManyErrors, "Number of faulty inputs too many")}, core.RunInfo{}
	}

	err = stringValues.UnmarshalPipelineParam(params)
	if err != nil {
		return core.Result{Error: err}, core.RunInfo{}
	}

	// Convert custom type to []interface{} cause Db.Raw only receive param with types []interface{}
	interfaceValues := make([]any, len(stringValues))
	for i, v := range stringValues {
		interfaceValues[i] = v
	}

	var result []map[string]any
	if err := q.Db.WithContext(ctx).
		Raw(q.Query, interfaceValues...).
		Scan(&result).
		Error; err != nil {
		return core.Result{Error: err}, core.RunInfo{}
	}

	return core.Result{Value: result}, core.RunInfo{}
}
