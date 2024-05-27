package core

import (
	"context"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type QueryDBTask struct {
	BaseTask `mapstructure:",squash"`
	Query    string `json:"query"`
	Params   string `json:"params"`
	db       *gorm.DB
}

var _ Task = (*QueryDBTask)(nil)

func (q QueryDBTask) Type() TaskType {
	return TaskTypeQueryDB
}

func (q QueryDBTask) Run(ctx context.Context, _ *zap.Logger, vars Vars, inputs []Result) (Result, RunInfo) {
	var (
		valuesAndErrs SliceParam
		stringValues  StringSliceParam
	)
	err := errors.Wrap(ResolveParam(&valuesAndErrs, From(VarExpr(q.Params, vars), JSONWithVarExprs(q.Params, vars, true), Inputs(inputs))), "params")
	if err != nil {
		return Result{Error: err}, RunInfo{}
	}

	params, faults := valuesAndErrs.FilterErrors()
	if faults > 0 {
		return Result{Error: errors.Wrapf(ErrTooManyErrors, "Number of faulty inputs too many")}, RunInfo{}
	}

	err = stringValues.UnmarshalPipelineParam(params)
	if err != nil {
		return Result{Error: err}, RunInfo{}
	}

	// Convert custom type to []interface{} cause db.Raw only receive param with types []interface{}
	interfaceValues := make([]any, len(stringValues))
	for i, v := range stringValues {
		interfaceValues[i] = v
	}

	var result []map[string]any
	if err := q.db.WithContext(ctx).
		Raw(q.Query, interfaceValues...).
		Scan(&result).
		Error; err != nil {
		return Result{Error: err}, RunInfo{}
	}

	return Result{Value: result}, RunInfo{}
}
