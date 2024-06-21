package internal

import (
	"context"
	"github.com/Holdstation-HUB/pipeline/core"
	"go.uber.org/zap"

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"go.uber.org/multierr"
)

const TaskTypeLength core.TaskType = "length"

// Return types:
//
//	*decimal.Decimal
type LengthTask struct {
	core.BaseTask `mapstructure:",squash"`
	Input         string `json:"input"`
}

var _ core.Task = (*LengthTask)(nil)

func (t *LengthTask) Type() core.TaskType {
	return TaskTypeLength
}

func (t *LengthTask) Run(_ context.Context, _ *zap.Logger, vars core.Vars, inputs []core.Result) (result core.Result, runInfo core.RunInfo) {
	_, err := core.CheckInputs(inputs, 0, 1, 0)
	if err != nil {
		return core.Result{Error: errors.Wrap(err, "task inputs")}, runInfo
	}

	var input core.BytesParam

	err = multierr.Combine(
		errors.Wrap(core.ResolveParam(&input, core.From(core.VarExpr(t.Input, vars), core.NonemptyString(t.Input), core.Input(inputs, 0))), "input"),
	)
	if err != nil {
		return core.Result{Error: err}, runInfo
	}

	return core.Result{Value: decimal.NewFromInt(int64(len(input)))}, runInfo
}
