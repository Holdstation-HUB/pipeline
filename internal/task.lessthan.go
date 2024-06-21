package internal

import (
	"context"
	"github.com/Holdstation-HUB/pipeline/core"
	"go.uber.org/zap"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

const TaskTypeLessThan core.TaskType = "lessthan"

// Return types:
//
//	bool
type LessThanTask struct {
	core.BaseTask `mapstructure:",squash"`
	Left          string `json:"input"`
	Right         string `json:"limit"`
}

var (
	_ core.Task = (*LessThanTask)(nil)
)

func (t *LessThanTask) Type() core.TaskType {
	return TaskTypeLessThan
}

func (t *LessThanTask) Run(_ context.Context, _ *zap.Logger, vars core.Vars, inputs []core.Result) (result core.Result, runInfo core.RunInfo) {
	_, err := core.CheckInputs(inputs, 0, 1, 0)
	if err != nil {
		return core.Result{Error: errors.Wrap(err, "task inputs")}, runInfo
	}

	var (
		a core.DecimalParam
		b core.DecimalParam
	)

	err = multierr.Combine(
		errors.Wrap(core.ResolveParam(&a, core.From(core.VarExpr(t.Left, vars), core.NonemptyString(t.Left), core.Input(inputs, 0))), "left"),
		errors.Wrap(core.ResolveParam(&b, core.From(core.VarExpr(t.Right, vars), core.NonemptyString(t.Right))), "right"),
	)
	if err != nil {
		return core.Result{Error: err}, runInfo
	}

	value := a.Decimal().LessThan(b.Decimal())
	return core.Result{Value: value}, runInfo
}
