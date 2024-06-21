package task

import (
	"context"
	"github.com/Holdstation-HUB/pipeline/core"
	"go.uber.org/zap"
	"math"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

const TaskTypeMultiply core.TaskType = "multiply"

// Return types:
//
//	*decimal.Decimal
type MultiplyTask struct {
	core.BaseTask `mapstructure:",squash"`
	Input         string `json:"input"`
	Times         string `json:"times"`
}

var (
	_                  core.Task = (*MultiplyTask)(nil)
	ErrMultiplyOverlow           = errors.New("multiply overflow")
)

func (t *MultiplyTask) Type() core.TaskType {
	return TaskTypeMultiply
}

func (t *MultiplyTask) Run(_ context.Context, _ *zap.Logger, vars core.Vars, inputs []core.Result) (result core.Result, runInfo core.RunInfo) {
	_, err := core.CheckInputs(inputs, 0, 1, 0)
	if err != nil {
		return core.Result{Error: errors.Wrap(err, "task inputs")}, runInfo
	}

	var (
		a core.DecimalParam
		b core.DecimalParam
	)

	err = multierr.Combine(
		errors.Wrap(core.ResolveParam(&a, core.From(core.VarExpr(t.Input, vars), core.NonemptyString(t.Input), core.Input(inputs, 0))), "input"),
		errors.Wrap(core.ResolveParam(&b, core.From(core.VarExpr(t.Times, vars), core.NonemptyString(t.Times))), "times"),
	)
	if err != nil {
		return core.Result{Error: err}, runInfo
	}

	newExp := int64(a.Decimal().Exponent()) + int64(b.Decimal().Exponent())
	if newExp > math.MaxInt32 || newExp < math.MinInt32 {
		return core.Result{Error: ErrMultiplyOverlow}, runInfo
	}

	value := a.Decimal().Mul(b.Decimal())
	return core.Result{Value: value}, runInfo
}
