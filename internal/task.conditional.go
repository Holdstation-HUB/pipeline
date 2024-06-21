package internal

import (
	"context"
	"github.com/Holdstation-HUB/pipeline/core"
	"go.uber.org/zap"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

const TaskTypeConditional core.TaskType = "conditional"

// ConditionalTask checks if data is false
// for now this is all we need but in the future we can
// expand this to handle more general conditional statements
type ConditionalTask struct {
	core.BaseTask `mapstructure:",squash"`
	Data          string `json:"data"`
}

var _ core.Task = (*ConditionalTask)(nil)

func (t *ConditionalTask) Type() core.TaskType {
	return TaskTypeConditional
}

func (t *ConditionalTask) Run(_ context.Context, _ *zap.Logger, vars core.Vars, inputs []core.Result) (result core.Result, runInfo core.RunInfo) {
	_, err := core.CheckInputs(inputs, 0, 1, 0)
	if err != nil {
		return core.Result{Error: errors.Wrap(err, "task inputs")}, runInfo
	}
	var (
		boolParam core.BoolParam
	)
	err = multierr.Combine(
		errors.Wrap(core.ResolveParam(&boolParam, core.From(core.VarExpr(t.Data, vars), core.Input(inputs, 0), nil)), "data"),
	)
	if err != nil {
		return core.Result{Error: err}, runInfo
	}
	if !boolParam {
		return core.Result{Error: errors.New("conditional was not satisfied")}, runInfo
	}
	return core.Result{Value: true}, runInfo
}
