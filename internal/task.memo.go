package internal

import (
	"context"
	"github.com/Holdstation-HUB/pipeline/core"
	"go.uber.org/zap"

	"github.com/pkg/errors"
)

const TaskTypeMemo core.TaskType = "memo"

// Memo task returns its value as a result
//
// e.g. [type=memo value=10] => 10

type MemoTask struct {
	core.BaseTask `mapstructure:",squash"`
	Value         string `json:"value"`
}

var _ core.Task = (*MemoTask)(nil)

func (t *MemoTask) Type() core.TaskType {
	return TaskTypeMemo
}

func (t *MemoTask) Run(_ context.Context, _ *zap.Logger, vars core.Vars, inputs []core.Result) (core.Result, core.RunInfo) {
	_, err := core.CheckInputs(inputs, 0, 1, 0)
	if err != nil {
		return core.Result{Error: errors.Wrap(err, "task value missing")}, core.RunInfo{}
	}

	var value core.ObjectParam
	err = errors.Wrap(core.ResolveParam(&value, core.From(core.JSONWithVarExprs(t.Value, vars, false), core.Input(inputs, 0))), "value")
	if err != nil {
		return core.Result{Error: err}, core.RunInfo{}
	}

	return core.Result{Value: value}, core.RunInfo{}
}
