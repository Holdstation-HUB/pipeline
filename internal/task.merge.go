package internal

import (
	"context"
	"github.com/Holdstation-HUB/pipeline/core"
	"go.uber.org/zap"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
)

const TaskTypeMerge core.TaskType = "merge"

// Return types:
//
//	map[string]interface{}
type MergeTask struct {
	core.BaseTask `mapstructure:",squash"`
	Left          string `json:"left"`
	Right         string `json:"right"`
}

var _ core.Task = (*MergeTask)(nil)

func (t *MergeTask) Type() core.TaskType {
	return TaskTypeMerge
}

func (t *MergeTask) Run(_ context.Context, _ *zap.Logger, vars core.Vars, inputs []core.Result) (result core.Result, runInfo core.RunInfo) {
	_, err := core.CheckInputs(inputs, 0, 1, 0)
	if err != nil {
		return core.Result{Error: errors.Wrap(err, "task inputs")}, runInfo
	}

	var (
		lMap core.MapParam
		rMap core.MapParam
	)
	err = multierr.Combine(
		errors.Wrap(core.ResolveParam(&lMap, core.From(core.VarExpr(t.Left, vars), core.JSONWithVarExprs(t.Left, vars, false), core.Input(inputs, 0))), "left-side"),
		errors.Wrap(core.ResolveParam(&rMap, core.From(core.VarExpr(t.Right, vars), core.JSONWithVarExprs(t.Right, vars, false))), "right-side"),
	)
	if err != nil {
		return core.Result{Error: err}, runInfo
	}

	// clobber lMap with rMap values
	// "nil" values on the right will clobber
	for key, value := range rMap {
		lMap[key] = value
	}

	return core.Result{Value: lMap.Map()}, runInfo
}
