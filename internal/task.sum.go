package internal

import (
	"context"
	"github.com/Holdstation-HUB/pipeline/core"
	"go.uber.org/zap"

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"go.uber.org/multierr"
)

const TaskTypeSum core.TaskType = "sum"

// Return types:
//
//	*decimal.Decimal
type SumTask struct {
	core.BaseTask `mapstructure:",squash"`
	Values        string `json:"values"`
	AllowedFaults string `json:"allowedFaults"`
}

var _ core.Task = (*SumTask)(nil)

func (t *SumTask) Type() core.TaskType {
	return TaskTypeSum
}

func (t *SumTask) Run(_ context.Context, _ *zap.Logger, vars core.Vars, inputs []core.Result) (result core.Result, runInfo core.RunInfo) {
	var (
		maybeAllowedFaults core.MaybeUint64Param
		valuesAndErrs      core.SliceParam
		decimalValues      core.DecimalSliceParam
		allowedFaults      int
		faults             int
	)
	err := multierr.Combine(
		errors.Wrap(core.ResolveParam(&maybeAllowedFaults, core.From(t.AllowedFaults)), "allowedFaults"),
		errors.Wrap(core.ResolveParam(&valuesAndErrs, core.From(core.VarExpr(t.Values, vars), core.JSONWithVarExprs(t.Values, vars, true), core.Inputs(inputs))), "values"),
	)
	if err != nil {
		return core.Result{Error: err}, runInfo
	}

	if allowed, isSet := maybeAllowedFaults.Uint64(); isSet {
		allowedFaults = int(allowed)
	} else {
		allowedFaults = len(valuesAndErrs) - 1
	}

	values, faults := valuesAndErrs.FilterErrors()
	if faults > allowedFaults {
		return core.Result{Error: errors.Wrapf(core.ErrTooManyErrors, "Number of faulty inputs %v to sum task > number allowed faults %v", faults, allowedFaults)}, runInfo
	} else if len(values) == 0 {
		return core.Result{Error: errors.Wrap(core.ErrWrongInputCardinality, "values")}, runInfo
	}

	err = decimalValues.UnmarshalPipelineParam(values)
	if err != nil {
		return core.Result{Error: errors.Wrapf(core.ErrBadInput, "values: %v", err)}, runInfo
	}

	sum := decimal.NewFromInt(0)
	for _, val := range decimalValues {
		sum = sum.Add(val)
	}
	return core.Result{Value: sum}, runInfo
}
