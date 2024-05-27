package core

import (
	"context"
	"errors"
	commonconfig "github.com/smartcontractkit/chainlink-common/pkg/config"
	"github.com/smartcontractkit/chainlink-common/pkg/utils/jsonserializable"
	"go.uber.org/zap"
	"reflect"
	"sort"
	"time"

	"github.com/google/uuid"
	pkgerrors "github.com/pkg/errors"
	"gopkg.in/guregu/null.v4"
)

//go:generate mockery --quiet --name Config --output ./mocks/ --case=underscore

type (
	Task interface {
		Type() TaskType
		ID() int
		DotID() string
		Run(ctx context.Context, lggr *zap.Logger, vars Vars, inputs []Result) (Result, RunInfo)
		Base() *BaseTask
		Outputs() []Task
		Inputs() []TaskDependency
		OutputIndex() int32
		TaskTimeout() (time.Duration, bool)
		TaskRetries() uint32
		TaskMinBackoff() time.Duration
		TaskMaxBackoff() time.Duration
	}

	Config interface {
		DefaultHTTPLimit() int64
		DefaultHTTPTimeout() commonconfig.Duration
		MaxRunDuration() time.Duration
		ReaperInterval() time.Duration
		ReaperThreshold() time.Duration
		VerboseLogging() bool
	}
)

func NewDefaultConfig() *DefaultConfig {
	return &DefaultConfig{}
}

var _ Config = &DefaultConfig{}

type DefaultConfig struct {
}

func (d DefaultConfig) DefaultHTTPLimit() int64 {
	return 100
}

func (d DefaultConfig) DefaultHTTPTimeout() commonconfig.Duration {
	duration, err := commonconfig.NewDuration(time.Minute)
	if err != nil {
		panic(err)
	}
	return duration
}

func (d DefaultConfig) MaxRunDuration() time.Duration {
	return time.Minute
}

func (d DefaultConfig) ReaperInterval() time.Duration {
	return time.Minute * 5
}

func (d DefaultConfig) ReaperThreshold() time.Duration {
	return time.Minute * 5
}

func (d DefaultConfig) VerboseLogging() bool {
	return false
}

// Wraps the input Task for the given dependent task along with a bool variable PropagateResult,
// which Indicates whether result of InputTask should be propagated to its dependent task.
// If the edge between these tasks was an implicit edge, then results are not propagated. This is because
// some tasks cannot handle an input from an edge which wasn't specified in the spec.
type TaskDependency struct {
	PropagateResult bool
	InputTask       Task
}

var (
	ErrWrongInputCardinality = errors.New("wrong number of task inputs")
	ErrBadInput              = errors.New("bad input for task")
	ErrInputTaskErrored      = errors.New("input task errored")
	ErrParameterEmpty        = errors.New("parameter is empty")
	ErrIndexOutOfRange       = errors.New("index out of range")
	ErrTooManyErrors         = errors.New("too many errors")
	ErrTimeout               = errors.New("timeout")
	ErrTaskRunFailed         = errors.New("task run failed")
	ErrCancelled             = errors.New("task run cancelled (fail early)")
)

const (
	InputTaskKey = "input"
)

// RunInfo contains additional information about the finished TaskRun
type RunInfo struct {
	IsRetryable bool
	IsPending   bool
}

func isRetryableHTTPError(statusCode int, err error) bool {
	if statusCode >= 400 && statusCode < 500 {
		// Client errors are not likely to succeed by resubmitting the exact same information again
		return false
	} else if statusCode >= 500 {
		// Remote errors _might_ work on a retry
		return true
	}
	return err != nil
}

// Result is the result of a TaskRun
type Result struct {
	Value interface{}
	Error error
}

// OutputDB dumps a single result output for a pipeline_run or pipeline_task_run
func (result Result) OutputDB() jsonserializable.JSONSerializable {
	return jsonserializable.JSONSerializable{Val: result.Value, Valid: !(result.Value == nil || (reflect.ValueOf(result.Value).Kind() == reflect.Ptr && reflect.ValueOf(result.Value).IsNil()))}
}

// ErrorDB dumps a single result error for a pipeline_task_run
func (result Result) ErrorDB() null.String {
	var errString null.String
	if result.Error != nil {
		errString = null.StringFrom(result.Error.Error())
	}
	return errString
}

// FinalResult is the result of a Run
type FinalResult struct {
	Values      []interface{}
	AllErrors   []error
	FatalErrors []error
}

// HasFatalErrors returns true if the final result has any errors
func (result FinalResult) HasFatalErrors() bool {
	for _, err := range result.FatalErrors {
		if err != nil {
			return true
		}
	}
	return false
}

// HasErrors returns true if the final result has any errors
func (result FinalResult) HasErrors() bool {
	for _, err := range result.AllErrors {
		if err != nil {
			return true
		}
	}
	return false
}

func (result FinalResult) CombinedError() error {
	if !result.HasErrors() {
		return nil
	}
	return errors.Join(result.AllErrors...)
}

// SingularResult returns a single result if the FinalResult only has one set of outputs/errors
func (result FinalResult) SingularResult() (Result, error) {
	if len(result.FatalErrors) != 1 || len(result.Values) != 1 {
		return Result{}, pkgerrors.Errorf("cannot cast FinalResult to singular result; it does not have exactly 1 error and exactly 1 output: %#v", result)
	}
	return Result{Error: result.FatalErrors[0], Value: result.Values[0]}, nil
}

// TaskRunResult describes the result of a task run, suitable for database
// update or insert.
// ID might be zero if the TaskRun has not been inserted yet
// TaskSpecID will always be non-zero
type TaskRunResult struct {
	ID         uuid.UUID
	Task       Task    `json:"-"`
	TaskRun    TaskRun `json:"-"`
	Result     Result
	Attempts   uint
	CreatedAt  time.Time
	FinishedAt null.Time
	// runInfo is never persisted
	runInfo RunInfo
}

func (result *TaskRunResult) IsPending() bool {
	return !result.FinishedAt.Valid && result.Result == Result{}
}

func (result *TaskRunResult) IsTerminal() bool {
	return len(result.Task.Outputs()) == 0
}

// TaskRunResults represents a collection of results for all task runs for one pipeline run
type TaskRunResults []TaskRunResult

// GetTaskRunResultsFinishedAt returns latest finishedAt time from TaskRunResults.
func (trrs TaskRunResults) GetTaskRunResultsFinishedAt() time.Time {
	var finishedTime time.Time
	for _, trr := range trrs {
		if trr.FinishedAt.Valid && trr.FinishedAt.Time.After(finishedTime) {
			finishedTime = trr.FinishedAt.Time
		}
	}
	return finishedTime
}

// FinalResult pulls the FinalResult for the pipeline_run from the task runs
// It needs to respect the output index of each task
func (trrs TaskRunResults) FinalResult(l *zap.Logger) FinalResult {
	var found bool
	var fr FinalResult
	sort.Slice(trrs, func(i, j int) bool {
		return trrs[i].Task.OutputIndex() < trrs[j].Task.OutputIndex()
	})
	for _, trr := range trrs {
		fr.AllErrors = append(fr.AllErrors, trr.Result.Error)
		if trr.IsTerminal() {
			fr.Values = append(fr.Values, trr.Result.Value)
			fr.FatalErrors = append(fr.FatalErrors, trr.Result.Error)
			found = true
		}
	}

	if !found {
		l.Panic("Expected at least one task to be final", zap.Any("tasks", trrs))
	}
	return fr
}

// Terminals returns all terminal task run results
func (trrs TaskRunResults) Terminals() (terminals []TaskRunResult) {
	for _, trr := range trrs {
		if trr.IsTerminal() {
			terminals = append(terminals, trr)
		}
	}
	return
}

// GetNextTaskOf returns the task with the next id or nil if it does not exist
func (trrs *TaskRunResults) GetNextTaskOf(task TaskRunResult) *TaskRunResult {
	nextID := task.Task.Base().id + 1

	for _, trr := range *trrs {
		if trr.Task.Base().id == nextID {
			return &trr
		}
	}

	return nil
}

func CheckInputs(inputs []Result, minLen, maxLen, maxErrors int) ([]interface{}, error) {
	if minLen >= 0 && len(inputs) < minLen {
		return nil, pkgerrors.Wrapf(ErrWrongInputCardinality, "min: %v max: %v (got %v)", minLen, maxLen, len(inputs))
	} else if maxLen >= 0 && len(inputs) > maxLen {
		return nil, pkgerrors.Wrapf(ErrWrongInputCardinality, "min: %v max: %v (got %v)", minLen, maxLen, len(inputs))
	}
	var vals []interface{}
	var errs int
	for _, input := range inputs {
		if input.Error != nil {
			errs++
			continue
		}
		vals = append(vals, input.Value)
	}
	if maxErrors >= 0 && errs > maxErrors {
		return nil, ErrTooManyErrors
	}
	return vals, nil
}
