package core

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"reflect"
	"sort"
	"time"

	"github.com/google/uuid"
	error2 "github.com/pkg/errors"
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
		DefaultHTTPTimeout() Duration
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

func (d DefaultConfig) DefaultHTTPTimeout() Duration {
	duration, err := NewDuration(time.Minute)
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

func IsRetryableHTTPError(statusCode int, err error) bool {
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
func (result Result) OutputDB() JSONSerializable {
	return JSONSerializable{Val: result.Value, Valid: !(result.Value == nil || (reflect.ValueOf(result.Value).Kind() == reflect.Ptr && reflect.ValueOf(result.Value).IsNil()))}
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

// Interval represents a time.Duration stored as a Postgres interval type
type Interval time.Duration

// NewInterval creates Interval for specified duration
func NewInterval(d time.Duration) *Interval {
	i := new(Interval)
	*i = Interval(d)
	return i
}

func (i Interval) Duration() time.Duration {
	return time.Duration(i)
}

// MarshalText implements the text.Marshaler interface.
func (i Interval) MarshalText() ([]byte, error) {
	return []byte(time.Duration(i).String()), nil
}

// UnmarshalText implements the text.Unmarshaler interface.
func (i *Interval) UnmarshalText(input []byte) error {
	v, err := time.ParseDuration(string(input))
	if err != nil {
		return err
	}
	*i = Interval(v)
	return nil
}

func (i *Interval) Scan(v interface{}) error {
	if v == nil {
		*i = Interval(time.Duration(0))
		return nil
	}
	asInt64, is := v.(int64)
	if !is {
		return error2.Errorf("models.Interval#Scan() wanted int64, got %T", v)
	}
	*i = Interval(time.Duration(asInt64) * time.Nanosecond)
	return nil
}

func (i Interval) Value() (driver.Value, error) {
	return time.Duration(i).Nanoseconds(), nil
}

func (i Interval) IsZero() bool {
	return time.Duration(i) == time.Duration(0)
}

// Duration is a non-negative time duration.
type Duration struct{ d time.Duration }

func NewDuration(d time.Duration) (Duration, error) {
	if d < time.Duration(0) {
		return Duration{}, fmt.Errorf("cannot make negative time duration: %s", d)
	}
	return Duration{d: d}, nil
}

func MustNewDuration(d time.Duration) *Duration {
	rv, err := NewDuration(d)
	if err != nil {
		panic(err)
	}
	return &rv
}

func ParseDuration(s string) (Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return Duration{}, err
	}

	return NewDuration(d)
}

func (d Duration) Duration() time.Duration {
	return d.d
}

// Before returns the time d units before time t
func (d Duration) Before(t time.Time) time.Time {
	return t.Add(-d.Duration())
}

// Shorter returns true if and only if d is shorter than od.
func (d Duration) Shorter(od Duration) bool { return d.d < od.d }

// IsInstant is true if and only if d is of duration 0
func (d Duration) IsInstant() bool { return d.d == 0 }

// String returns a string representing the duration in the form "72h3m0.5s".
// Leading zero units are omitted. As a special case, durations less than one
// second format use a smaller unit (milli-, micro-, or nanoseconds) to ensure
// that the leading digit is non-zero. The zero duration formats as 0s.
func (d Duration) String() string {
	return d.Duration().String()
}

// MarshalJSON implements the json.Marshaler interface.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (d *Duration) UnmarshalJSON(input []byte) error {
	var txt string
	err := json.Unmarshal(input, &txt)
	if err != nil {
		return err
	}
	v, err := time.ParseDuration(txt)
	if err != nil {
		return err
	}
	*d, err = NewDuration(v)
	if err != nil {
		return err
	}
	return nil
}

func (d *Duration) Scan(v interface{}) (err error) {
	switch tv := v.(type) {
	case int64:
		*d, err = NewDuration(time.Duration(tv))
		return err
	default:
		return fmt.Errorf(`don't know how to parse "%s" of type %T as a `+
			`models.Duration`, tv, tv)
	}
}

func (d Duration) Value() (driver.Value, error) {
	return int64(d.d), nil
}

// MarshalText implements the text.Marshaler interface.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.d.String()), nil
}

// UnmarshalText implements the text.Unmarshaler interface.
func (d *Duration) UnmarshalText(input []byte) error {
	v, err := time.ParseDuration(string(input))
	if err != nil {
		return err
	}
	pd, err := NewDuration(v)
	if err != nil {
		return err
	}
	*d = pd
	return nil
}
