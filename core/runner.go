package core

import (
	"context"
	"fmt"
	"github.com/smartcontractkit/chainlink-common/pkg/utils/jsonserializable"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	pkgerrors "github.com/pkg/errors"
	"gopkg.in/guregu/null.v4"

	"github.com/smartcontractkit/chainlink-common/pkg/services"
	"github.com/smartcontractkit/chainlink-common/pkg/sqlutil"
	commonutils "github.com/smartcontractkit/chainlink-common/pkg/utils"

	"github.com/smartcontractkit/chainlink/v2/core/config/env"
	"github.com/smartcontractkit/chainlink/v2/core/store/models"
	"github.com/smartcontractkit/chainlink/v2/core/utils"
)

//go:generate mockery --quiet --name Runner --output ./mocks/ --case=underscore

type Runner = *runner

type RunnerInf interface {
	services.Service

	// Run is a blocking call that will execute the run until no further progress can be made.
	// If `incomplete` is true, the run is only partially complete and is suspended, awaiting to be resumed when more data comes in.
	// Note that `saveSuccessfulTaskRuns` value is ignored if the run contains async tasks.
	Run(ctx context.Context, run *Run, l *zap.Logger, saveSuccessfulTaskRuns bool, fn func(tx sqlutil.DataSource) error) (incomplete bool, err error)

	// ExecuteRun executes a new run in-memory according to a spec and returns the results.
	// We expect spec.JobID and spec.JobName to be set for logging/prometheus.
	ExecuteRun(ctx context.Context, spec Spec, vars Vars, l *zap.Logger) (run *Run, trrs TaskRunResults, err error)

	// ExecuteAndInsertFinishedRun executes a new run in-memory according to a spec, persists and saves the results.
	// It is a combination of ExecuteRun and InsertFinishedRun.
	// Note that the spec MUST have a DOT graph for this to work.
	// This will persist the Spec in the DB if it doesn't have an ID.
	ExecuteAndInsertFinishedRun(ctx context.Context, spec Spec, vars Vars, l *zap.Logger, saveSuccessfulTaskRuns bool) (runID int64, results TaskRunResults, err error)

	OnRunFinished(func(*Run))
	InitializePipeline(spec Spec) (*Pipeline, error)
}

type runner struct {
	services.StateMachine
	config                 Config
	runReaperWorker        *commonutils.SleeperTask
	lggr                   *zap.Logger
	httpClient             *http.Client
	unrestrictedHTTPClient *http.Client
	db                     *gorm.DB

	// test helper
	runFinished func(*Run)

	chStop services.StopChan
	wgDone sync.WaitGroup
}

func NewRunner(cfg Config, lggr *zap.Logger, httpClient, unrestrictedHTTPClient *http.Client, db *gorm.DB) Runner {
	r := &runner{
		config:                 cfg,
		chStop:                 make(chan struct{}),
		wgDone:                 sync.WaitGroup{},
		runFinished:            func(*Run) {},
		lggr:                   lggr.Named("PipelineRunner"),
		httpClient:             httpClient,
		unrestrictedHTTPClient: unrestrictedHTTPClient,
		db:                     db,
	}
	r.runReaperWorker = commonutils.NewSleeperTask(
		commonutils.SleeperFuncTask(r.runReaper, "PipelineRunnerReaper"),
	)
	return r
}

// Start starts Runner.
func (r *runner) Start(context.Context) error {
	return r.StartOnce("PipelineRunner", func() error {
		r.wgDone.Add(1)
		go r.scheduleUnfinishedRuns()
		if r.config.ReaperInterval() != time.Duration(0) {
			r.wgDone.Add(1)
			go r.runReaperLoop()
		}
		return nil
	})
}

func (r *runner) Close() error {
	return r.StopOnce("PipelineRunner", func() error {
		close(r.chStop)
		r.wgDone.Wait()
		return nil
	})
}

func (r *runner) Name() string {
	return r.lggr.Name()
}

func (r *runner) HealthReport() map[string]error {
	return map[string]error{r.Name(): r.Healthy()}
}

func (r *runner) destroy() {
	err := r.runReaperWorker.Stop()
	if err != nil {
		r.lggr.Error(err.Error())
	}
}

func (r *runner) runReaperLoop() {
	defer r.wgDone.Done()
	defer r.destroy()
	if r.config.ReaperInterval() == 0 {
		return
	}

	runReaperTicker := time.NewTicker(utils.WithJitter(r.config.ReaperInterval()))
	defer runReaperTicker.Stop()
	for {
		select {
		case <-r.chStop:
			return
		case <-runReaperTicker.C:
			r.runReaperWorker.WakeUp()
			runReaperTicker.Reset(utils.WithJitter(r.config.ReaperInterval()))
		}
	}
}

type memoryTaskRun struct {
	task     Task
	inputs   []Result // sorted by input index
	vars     Vars
	attempts uint
}

// When a task panics, we catch the panic and wrap it in an error for reporting to the scheduler.
type ErrRunPanicked struct {
	v interface{}
}

func (err ErrRunPanicked) Error() string {
	return fmt.Sprintf("goroutine panicked when executing run: %v", err.v)
}

func NewRun(spec Spec, vars Vars) *Run {
	return &Run{
		State:          RunStatusRunning,
		JobID:          spec.JobID,
		PruningKey:     spec.JobID,
		PipelineSpec:   spec,
		PipelineSpecID: spec.ID,
		Inputs:         jsonserializable.JSONSerializable{Val: vars.vars, Valid: true},
		Outputs:        jsonserializable.JSONSerializable{Val: nil, Valid: false},
		CreatedAt:      time.Now(),
	}
}

func (r *runner) OnRunFinished(fn func(*Run)) {
	r.runFinished = fn
}

// github.com/smartcontractkit/libocr/offchainreporting2plus/internal/protocol.ReportingPluginTimeoutWarningGracePeriod
var overtime = 100 * time.Millisecond

func init() {
	// undocumented escape hatch
	if v := env.PipelineOvertime.Get(); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			overtime = d
		}
	}
}

func (r *runner) ExecuteRun(
	ctx context.Context,
	spec Spec,
	vars Vars,
	l *zap.Logger,
) (*Run, TaskRunResults, error) {
	// Pipeline runs may return results after the context is cancelled, so we modify the
	// deadline to give them time to return before the parent context deadline.
	var cancel func()
	ctx, cancel = commonutils.ContextWithDeadlineFn(ctx, func(orig time.Time) time.Time {
		if tenPct := time.Until(orig) / 10; overtime > tenPct {
			return orig.Add(-tenPct)
		}
		return orig.Add(-overtime)
	})
	defer cancel()

	var pipeline *Pipeline
	if spec.Pipeline != nil {
		// assume if set that it has been pre-initialized
		pipeline = spec.Pipeline
	} else {
		var err error
		pipeline, err = r.InitializePipeline(spec)
		if err != nil {
			return nil, nil, err
		}
	}

	run := NewRun(spec, vars)
	taskRunResults := r.run(ctx, pipeline, run, vars, l)

	if run.Pending {
		return run, nil, fmt.Errorf("unexpected async run for spec ID %v, tried executing via ExecuteRun", spec.ID)
	}

	return run, taskRunResults, nil
}

func (r *runner) InitializePipeline(spec Spec) (pipeline *Pipeline, err error) {
	pipeline, err = spec.GetOrParsePipeline()
	if err != nil {
		return
	}

	// initialize certain task Params
	for _, task := range pipeline.Tasks {
		task.Base().uuid = uuid.New()

		switch task.Type() {
		case TaskTypeHTTP:
			task.(*HTTPTask).config = r.config
			task.(*HTTPTask).httpClient = r.httpClient
			task.(*HTTPTask).unrestrictedHTTPClient = r.unrestrictedHTTPClient
		case TaskTypeQueryDB:
			task.(*QueryDBTask).db = r.db
		default:
		}
	}

	return pipeline, nil
}

func (r *runner) run(ctx context.Context, pipeline *Pipeline, run *Run, vars Vars, l *zap.Logger) TaskRunResults {
	l.Debug("Initiating tasks for pipeline run of spec", zap.Int64("run.ID", run.ID),
		zap.String("executionID", uuid.NewString()), zap.Int32("specID", run.PipelineSpecID),
		zap.Int32("jobID", run.PipelineSpec.JobID), zap.String("jobName", run.PipelineSpec.JobName))

	scheduler := newScheduler(pipeline, run, vars, l)
	go scheduler.Run()

	// This is "just in case" for cleaning up any stray reports.
	// Normally the scheduler loop doesn't stop until all in progress runs report back
	reportCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if pipelineTimeout := r.config.MaxRunDuration(); pipelineTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, pipelineTimeout)
		defer cancel()
	}

	for taskRun := range scheduler.taskCh {
		taskRun := taskRun
		// execute
		go WrapRecoverHandle(l, func() {
			result := r.executeTaskRun(ctx, run.PipelineSpec, taskRun, l)
			scheduler.report(reportCtx, result)
		}, func(err interface{}) {
			t := time.Now()
			scheduler.report(reportCtx, TaskRunResult{
				ID:         uuid.New(),
				Task:       taskRun.task,
				Result:     Result{Error: ErrRunPanicked{err}},
				FinishedAt: null.TimeFrom(t),
				CreatedAt:  t, // TODO: more accurate start time
			})
		})
	}

	// if the run is suspended, awaiting resumption
	run.Pending = scheduler.pending
	// scheduler.exiting = we had an error and the task was marked to failEarly
	run.FailSilently = scheduler.exiting
	run.State = RunStatusSuspended

	var runTime time.Duration
	if !scheduler.pending {
		run.FinishedAt = null.TimeFrom(time.Now())

		// NOTE: runTime can be very long now because it'll include suspend
		runTime = run.FinishedAt.Time.Sub(run.CreatedAt)
	}

	// Update run results
	run.PipelineTaskRuns = nil
	for _, result := range scheduler.results {
		output := result.Result.OutputDB()
		run.PipelineTaskRuns = append(run.PipelineTaskRuns, TaskRun{
			ID:            result.ID,
			PipelineRunID: run.ID,
			Type:          result.Task.Type(),
			Index:         result.Task.OutputIndex(),
			Output:        output,
			Error:         result.Result.ErrorDB(),
			DotID:         result.Task.DotID(),
			CreatedAt:     result.CreatedAt,
			FinishedAt:    result.FinishedAt,
			task:          result.Task,
		})

		sort.Slice(run.PipelineTaskRuns, func(i, j int) bool {
			if run.PipelineTaskRuns[i].task.OutputIndex() == run.PipelineTaskRuns[j].task.OutputIndex() {
				return run.PipelineTaskRuns[i].FinishedAt.ValueOrZero().Before(run.PipelineTaskRuns[j].FinishedAt.ValueOrZero())
			}
			return run.PipelineTaskRuns[i].task.OutputIndex() < run.PipelineTaskRuns[j].task.OutputIndex()
		})
	}

	// Update run errors/outputs
	if run.FinishedAt.Valid {
		var errors []null.String
		var fatalErrors []null.String
		var outputs []interface{}
		for _, result := range run.PipelineTaskRuns {
			if result.Error.Valid {
				errors = append(errors, result.Error)
			}
			// skip non-terminal results
			if len(result.task.Outputs()) != 0 {
				continue
			}
			fatalErrors = append(fatalErrors, result.Error)
			outputs = append(outputs, result.Output.Val)
		}
		run.AllErrors = errors
		run.FatalErrors = fatalErrors
		run.Outputs = jsonserializable.JSONSerializable{Val: outputs, Valid: true}

		if run.HasFatalErrors() {
			run.State = RunStatusErrored
		} else {
			run.State = RunStatusCompleted
		}
	}

	// TODO: drop this once we stop using TaskRunResults
	var taskRunResults TaskRunResults
	for _, result := range scheduler.results {
		taskRunResults = append(taskRunResults, result)
	}

	var idxs []int32
	for i := range taskRunResults {
		idxs = append(idxs, taskRunResults[i].Task.OutputIndex())
	}
	// Ensure that task run results are ordered by their output index
	sort.SliceStable(taskRunResults, func(i, j int) bool {
		return taskRunResults[i].Task.OutputIndex() < taskRunResults[j].Task.OutputIndex()
	})
	for i := range taskRunResults {
		idxs[i] = taskRunResults[i].Task.OutputIndex()
	}

	if r.config.VerboseLogging() {
		l = l.With(
			zap.Any("run.PipelineTaskRuns", run.PipelineTaskRuns),
			zap.Any("run.Outputs", run.Outputs),
			zap.Time("run.CreatedAt", run.CreatedAt),
			zap.Any("run.FinishedAt", run.FinishedAt),
			zap.Any("run.Meta", run.Meta),
			zap.Any("run.Inputs", run.Inputs),
		)
	}
	l = l.With(zap.Any("run.State", run.State), zap.Bool("fatal", run.HasFatalErrors()), zap.Duration("runTime", runTime))
	if run.HasFatalErrors() {
		// This will also log at error level in OCR if it fails Observe so the
		// level is appropriate
		l = l.With(zap.Any("run.FatalErrors", run.FatalErrors))

		l.Debug("Completed pipeline run with fatal errors")
	} else if run.HasErrors() {
		l = l.With(zap.Any("run.AllErrors", run.AllErrors))
		l.Debug("Completed pipeline run with errors")
	} else {
		l.Debug("Completed pipeline run successfully")
	}

	return taskRunResults
}

func (r *runner) executeTaskRun(ctx context.Context, spec Spec, taskRun *memoryTaskRun, l *zap.Logger) TaskRunResult {
	start := time.Now()
	l = l.With(zap.String("taskName", taskRun.task.DotID()),
		zap.Any("taskType", taskRun.task.Type()),
		zap.Uint("attempt", taskRun.attempts))

	// Task timeout will be whichever of the following timesout/cancels first:
	// - Pipeline-level timeout
	// - Specific task timeout (task.TaskTimeout)
	// - Job level task timeout (spec.MaxTaskDuration)
	// - Passed in context

	// CAUTION: Think twice before changing any of the context handling code
	// below. It has already been changed several times trying to "fix" a bug,
	// but actually introducing new ones. Please leave it as-is unless you have
	// an extremely good reason to change it.
	ctx, cancel := r.chStop.Ctx(ctx)
	defer cancel()
	if taskTimeout, isSet := taskRun.task.TaskTimeout(); isSet && taskTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, taskTimeout)
		defer cancel()
	}
	if spec.MaxTaskDuration != models.Interval(time.Duration(0)) {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(spec.MaxTaskDuration))
		defer cancel()
	}

	result, runInfo := taskRun.task.Run(ctx, l, taskRun.vars, taskRun.inputs)
	loggerFields := []zap.Field{zap.Any("runInfo", runInfo),
		zap.Any("resultValue", result.Value),
		zap.Errors("resultError", []error{result.Error}),
		zap.String("resultType", fmt.Sprintf("%T", result.Value)),
	}
	switch v := result.Value.(type) {
	case []byte:
		loggerFields = append(loggerFields, zap.String("resultString", fmt.Sprintf("%q", v)))
		loggerFields = append(loggerFields, zap.String("resultHex", fmt.Sprintf("%x", v)))
	}
	if r.config.VerboseLogging() {
		l.Info("Pipeline task completed", loggerFields...)
	}

	now := time.Now()

	var finishedAt null.Time
	if !runInfo.IsPending {
		finishedAt = null.TimeFrom(now)
	}
	return TaskRunResult{
		ID:         taskRun.task.Base().uuid,
		Task:       taskRun.task,
		Result:     result,
		CreatedAt:  start,
		FinishedAt: finishedAt,
		runInfo:    runInfo,
	}
}

// ExecuteAndInsertFinishedRun executes a run in memory then inserts the finished run/task run records, returning the final result
func (r *runner) ExecuteAndInsertFinishedRun(ctx context.Context, spec Spec, vars Vars, l *zap.Logger, saveSuccessfulTaskRuns bool) (runID int64, results TaskRunResults, err error) {
	run, trrs, err := r.ExecuteRun(ctx, spec, vars, l)
	if err != nil {
		return 0, trrs, pkgerrors.Wrapf(err, "error executing run for spec ID %v", spec.ID)
	}

	// don't insert if we exited early
	if run.FailSilently {
		return 0, trrs, nil
	}

	// TODO: maybe insert to db for tracing in feature
	return run.ID, trrs, nil
}

func (r *runner) Run(ctx context.Context, run *Run, l *zap.Logger, saveSuccessfulTaskRuns bool, fn func(tx sqlutil.DataSource) error) (incomplete bool, err error) {
	pipeline, err := r.InitializePipeline(run.PipelineSpec)
	if err != nil {
		return false, err
	}

	// retain old UUID values
	for _, taskRun := range run.PipelineTaskRuns {
		task := pipeline.ByDotID(taskRun.DotID)
		if task == nil || task.Base() == nil {
			return false, pkgerrors.Errorf("failed to match a pipeline task for dot ID: %v", taskRun.DotID)
		}
		task.Base().uuid = taskRun.ID
	}

	for {
		r.run(ctx, pipeline, run, NewVarsFrom(run.Inputs.Val.(map[string]interface{})), l)
		if run.Pending {
			return false, pkgerrors.Wrapf(err, "a run without async returned as pending")
		}
		// don't insert if we exited early
		if run.FailSilently {
			return false, nil
		}

		r.runFinished(run)

		return run.Pending, err
	}
}

func (r *runner) runReaper() {
	r.lggr.Debug("Pipeline run reaper starting")
	_, cancel := r.chStop.CtxCancel(context.WithTimeout(context.Background(), r.config.ReaperInterval()))
	defer cancel()
}

// init task: Searches the database for runs stuck in the 'running' state while the node was previously killed.
// We pick up those runs and resume execution.
func (r *runner) scheduleUnfinishedRuns() {
	defer r.wgDone.Done()

	// limit using a createdAt < now() @ start of run to prevent executing new jobs
	//now := time.Now()

	if r.config.ReaperInterval() > time.Duration(0) {
		// immediately run reaper so we don't consider runs that are too old
		r.runReaper()
	}

	ctx, cancel := r.chStop.NewCtx()
	defer cancel()

	var wgRunsDone sync.WaitGroup

	wgRunsDone.Wait()

	if ctx.Err() != nil {
		return
	}
}
