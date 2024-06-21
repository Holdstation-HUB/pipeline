package core

type Wrapper struct {
	run            *Run
	taskRunResults *TaskRunResults
	err            error
	runner         *runner
}

func (o *Wrapper) AllInOne() (*Run, *TaskRunResults, error) {
	return o.run, o.taskRunResults, o.err
}

func (o *Wrapper) GetTaskRunResults() *TaskRunResults {
	return o.taskRunResults
}

func (o *Wrapper) GetError() error {
	return o.err
}

func (o *Wrapper) GetRunner() *runner {
	return o.runner
}

func (o *Wrapper) GetRun() *Run {
	return o.run
}

func (o *Wrapper) execute(fns ...func(trs *TaskRunResults, err error)) *Wrapper {
	for _, fn := range fns {
		fn(o.taskRunResults, o.err)
	}
	return o
}
