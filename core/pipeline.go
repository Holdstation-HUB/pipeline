package core

import "time"

type Pipeline struct {
	Tasks  []Task
	tree   *Graph
	Source string
}

//func (p *Pipeline) UnmarshalText(bs []byte) (err error) {
//	parsed, err := Parse(string(bs))
//	if err != nil {
//		return err
//	}
//	*p = *parsed
//	return nil
//}

func (p *Pipeline) MinTimeout() (time.Duration, bool, error) {
	var minTimeout time.Duration = 1<<63 - 1
	var aTimeoutSet bool
	for _, t := range p.Tasks {
		if timeout, set := t.TaskTimeout(); set && timeout < minTimeout {
			minTimeout = timeout
			aTimeoutSet = true
		}
	}
	return minTimeout, aTimeoutSet, nil
}

func (p *Pipeline) ByDotID(id string) Task {
	for _, task := range p.Tasks {
		if task.DotID() == id {
			return task
		}
	}
	return nil
}
