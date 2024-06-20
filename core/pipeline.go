package core

import (
	"github.com/pkg/errors"
	"gonum.org/v1/gonum/graph/topo"
	"sort"
	"strings"
)

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

//func (p *Pipeline) MinTimeout() (time.Duration, bool, error) {
//	var minTimeout time.Duration = 1<<63 - 1
//	var aTimeoutSet bool
//	for _, t := range p.Tasks {
//		if timeout, set := t.TaskTimeout(); set && timeout < minTimeout {
//			minTimeout = timeout
//			aTimeoutSet = true
//		}
//	}
//	return minTimeout, aTimeoutSet, nil
//}

func (p *Pipeline) ByDotID(id string) Task {
	for _, task := range p.Tasks {
		if task.DotID() == id {
			return task
		}
	}
	return nil
}

type PipelineParser func(text string) (*Pipeline, error)

func Parse(generatingTask GenerateTask, text string) (*Pipeline, error) {
	if strings.TrimSpace(text) == "" {
		return nil, errors.New("empty pipeline")
	}
	g := NewGraph()
	err := g.UnmarshalText([]byte(text))

	if err != nil {
		return nil, err
	}

	p := &Pipeline{
		tree:   g,
		Tasks:  make([]Task, 0, g.Nodes().Len()),
		Source: text,
	}

	// toposort all the nodes: dependencies ordered before outputs. This also does cycle checking for us.
	nodes, err := topo.SortStabilized(g, nil)

	if err != nil {
		return nil, errors.Wrap(err, "Unable to topologically sort the graph, cycle detected")
	}

	// we need a temporary mapping of graph.IDs to positional ids after toposort
	ids := make(map[int64]int)

	// use the new ordering as the id so that we can easily reproduce the original toposort
	for id, node := range nodes {
		node, is := node.(*GraphNode)
		if !is {
			panic("unreachable")
		}

		if node.dotID == InputTaskKey {
			return nil, errors.Errorf("'%v' is a reserved keyword that cannot be used as a task's name", InputTaskKey)
		}

		task, err := UnmarshalTaskFromMap(generatingTask, TaskType(node.attrs["type"]), node.attrs, id, node.dotID)
		if err != nil {
			return nil, err
		}

		// re-link the edges
		for inputs := g.To(node.ID()); inputs.Next(); {
			isImplicitEdge := g.IsImplicitEdge(inputs.Node().ID(), node.ID())
			from := p.Tasks[ids[inputs.Node().ID()]]

			from.Base().outputs = append(from.Base().outputs, task)
			task.Base().inputs = append(task.Base().inputs, TaskDependency{!isImplicitEdge, from})
		}

		// This is subtle: g.To doesn't return nodes in deterministic order, which would occasionally swap the order
		// of inputs, therefore we manually sort. We don't need to sort outputs the same way because these appends happen
		// in p.Task order, which is deterministic via topo.SortStable.
		sort.Slice(task.Base().inputs, func(i, j int) bool {
			return task.Base().inputs[i].InputTask.ID() < task.Base().inputs[j].InputTask.ID()
		})

		p.Tasks = append(p.Tasks, task)
		ids[node.ID()] = id
	}

	return p, nil
}
