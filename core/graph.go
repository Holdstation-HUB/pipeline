package core

import (
	"fmt"
	"github.com/pkg/errors"
	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/encoding"
	"gonum.org/v1/gonum/graph/encoding/dot"
	"gonum.org/v1/gonum/graph/simple"
	"regexp"
	"sort"
	"strings"
)

// tree fulfills the graph.DirectedGraph interface, which makes it possible
// for us to `dot.Unmarshal(...)` a DOT string directly into it.
type Graph struct {
	*simple.DirectedGraph
}

func NewGraph() *Graph {
	return &Graph{DirectedGraph: simple.NewDirectedGraph()}
}

func (g *Graph) NewNode() graph.Node {
	return &GraphNode{Node: g.DirectedGraph.NewNode()}
}

func (g *Graph) NewEdge(from, to graph.Node) graph.Edge {
	return &GraphEdge{Edge: g.DirectedGraph.NewEdge(from, to)}
}

func (g *Graph) UnmarshalText(bs []byte) (err error) {
	if g.DirectedGraph == nil {
		g.DirectedGraph = simple.NewDirectedGraph()
	}
	defer func() {
		if rerr := recover(); rerr != nil {
			err = fmt.Errorf("could not unmarshal DOT into a pipeline.Graph: %v", rerr)
		}
	}()
	bs = append([]byte("digraph {\n"), bs...)
	bs = append(bs, []byte("\n}")...)
	err = dot.Unmarshal(bs, g)
	if err != nil {
		return errors.Wrap(err, "could not unmarshal DOT into a pipeline.Graph")
	}
	g.AddImplicitDependenciesAsEdges()
	return nil
}

// Looks at node attributes and searches for implicit dependencies on other nodes
// expressed as attribute values. Adds those dependencies as implicit edges in the graph.
func (g *Graph) AddImplicitDependenciesAsEdges() {
	for nodesIter := g.Nodes(); nodesIter.Next(); {
		graphNode := nodesIter.Node().(*GraphNode)

		params := make(map[string]bool)
		// Walk through all attributes and find all Params which this node depends on
		for _, attr := range graphNode.Attributes() {
			for _, item := range VariableRegexp.FindAll([]byte(attr.Value), -1) {
				expr := strings.TrimSpace(string(item[2 : len(item)-1]))
				param := strings.Split(expr, ".")[0]
				params[param] = true
			}
		}
		// Iterate through all nodes and add a new edge if node belongs to Params set, and there already isn't an edge.
		for nodesIter2 := g.Nodes(); nodesIter2.Next(); {
			gn := nodesIter2.Node().(*GraphNode)
			if params[gn.DOTID()] {
				// If these are distinct nodes with no existing edge between them, then add an implicit edge.
				if gn.ID() != graphNode.ID() && !g.HasEdgeFromTo(gn.ID(), graphNode.ID()) {
					edge := g.NewEdge(gn, graphNode).(*GraphEdge)
					// Setting isImplicit indicates that this edge wasn't specified via the TOML spec,
					// but rather added automatically here.
					// This distinction is needed, as we don't want to propagate results of a task to its dependent
					// tasks along implicit edge, as some tasks can't handle unexpected inputs from implicit edges.
					edge.SetIsImplicit(true)
					g.SetEdge(edge)
				}
			}
		}
	}
}

// Indicates whether there's an implicit edge from uid -> vid.
// Implicit edged are ones that weren't added via the TOML spec, but via the pipeline parsing code
func (g *Graph) IsImplicitEdge(uid, vid int64) bool {
	edge := g.Edge(uid, vid).(*GraphEdge)
	if edge == nil {
		return false
	}
	return edge.IsImplicit()
}

type GraphEdge struct {
	graph.Edge

	// Indicates that this edge was implicitly added by the pipeline parser, and not via the TOML specs.
	isImplicit bool
}

func (e *GraphEdge) IsImplicit() bool {
	return e.isImplicit
}

func (e *GraphEdge) SetIsImplicit(isImplicit bool) {
	e.isImplicit = isImplicit
}

type GraphNode struct {
	graph.Node
	dotID string
	attrs map[string]string
}

func (n *GraphNode) DOTID() string {
	return n.dotID
}

func (n *GraphNode) SetDOTID(id string) {
	n.dotID = id
}

func (n *GraphNode) String() string {
	return n.dotID
}

var bracketQuotedAttrRegexp = regexp.MustCompile(`\A\s*<([^<>]+)>\s*\z`)

func (n *GraphNode) SetAttribute(attr encoding.Attribute) error {
	if n.attrs == nil {
		n.attrs = make(map[string]string)
	}

	// Strings quoted in angle brackets (supported natively by DOT) should
	// have those brackets removed before decoding to task parameter types
	sanitized := bracketQuotedAttrRegexp.ReplaceAllString(attr.Value, "$1")

	n.attrs[attr.Key] = sanitized
	return nil
}

func (n *GraphNode) Attributes() []encoding.Attribute {
	var r []encoding.Attribute
	for k, v := range n.attrs {
		r = append(r, encoding.Attribute{Key: k, Value: v})
	}
	// Ensure the slice returned is deterministic.
	sort.Slice(r, func(i, j int) bool {
		return r[i].Key < r[j].Key
	})
	return r
}
