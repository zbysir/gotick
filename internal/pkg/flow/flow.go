package flow

// node
// {
//    id: '1',
//    data: { label: 'Hello' },
//    position: { x: 0, y: 0 },
//    type: 'input',
//  }

type DAG struct {
	Nodes []*Node `json:"nodes"`
	Edges []*Edge `json:"edges"`
}

func (d *DAG) GetNodeLen(parentId string) int {
	var count int
	for _, n := range d.Nodes {
		if n.ParentNode == parentId {
			count++
		}
	}

	return count
}

func (d *DAG) Adjust() {
	for i := 0; i < len(d.Nodes); i++ {
		n, ok := d.GetNodeByIndex(i)
		if !ok {
			continue
		}

		if n.Type == InputNodeType {
			n.Position = DefaultPosition(0)
		} else {
			n.Position = DefaultPosition(d.GetNodeLen(n.ParentNode))
		}
	}
}
func (d *DAG) AppendNode(node Node, parentId string) {
	nodeLen := d.GetNodeLen(parentId)
	node.ParentNode = parentId
	node.Position = DefaultPosition(nodeLen)

	nodeTyp := DefaultNodeType
	if nodeLen == 0 {
		nodeTyp = InputNodeType
	}

	node.Type = nodeTyp

	d.Nodes = append(d.Nodes, &node)

	p, ok := d.GetNode(parentId)
	if ok {
		p.Children = append(p.Children, &node)

		//p.Style = map[string]interface{}{
		//	"width":  170,
		//	"height": 140,
		//}
	}
}

func (d DAG) GetNode(id string) (*Node, bool) {
	for _, d := range d.Nodes {
		if d.Id == id {
			return d, true
		}
	}

	return nil, false
}

func (d DAG) GetNodeByIndex(index int) (*Node, bool) {
	if len(d.Nodes) > index {
		return d.Nodes[index], true
	} else {
		return nil, false
	}
}

func (d *DAG) AppendEdges(e Edge) {
	d.Edges = append(d.Edges, &e)
}

type Node struct {
	Id         string      `json:"id"`
	Data       NodeData    `json:"data"`
	Position   Position    `json:"position"`
	Type       NodeType    `json:"type"`
	ParentNode string      `json:"parentNode,omitempty"`
	Style      interface{} `json:"style,omitempty"`

	Children []*Node `json:"-"`
}

func DefaultPosition(index int) Position {
	return Position{X: 0, Y: index * 100}
}

type NodeType string

// default, input, output, group
const (
	InputNodeType   NodeType = "input"
	OutputNodeType  NodeType = "output"
	DefaultNodeType NodeType = "default"
	GroupNodeType   NodeType = "group"
)

type NodeData struct {
	Label string      `json:"label"`
	Data  interface{} `json:"data,omitempty"`
}

// type XYPosition = {
//  x: number;
//  y: number;
//};

type Position struct {
	X int `json:"x"`
	Y int `json:"y"`
}

type Edge struct {
	Id        string      `json:"id"`
	Source    string      `json:"source"`
	Target    string      `json:"target"`
	MarkerEnd Marker      `json:"markerEnd"`
	Animated  bool        `json:"animated"`
	Label     string      `json:"label"`
	Data      interface{} `json:"data,omitempty"`
	Style     interface{} `json:"style,omitempty"`
}

type Marker struct {
	Type string `json:"type"`
}
