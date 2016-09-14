package gmaj

import "testing"

// Generally useful testing helper functions. Creates three successive nodes
// with ids 0 (node1), 10 (node2) and 20 (node3).
func create3SuccessiveNodes(t *testing.T) (*Node, *Node, *Node) {
	definedID := make([]byte, cfg.IDLength)
	node1 := createDefinedNode(t, nil, definedID)
	definedID = make([]byte, cfg.IDLength)
	definedID[0] = 55
	node2 := createDefinedNode(t, node1.RemoteNode(), definedID)
	definedID = make([]byte, cfg.IDLength)
	definedID[0] = 0xaa
	node3 := createDefinedNode(t, node1.RemoteNode(), definedID)
	return node1, node2, node3
}

func createSimpleNode(t *testing.T, ring *RemoteNode) *Node {
	return createDefinedNode(t, ring, nil)
}

func createDefinedNode(t *testing.T, ring *RemoteNode, id []byte) *Node {
	node, err := NewDefinedNode(ring, id)
	if err != nil {
		t.Fatalf("Unable to create node, received error:%v", err)
	}
	return node
}

// Helper for GetSuccessor tests. Issues an RPC to check if node2 is a successor
// of node1.
func assertSuccessor(t *testing.T, node1, node2 *Node) {
	if remoteNode, err := GetSuccessorRPC(node1.RemoteNode()); err != nil {
		t.Fatalf("Unexpected error:%v", err)
	} else if remoteNode.Addr != node2.Addr() {
		t.Fatalf(
			"Unexpected successor. Expected %v got %v",
			node2.remoteNode,
			remoteNode,
		)
	}
}

// Helper for FindSuccessor tests. Issues an RPC to check that node is id's
// successor.
func assertSuccessorID(t *testing.T, id byte, node *Node) {
	if remoteNode, err := FindSuccessorRPC(node.RemoteNode(), []byte{id}); err != nil {
		t.Fatalf("Unexpected error:%v", err)
	} else if remoteNode.Addr != node.Addr() {
		t.Fatalf("Unexpected successor. Expected %v got %v",
			node.remoteNode,
			remoteNode)
	}
}

// Helper for between tests.
func create3Points(x, a, b byte) ([]byte, []byte, []byte) {
	return []byte{x}, []byte{a}, []byte{b}
}

func assertBetweenFuncSlices(
	t *testing.T, betweenFunc func([]byte, []byte, []byte) bool, x, a, b []byte) {
	if !betweenFunc(x, a, b) {
		t.Fatalf("%d is not between %d and %d.", x, a, b)
	}
}

func assertNotBetweenFuncSlices(
	t *testing.T, betweenFunc func([]byte, []byte, []byte) bool, x, a, b []byte) {
	if betweenFunc(x, a, b) {
		t.Fatalf("%d is between %d and %d.", x, a, b)
	}
}

// Same as above but with bytes rather than byte slices.
func assertBetweenFunc(
	t *testing.T, betweenFunc func([]byte, []byte, []byte) bool, x, a, b byte) {
	slice1, slice2, slice3 := create3Points(x, a, b)
	assertBetweenFuncSlices(t, betweenFunc, slice1, slice2, slice3)
}

func assertNotBetweenFunc(
	t *testing.T, betweenFunc func([]byte, []byte, []byte) bool, x, a, b byte) {
	slice1, slice2, slice3 := create3Points(x, a, b)
	assertNotBetweenFuncSlices(t, betweenFunc, slice1, slice2, slice3)
}

// Finger test helper functions. This assertion tests that node.FingerTable
// points to node2 at entry i.
func assertFingerTable(t *testing.T, node *Node, i int, node2 *Node) {
	if want, got := node2.Addr(), node.fingerTable[i].RemoteNode.Addr; got != want {
		t.Errorf("Expected %v, got %v", want, got)
	}
}

// Helper for closest preceding finger. Asserts that closest is the closest
// preceding finger to id according to node.
func assertClosest(t *testing.T, node, closest *Node, id byte) {
	remoteNode, err := ClosestPrecedingFingerRPC(node.RemoteNode(), []byte{id})
	if err != nil {
		t.Fatalf("Unexpected error while getting closest:%v", err)
	} else if remoteNode.Addr != closest.Addr() {
		t.Fatalf("Expected %v, got %v", closest.ID(), remoteNode.Id)
	}
}

func assertCloseBombardment(t *testing.T, rangeStart, rangeEnd int, nodes []*Node, closest *Node) {
	for i := rangeStart; i <= rangeEnd; i++ {
		for _, node := range nodes {
			assertClosest(t, node, closest, byte(i))
		}
	}
}
