package gmaj

import (
	"testing"
)

// // Generally useful testing helper functions. Creates three successive nodes
// // with ids 0 (node1), 10 (node2) and 20 (node3).
// func create3SuccessiveNodes(t *testing.T) (*Node, *Node, *Node) {
// 	definedId := make([]byte, KeyLength/8)
// 	node1 := createDefinedNode(t, nil, definedId)
// 	definedId[0] += 10
// 	node2 := createDefinedNode(t, node1.RemoteNode, definedId)
// 	definedId[0] += 10
// 	node3 := createDefinedNode(t, node1.RemoteNode, definedId)
// 	return node1, node2, node3
// }

// func createSimpleNode(t *testing.T, ring *RemoteNode) *Node {
// 	return createDefinedNode(t, ring, nil)
// }

// func createDefinedNode(t *testing.T, ring *RemoteNode, id []byte) *Node {
// 	node, err := CreateDefinedNode(ring, id)
// 	if err != nil {
// 		t.Fatalf("Unable to create node, received error:%v\n", err)
// 	}
// 	return node
// }

// // Helper for GetSuccessor tests. Issues an RPC to check if node2 is a successor
// // of node1.
// func assertSuccessor(t *testing.T, node1, node2 *Node) {
// 	if remoteNode, err := GetSuccessorId_RPC(node1.RemoteNode); err != nil {
// 		t.Fatalf("Unexpected error:%v", err)
// 	} else if remoteNode.Addr != node2.RemoteNode.Addr {
// 		t.Fatalf("Unexpected successor. Expected %v got %v",
// 			node2.RemoteNode,
// 			remoteNode)
// 	}
// }

// // Helper for FindSuccessor tests. Issues an RPC to check that node is id's
// // successor.
// func assertSuccessorId(t *testing.T, id byte, node *Node) {
// 	if remoteNode, err := FindSuccessor_RPC(node.RemoteNode, []byte{id}); err != nil {
// 		t.Fatalf("Unexpected error:%v", err)
// 	} else if remoteNode.Addr != node.RemoteNode.Addr {
// 		t.Fatalf("Unexpected successor. Expected %v got %v",
// 			node.RemoteNode,
// 			remoteNode)
// 	}
// }

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

// // Finger test helper functions. This assertion tests that node.FingerTable
// // points to node2 at entry i.
// func assertFingerTable(t *testing.T, node *Node, i int, node2 *Node) {
// 	if node.FingerTable[i].Node.Addr != node2.Addr {
// 		t.Fatalf("Expected %v, got %v", node2.Id, node.FingerTable[i].Node.Id)
// 	}
// }

// // Helper for closest preceding finger. Asserts that closest is the closest
// // preceding finger to id according to node.
// func assertClosest(t *testing.T, node, closest *Node, id byte) {
// 	if remoteNode, err := ClosestPrecedingFinger_RPC(node.RemoteNode, []byte{id}); err != nil {
// 		t.Fatalf("Unexpected error while getting closest:%v", err)
// 	} else if remoteNode.Addr != closest.Addr {
// 		t.Fatalf("Expected %v, got %v", closest.Id, remoteNode.Id)
// 	}
// }

// func assertCloseBombardment(t *testing.T, rangeStart, rangeEnd int, nodes []*Node, closest *Node) {
// 	for i := rangeStart; i <= rangeEnd; i++ {
// 		for _, node := range nodes {
// 			assertClosest(t, node, closest, byte(i))
// 		}
// 	}
// }
