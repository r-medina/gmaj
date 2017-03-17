package gmaj

import (
	"testing"
	"time"
)

func TestGetSuccessorIsYourself(t *testing.T) {
	t.Parallel()

	node := createSimpleNode(t, nil)
	assertSuccessor(t, node, node)
}

func TestGetSuccessorSimple(t *testing.T) {
	t.Parallel()

	node1 := createSimpleNode(t, nil)
	node2 := createSimpleNode(t, node1.Node)

	// Wait for ring to stabilize.
	<-time.After(testTimeout)

	assertSuccessor(t, node1, node2)
	assertSuccessor(t, node2, node1)
}

func TestGetSuccessorThreeNodes(t *testing.T) {
	t.Parallel()

	node1, node2, node3 := create3SuccessiveNodes(t)

	// Wait for ring to stabilize.
	<-time.After(testTimeout)

	assertSuccessor(t, node1, node2)
	assertSuccessor(t, node2, node3)
	assertSuccessor(t, node3, node1)

	// Remove node2 from ring.
	node2.Shutdown()

	// Wait for node to shut down gracefully.
	<-time.After(testTimeout)

	assertSuccessor(t, node3, node1)
	assertSuccessor(t, node1, node3)

	node4 := createDefinedNode(t, node1.Node, []byte{0xbb})

	<-time.After(testTimeout)

	assertSuccessor(t, node1, node3)
	assertSuccessor(t, node3, node4)
	assertSuccessor(t, node4, node1)
}

func TestNotifySimpleCorrect(t *testing.T) {
	t.Parallel()

	node1, node2, node3 := create3SuccessiveNodes(t)

	// Manually stabilize the predecessor pointers.
	node1.predMtx.Lock()
	node1.predecessor = node3.Node
	node1.predMtx.Unlock()

	node2.predMtx.Lock()
	node2.predecessor = node1.Node
	node2.predMtx.Unlock()

	node3.predMtx.Lock()
	node3.predecessor = node2.Node
	node3.predMtx.Unlock()

	if err := node1.notifyRPC(node1.Node, node3.Node); err != nil {
		t.Fatalf("Unexpected error notifying node: %v", err)
	}

	// Tests that notify wraps around correctly.
	if err := node3.notifyRPC(node3.Node, node2.Node); err != nil {
		t.Fatalf("Unexpected error notifying node: %v", err)
	}
}

func TestNotifySimpleIncorrect(t *testing.T) {
	t.Parallel()

	node1, node2, node3 := create3SuccessiveNodes(t)

	// Manually stabilize the predecessor pointers.
	node1.predMtx.Lock()
	node1.predecessor = node3.Node
	node1.predMtx.Unlock()

	node2.predMtx.Lock()
	node2.predecessor = node1.Node
	node2.predMtx.Unlock()

	node3.predMtx.Lock()
	node3.predecessor = node2.Node
	node3.predMtx.Unlock()

	if err := node2.notifyRPC(node2.Node, node3.Node); err == nil {
		t.Fatalf("Unexpected success notifying node1")
	}

	if err := node3.notifyRPC(node3.Node, node1.Node); err == nil {
		t.Fatalf("Unexpected success notifying node2")
	}

	if err := node1.notifyRPC(node1.Node, node2.Node); err == nil {
		t.Fatalf("Unexpected success notifying node3")
	}
}

func TestFindSuccessorSimple(t *testing.T) {
	t.Parallel()

	node := createDefinedNode(t, nil, []byte{10})
	assertSuccessorID(t, 5, node)
	assertSuccessorID(t, 0, node)
	assertSuccessorID(t, 10, node)
	assertSuccessorID(t, 12, node)
	assertSuccessorID(t, 240, node)
}

func TestFindSuccessorMultipleNodes(t *testing.T) {
	node1, node2, node3 := create3SuccessiveNodes(t)

	<-time.After(testTimeout << 1)

	assertSuccessorID(t, 0, node1)
	assertSuccessorID(t, 10, node2)
	assertSuccessorID(t, 56, node3)
	assertSuccessorID(t, 5, node2)
	assertSuccessorID(t, 0xa9, node3)
	assertSuccessorID(t, 0xbb, node1)

	node1.Shutdown()

	<-time.After(testTimeout)

	assertSuccessorID(t, 0, node2)
	assertSuccessorID(t, 10, node2)
	assertSuccessorID(t, 56, node3)
	assertSuccessorID(t, 5, node2)
	assertSuccessorID(t, 0xa9, node3)
	assertSuccessorID(t, 0xbb, node2)

	node4 := createDefinedNode(t, node3.Node, []byte{0xbb})

	<-time.After(testTimeout)

	assertSuccessorID(t, 0, node2)
	assertSuccessorID(t, 10, node2)
	assertSuccessorID(t, 56, node3)
	assertSuccessorID(t, 5, node2)
	assertSuccessorID(t, 0xa9, node3)
	assertSuccessorID(t, 0xba, node4)
	assertSuccessorID(t, 0xc3, node2)
	assertSuccessorID(t, 0xbb, node4)
}

func TestClosestPrecedingFingerSimple(t *testing.T) {
	t.Parallel()

	node := createDefinedNode(t, nil, []byte{50})
	assertClosest(t, node, node, 0)
	assertClosest(t, node, node, 49)
	assertClosest(t, node, node, 50)
	assertClosest(t, node, node, 51)
	assertClosest(t, node, node, 52)

	node2 := createDefinedNode(t, node.Node, []byte{51})
	<-time.After(testTimeout)
	assertClosest(t, node, node, 51)
	assertClosest(t, node2, node, 51)

	node3 := createDefinedNode(t, node2.Node, []byte{49})
	<-time.After(testTimeout)
	assertClosest(t, node, node3, 50)
	assertClosest(t, node2, node3, 50)
	assertClosest(t, node3, node3, 50)
	assertClosest(t, node, node2, 49)
}

func TestClosestPrecedingFingerComplicated(t *testing.T) {
	// nodeX refers to a node with node.Id == X
	node0, node10, node20 := create3SuccessiveNodes(t)
	nodes := []*Node{node0, node10, node20}

	<-time.After(testTimeout)

	assertCloseBombardment(t, 1, 10, nodes, node0)
	assertCloseBombardment(t, 56, 0xaa, nodes[:2], node10)
	assertCloseBombardment(t, 0xab, 0xff, nodes, node20)
}

// Helper for GetSuccessor tests. Issues an RPC to check if node2 is a successor
// of node1.
func assertSuccessor(t *testing.T, node1, node2 *Node) {
	if remoteNode, err := node1.getSuccessorRPC(node1.Node); err != nil {
		t.Fatalf("Unexpected error:%v", err)
	} else if remoteNode.Addr != node2.Addr {
		t.Fatalf(
			"Unexpected successor. Expected %v got %v",
			node2.Node,
			remoteNode,
		)
	}
}

// Helper for FindSuccessor tests. Issues an RPC to check that node is id's
// successor.
func assertSuccessorID(t *testing.T, id byte, node *Node) {
	if remoteNode, err := node.findSuccessorRPC(node.Node, []byte{id}); err != nil {
		t.Fatalf("Unexpected error:%v", err)
	} else if remoteNode.Addr != node.Addr {
		t.Fatalf("Unexpected successor. Expected %v got %v",
			node.Node,
			remoteNode)
	}
}

// Helper for closest preceding finger. Asserts that closest is the closest
// preceding finger to id according to node.
func assertClosest(t *testing.T, node, closest *Node, id byte) {
	remoteNode, err := node.closestPrecedingFingerRPC(node.Node, []byte{id})
	if err != nil {
		t.Fatalf("Unexpected error while getting closest:%v", err)
	} else if !idsEqual(remoteNode.Id, closest.Id) {
		t.Fatalf("Expected %v, got %v", closest.Id, remoteNode.Id)
	}
}

func assertCloseBombardment(t *testing.T, rangeStart, rangeEnd int, nodes []*Node, closest *Node) {
	for i := rangeStart; i <= rangeEnd; i++ {
		for _, node := range nodes {
			assertClosest(t, node, closest, byte(i))
		}
	}
}
