package gmaj

import (
	"testing"
	"time"
)

func TestGetSuccessorIsYourself(t *testing.T) {
	node := createSimpleNode(t, nil)
	assertSuccessor(t, node, node)
}

func TestGetSuccessorSimple(t *testing.T) {
	node1 := createSimpleNode(t, nil)
	node2 := createSimpleNode(t, &node1.remoteNode)

	// Wait for ring to stabilize.
	<-time.After(200 * time.Millisecond)

	assertSuccessor(t, node1, node2)
	assertSuccessor(t, node2, node1)
}

func TestGetSuccessorThreeNodes(t *testing.T) {
	node1, node2, node3 := create3SuccessiveNodes(t)

	// Wait for ring to stabilize.
	<-time.After(time.Second)

	assertSuccessor(t, node1, node2)
	assertSuccessor(t, node2, node3)
	assertSuccessor(t, node3, node1)

	// Remove node2 from ring.
	node2.Shutdown()

	// Wait for node to shut down gracefully.
	<-time.After(time.Second)

	assertSuccessor(t, node3, node1)
	assertSuccessor(t, node1, node3)

	node4 := createDefinedNode(t, &node1.remoteNode, []byte{50})

	<-time.After(time.Second)

	assertSuccessor(t, node1, node3)
	assertSuccessor(t, node3, node4)
	assertSuccessor(t, node4, node1)
}

func TestNotifySimpleCorrect(t *testing.T) {
	node1, node2, node3 := create3SuccessiveNodes(t)

	// Artificially set predecessor pointers to something so that notify does
	// not succeed due to predecessors being nil.
	node1.Predecessor = &node2.remoteNode
	node2.Predecessor = &node3.remoteNode
	node3.Predecessor = &node1.remoteNode

	if err := NotifyRPC(&node3.remoteNode, &node2.remoteNode); err != nil {
		t.Fatalf("Unexpected error notifying node:%v", err)
	}

	// Tests that notify wraps around correctly.
	if err := NotifyRPC(&node1.remoteNode, &node3.remoteNode); err != nil {
		t.Fatalf("Unexpected error notifying node:%v", err)
	}
}

func TestNotifySimpleIncorrect(t *testing.T) {
	node1, node2, node3 := create3SuccessiveNodes(t)

	// Manually stabilize the predecessor pointers.
	node1.Predecessor = &node3.remoteNode
	node2.Predecessor = &node1.remoteNode
	node3.Predecessor = &node2.remoteNode

	if err := NotifyRPC(&node2.remoteNode, &node3.remoteNode); err == nil {
		t.Fatalf("Unexpected success notifying node1")
	}

	if err := NotifyRPC(&node3.remoteNode, &node1.remoteNode); err == nil {
		t.Fatalf("Unexpected success notifying node2")
	}

	if err := NotifyRPC(&node1.remoteNode, &node2.remoteNode); err == nil {
		t.Fatalf("Unexpected success notifying node3")
	}
}

func TestFindSuccessorSimple(t *testing.T) {
	node := createDefinedNode(t, nil, []byte{10})
	assertSuccessorID(t, 5, node)
	assertSuccessorID(t, 0, node)
	assertSuccessorID(t, 10, node)
	assertSuccessorID(t, 12, node)
	assertSuccessorID(t, 240, node)
}

func TestFindSuccessorMultipleNodes(t *testing.T) {
	node1, node2, node3 := create3SuccessiveNodes(t)

	<-time.After(time.Second)

	assertSuccessorID(t, 0, node1)
	assertSuccessorID(t, 10, node2)
	assertSuccessorID(t, 20, node3)
	assertSuccessorID(t, 5, node2)
	assertSuccessorID(t, 15, node3)
	assertSuccessorID(t, 240, node1)

	node1.Shutdown()

	<-time.After(time.Second)

	assertSuccessorID(t, 0, node2)
	assertSuccessorID(t, 8, node2)
	assertSuccessorID(t, 10, node2)
	assertSuccessorID(t, 240, node2)
	assertSuccessorID(t, 15, node3)
	assertSuccessorID(t, 20, node3)
	assertSuccessorID(t, 21, node2)

	node4 := createDefinedNode(t, &node3.remoteNode, []byte{60})

	<-time.After(time.Second)

	assertSuccessorID(t, 21, node4)
	assertSuccessorID(t, 240, node2)
	assertSuccessorID(t, 60, node4)
	assertSuccessorID(t, 61, node2)
	assertSuccessorID(t, 20, node3)
	assertSuccessorID(t, 18, node3)
}

func TestClosestPrecedingFingerSimple(t *testing.T) {
	node := createDefinedNode(t, nil, []byte{50})
	assertClosest(t, node, node, 0)
	assertClosest(t, node, node, 49)
	assertClosest(t, node, node, 50)
	assertClosest(t, node, node, 51)
	assertClosest(t, node, node, 52)

	node2 := createDefinedNode(t, &node.remoteNode, []byte{51})
	<-time.After(time.Second)
	assertClosest(t, node, node, 51)
	assertClosest(t, node2, node, 51)

	node3 := createDefinedNode(t, &node2.remoteNode, []byte{49})
	<-time.After(time.Second)
	assertClosest(t, node, node3, 50)
	assertClosest(t, node2, node3, 50)
	assertClosest(t, node3, node3, 50)
	assertClosest(t, node, node2, 49)
}

func TestClosestPrecedingFingerComplicated(t *testing.T) {
	// nodeX refers to a node with node.Id == X
	node0, node10, node20 := create3SuccessiveNodes(t)
	nodes := []*Node{node0, node10, node20}
	<-time.After(time.Second)
	assertCloseBombardment(t, 1, 10, nodes, node0)
	// node20's closest preceding finger should be 0 for this range.
	assertCloseBombardment(t, 11, 20, nodes[:2], node10)
	assertCloseBombardment(t, 21, 255, nodes, node20)
}
