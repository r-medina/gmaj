package gmaj

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

func TestInitFingerTable(t *testing.T) {
	node := createSimpleNode(t, nil)

	node.initFingerTable()

	if size := len(node.fingerTable); size != KeyLength {
		t.Fatalf("Expected finger table length %v, got %v.", KeyLength, size)
	}

	if !bytes.Equal(node.fingerTable[0].StartID, AddIDs(node.ID(), []byte{1})) {
		t.Fatalf("First finger entry start is wrong. got %v, expected %v",
			node.fingerTable[0].StartID,
			AddIDs(node.ID(), []byte{1}))
	}

	if !reflect.DeepEqual(*node.fingerTable[0].RemoteNode, node.remoteNode) {
		t.Fatalf("Finger entry does not point to itself.")
	}
}

func TestFixNextFinger(t *testing.T) {
	node1 := new(Node)
	node1.remoteNode.Id = []byte{10}
	node1.remoteNode.Addr = "localhost"
	node1.initFingerTable()
	next := 1
	next = node1.fixNextFinger(next) // shouldn't do anything because no rpc

	if next != 1 {
		t.Fatalf("next should not have changed.")
	}

	if !bytes.Equal(node1.fingerTable[0].StartID, AddIDs(node1.ID(), []byte{1})) {
		t.Fatalf("First finger entry start is wrong.")
	}

	if !reflect.DeepEqual(*node1.fingerTable[0].RemoteNode, node1.remoteNode) {
		t.Fatalf("Finger entry does not point to itself.")
	}

	node1, err := NewNode(nil)
	if err != nil {
		t.Fatal(err)
	}

	// TODO(r-medina): actually test this when rpc calls and stuff work
}

func TestFingerMath(t *testing.T) {
	tests := []struct {
		n   []byte
		i   int
		exp byte
	}{
		{n: []byte{0}, i: 0, exp: 1},
		{n: []byte{2}, i: 0, exp: 3},
		{n: []byte{20}, i: 0, exp: 21},
		{n: []byte{1}, i: 5, exp: 33},
		{n: []byte{2}, i: 5, exp: 34},
		{n: []byte{20}, i: 5, exp: 52},
		{n: []byte{1, 0}, i: 0, exp: 1},
		{n: []byte{1, 10}, i: 0, exp: 11},
	}

	for _, test := range tests {
		want, got := test.exp, fingerMath(test.n, test.i, KeyLength)[0]
		if got != want {
			t.Fatalf("Expected %v, got %v.", want, got)
		}
	}
}

func TestStabilizedFingerTable(t *testing.T) {
	node1, node2, node3 := create3SuccessiveNodes(t)

	// Should be enough time to stabilize finger tables.
	<-time.After(time.Second)

	tests := []struct {
		n1 *Node
		n2 *Node
		i  int
	}{
		{n1: node1, n2: node2, i: 0},
		{n1: node1, n2: node2, i: 1},
		{n1: node1, n2: node2, i: 2},
		{n1: node1, n2: node2, i: 3},
		{n1: node1, n2: node3, i: 4},
		{n1: node1, n2: node1, i: 5},
		{n1: node1, n2: node1, i: 6},
		{n1: node1, n2: node1, i: 7},
		{n1: node2, n2: node3, i: 0},
		{n1: node2, n2: node3, i: 1},
		{n1: node2, n2: node3, i: 2},
		{n1: node2, n2: node3, i: 3},
		{n1: node2, n2: node1, i: 4},
		{n1: node2, n2: node1, i: 5},
		{n1: node2, n2: node1, i: 6},
		{n1: node2, n2: node1, i: 7},
		{n1: node3, n2: node1, i: 0},
		{n1: node3, n2: node1, i: 1},
		{n1: node3, n2: node1, i: 2},
		{n1: node3, n2: node1, i: 3},
		{n1: node3, n2: node1, i: 4},
		{n1: node3, n2: node1, i: 5},
		{n1: node3, n2: node1, i: 6},
		{n1: node3, n2: node1, i: 7},
	}

	for _, test := range tests {
		assertFingerTable(t, test.n1, test.i, test.n2)
	}
}
