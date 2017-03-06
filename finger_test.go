package gmaj

import (
	"bytes"
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/r-medina/gmaj/gmajpb"
)

func TestInitFingerTable(t *testing.T) {
	t.Parallel()

	node := createSimpleNode(t, nil)

	if want, got := config.KeySize, len(node.fingerTable); got != want {
		t.Fatalf("Expected finger table length %v, got %v.", want, got)
	}
	node.ftMtx.RLock()
	if !bytes.Equal(node.fingerTable[0].StartID, addIDs(node.Id, []byte{1})) {
		node.ftMtx.RUnlock()
		t.Fatalf("First finger entry start is wrong. got %v, expected %v",
			node.fingerTable[0].StartID,
			addIDs(node.Id, []byte{1}))
	}
	node.ftMtx.RUnlock()

	node.ftMtx.Lock()
	defer node.ftMtx.Unlock()
	if !reflect.DeepEqual(node.fingerTable[0].RemoteNode, node.Node) {
		t.Fatalf("Finger entry does not point to itself.")
	}
}

func TestFixNextFinger(t *testing.T) {
	t.Parallel()

	node1 := &Node{Node: new(gmajpb.Node)}
	node1.Id = []byte{10}
	node1.Addr = "localhost"
	node1.ftMtx.Lock()
	node1.fingerTable = newFingerTable(node1.Node)
	next := 1
	next = node1.fixNextFinger(next) // shouldn't do anything because no rpc

	if next != 1 {
		t.Fatalf("next should not have changed.")
	}

	if !bytes.Equal(node1.fingerTable[0].StartID, addIDs(node1.Id, []byte{1})) {
		t.Fatalf("First finger entry start is wrong.")
	}

	if !reflect.DeepEqual(node1.fingerTable[0].RemoteNode, node1.Node) {
		t.Fatalf("Finger entry does not point to itself.")
	}

	node1, err := NewNode(nil)
	if err != nil {
		t.Fatal(err)
	}

	// TODO(r-medina): actually test this when rpc calls and stuff work
}

func TestFingerMath(t *testing.T) {
	t.Parallel()

	// this test expects the key size to be 8
	tests := []struct {
		n   int64
		i   int
		exp int64
	}{
		{n: 0, i: 0, exp: 1},
		{n: 2, i: 0, exp: 3},
		{n: 8, i: 0, exp: 9},
		{n: 64, i: 0, exp: 65},
		{n: 256, i: 0, exp: 1},
		{n: 10000, i: 0, exp: 17},
		{n: 512130, i: 0, exp: 131},

		{n: 0, i: 2, exp: 4},
		{n: 2, i: 2, exp: 6},
		{n: 8, i: 2, exp: 12},
		{n: 64, i: 2, exp: 68},
		{n: 256, i: 2, exp: 4},
		{n: 10000, i: 2, exp: 20},
		{n: 512130, i: 2, exp: 134},

		{n: 0, i: 8, exp: 0},
		{n: 2, i: 8, exp: 2},
		{n: 8, i: 8, exp: 8},
		{n: 64, i: 8, exp: 64},
		{n: 256, i: 8, exp: 0},
		{n: 10000, i: 8, exp: 16},
		{n: 512130, i: 8, exp: 130},
	}

	for i, test := range tests {
		result := fingerMath(big.NewInt(test.n).Bytes(), test.i, config.KeySize)
		want, got := padID(big.NewInt(test.exp).Bytes()), result
		if !bytes.Equal(got, want) {
			t.Logf("running test [%02d]", i)
			t.Fatalf("Expected %v, got %v.", test.exp, (&big.Int{}).SetBytes(result))
		}
	}
}

func TestStabilizedFingerTable(t *testing.T) {
	node1, node2, node3 := create3SuccessiveNodes(t)

	// Should be enough time to stabilize finger tables.
	<-time.After(testTimeout)

	tests := []struct {
		n1 *Node
		n2 *Node
		i  int
	}{
		{n1: node1, n2: node2, i: 0},
		{n1: node1, n2: node2, i: 1},
		{n1: node1, n2: node2, i: 2},
		{n1: node1, n2: node2, i: 3},
		{n1: node1, n2: node2, i: 4},
		{n1: node1, n2: node2, i: 5},
		{n1: node1, n2: node3, i: 6},
		{n1: node1, n2: node3, i: 7},

		{n1: node2, n2: node3, i: 0},
		{n1: node2, n2: node3, i: 1},
		{n1: node2, n2: node3, i: 2},
		{n1: node2, n2: node3, i: 3},
		{n1: node2, n2: node3, i: 4},
		{n1: node2, n2: node3, i: 5},
		{n1: node2, n2: node3, i: 6},
		{n1: node2, n2: node1, i: 7},

		{n1: node3, n2: node1, i: 0},
		{n1: node3, n2: node1, i: 1},
		{n1: node3, n2: node1, i: 2},
		{n1: node3, n2: node1, i: 3},
		{n1: node3, n2: node1, i: 4},
		{n1: node3, n2: node1, i: 5},
		{n1: node3, n2: node1, i: 6},
		{n1: node3, n2: node2, i: 7},
	}

	for i, test := range tests {
		want := test.n2.Id
		test.n1.ftMtx.RLock()
		got := test.n1.fingerTable[test.i].RemoteNode.Id
		test.n1.ftMtx.RUnlock()

		if !reflect.DeepEqual(got, want) {
			t.Logf("running test [%02d]", i)
			t.Errorf("Expected %v, got %v", want, got)
		}
	}
}
