package gmaj

import (
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/r-medina/gmaj/gmajpb"

	"google.golang.org/grpc/grpclog"
)

var testTimeout = 350 * time.Millisecond

func init() {
	grpclog.SetLogger(log.New(ioutil.Discard, "", 0))

}

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

func createSimpleNode(t *testing.T, ring *gmajpb.RemoteNode) *Node {
	return createDefinedNode(t, ring, nil)
}

func createDefinedNode(t *testing.T, ring *gmajpb.RemoteNode, id []byte) *Node {
	node, err := NewDefinedNode(ring, id)
	if err != nil {
		t.Fatalf("Unable to create node, received error:%v", err)
	}

	return node
}
