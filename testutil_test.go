package gmaj

import (
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/r-medina/gmaj/gmajcfg"
	"github.com/r-medina/gmaj/gmajpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var testTimeout = 1000 * time.Millisecond

func init() {
	grpclog.SetLogger(log.New(ioutil.Discard, "", 0))

	cfg := gmajcfg.Config{
		KeySize:               8,
		IDLength:              1, // key length bytes
		FixNextFingerInterval: 25 * time.Millisecond,
		StabilizeInterval:     50 * time.Millisecond,
		RetryInterval:         75 * time.Millisecond,
		DialOptions: []grpc.DialOption{
			grpc.WithInsecure(),
		},
		Log: log.New(ioutil.Discard, "", 0),
	}

	if err := Init(&cfg); err != nil {
		panic(err)
	}
}

// Generally useful testing helper functions. Creates three successive nodes
// with ids 0 (node1), 10 (node2) and 20 (node3).
func create3SuccessiveNodes(t *testing.T) (*Node, *Node, *Node) {
	definedID := make([]byte, config.IDLength)
	node1 := createDefinedNode(t, nil, definedID)
	definedID = make([]byte, config.IDLength)
	definedID[0] = 55
	node2 := createDefinedNode(t, node1.Node, definedID)
	definedID = make([]byte, config.IDLength)
	definedID[0] = 0xaa
	node3 := createDefinedNode(t, node1.Node, definedID)
	return node1, node2, node3
}

func createSimpleNode(t *testing.T, ring *gmajpb.Node) *Node {
	return createDefinedNode(t, ring, nil)
}

func createDefinedNode(t *testing.T, ring *gmajpb.Node, id []byte) *Node {
	node, err := NewNode(ring, WithID(id))
	if err != nil {
		t.Fatalf("Unable to create node, received error:%v", err)
	}

	return node
}
