package gmaj

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/r-medina/gmaj/gmajpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

// Node represents a node in the Chord mesh.
type Node struct {
	remoteNode gmajpb.RemoteNode

	grpcs *grpc.Server

	Predecessor *gmajpb.RemoteNode // This Node's predecessor
	predMtx     sync.RWMutex

	Successor *gmajpb.RemoteNode // This Node's successor
	succMtx   sync.RWMutex

	shutdownCh chan struct{}

	fingerTable fingerTable  // Finger table entries
	ftMtx       sync.RWMutex // RWLock for finger table

	dataStore map[string]string // Local datastore for this node
	dsMtx     sync.RWMutex      // RWLock for datastore

	dialOpts []grpc.DialOption

	log grpclog.Logger

	clientConns map[string]*clientConn
	connMtx     sync.RWMutex
}

var _ gmajpb.NodeServer = (*Node)(nil)

// NewNode creates a Chord node with random ID based on listener address.
func NewNode(parent *gmajpb.RemoteNode, opts ...grpc.DialOption) (*Node, error) {
	return NewDefinedNode(parent, nil, opts...)
}

// NewDefinedNode creates a Chord node with a pre-defined ID (useful for
// testing) if a non-nil id is provided.
func NewDefinedNode(
	parent *gmajpb.RemoteNode, id []byte, dialOpts ...grpc.DialOption,
) (*Node, error) {
	lis, err := net.Listen("tcp", "")
	if err != nil {
		return nil, err
	}

	node := &Node{
		shutdownCh:  make(chan struct{}),
		dialOpts:    dialOpts,
		clientConns: make(map[string]*clientConn),
	}
	node.grpcs = grpc.NewServer()
	gmajpb.RegisterNodeServer(node.grpcs, node)

	if id != nil {
		node.remoteNode.Id = id
	} else {
		node.remoteNode.Id = HashKey(lis.Addr().String())
	}
	node.remoteNode.Addr = lis.Addr().String()
	node.dataStore = make(map[string]string)

	// Populate finger table
	node.initFingerTable()

	// Start RPC server
	go node.grpcs.Serve(lis)

	// Join this node to the same chord ring as parent
	var joinNode *gmajpb.RemoteNode
	if parent != nil {
		// Ask if our id exists on the ring.
		remoteNode, err := node.FindSuccessorRPC(parent, node.remoteNode.Id)
		if err != nil {
			return nil, err
		}

		if IDsEqual(remoteNode.Id, node.remoteNode.Id) {
			return nil, errors.New("node with id already exists")
		}

		joinNode = parent
	} else {
		joinNode = &node.remoteNode
	}

	if err = node.join(joinNode); err != nil {
		return nil, err
	}

	// thread 2: kick off timer to stabilize periodically
	go func() {
		for {
			select {
			case <-time.After(cfg.StabilizeInterval):
				node.stabilize()
			case <-node.shutdownCh:
				return
			}
		}
	}()

	// thread 3: kick off timer to fix finger table periodically
	go func() {
		next := 0
		for {
			select {
			case <-time.After(cfg.FixNextFingerInterval):
				next = node.fixNextFinger(next)
			case <-node.shutdownCh:
				return
			}
		}
	}()

	<-time.After(cfg.StabilizeInterval)

	return node, nil
}

// RemoteNode returns a pointer to the gmajpb.RemoteNode of the node.
func (node *Node) RemoteNode() *gmajpb.RemoteNode {
	remoteNode := node.remoteNode
	return &remoteNode
}

// ID returns a copy of the node's ID
func (node *Node) ID() []byte {
	out := make([]byte, len(node.remoteNode.Id))
	copy(out, node.remoteNode.Id)

	return out
}

// Addr returns the node's address.
func (node *Node) Addr() string {
	return node.remoteNode.Addr
}

// join allows this node to join an existing ring that a remote node
// is a part of (i.e., other).
func (node *Node) join(other *gmajpb.RemoteNode) error {
	succ, err := node.FindSuccessorRPC(other, node.remoteNode.Id)
	if err != nil {
		return err
	}
	node.succMtx.Lock()
	node.Successor = succ
	node.succMtx.Unlock()

	return node.obtainNewKeys()
}

// stabilize attempts to stabilize a node.
// This is an implementation of the psuedocode from figure 7 of chord paper.
func (node *Node) stabilize() {
	node.succMtx.RLock()
	_succ := node.Successor
	if _succ == nil {
		node.succMtx.RUnlock()
		return
	}
	node.succMtx.RUnlock()

	// TODO(r-medina): handle error
	succ, err := node.GetPredecessorRPC(_succ)
	if succ == nil || err != nil {
		return
	}

	// If the predecessor of our successor is nil (succ), it means that our
	// successor has not had the chance to update their predecessor pointer. We
	// still want to notify them of our belief that we are its predecessor.
	if succ.Id != nil && Between(succ.Id, node.remoteNode.Id, node.Successor.Id) {
		node.succMtx.Lock()
		node.Successor = succ
		node.succMtx.Unlock()
	}

	// TODO(r-medina): handle error (necessary?)
	_ = node.NotifyRPC(node.Successor, &node.remoteNode)

	return
}

// notify is called when a remote node thinks its our predecessor. This is an
// implementation of the psuedocode from figure 7 of chord paper.
func (node *Node) notify(remoteNode *gmajpb.RemoteNode) {
	node.predMtx.Lock()
	defer node.predMtx.Unlock()

	// We only update predecessor if it is not us (i.e. we are only one in the
	// circle) since we are guaranteed that each node's successor link is
	// correct.
	if !(node.Predecessor == nil ||
		Between(remoteNode.Id, node.Predecessor.Id, node.remoteNode.Id)) {
		return
	}

	var prevID []byte
	if node.Predecessor != nil {
		prevID = node.Predecessor.Id
	}

	// Update predecessor and transfer keys.
	node.Predecessor = remoteNode

	if Between(node.Predecessor.Id, prevID, node.remoteNode.Id) {
		node.transferKeys(
			&gmajpb.TransferMsg{FromID: prevID, ToNode: node.Predecessor},
		)
	}
}

// findSuccessor finds the node's successor. This implements psuedocode from
// figure 4 of chord paper.
func (node *Node) findSuccessor(id []byte) (*gmajpb.RemoteNode, error) {
	pred, err := node.findPredecessor(id)
	if err != nil {
		return nil, err
	}

	// TODO(r-medina): make an error in the rpc stuff for empty responses?
	if pred.Addr == "" {
		return &node.remoteNode, nil
	}

	succ, err := node.GetSuccessorRPC(pred)
	if err != nil {
		return nil, err
	}

	if succ.Addr == "" {
		return &node.remoteNode, nil
	}

	return succ, nil
}

// findPredecessor finds the node's predecessor. This implements psuedocode from
// figure 4 of chord paper.
func (node *Node) findPredecessor(id []byte) (*gmajpb.RemoteNode, error) {
	pred := &node.remoteNode
	node.succMtx.Lock()
	succ := node.Successor
	node.succMtx.Unlock()
	if succ == nil {
		return pred, nil
	}
	if !BetweenRightIncl(id, pred.Id, succ.Id) {
		pred = node.closestPrecedingFinger(id)
	} else {
		return pred, nil
	}

	// TODO(asubiotto): Handle error?
	succ, _ = node.GetSuccessorRPC(pred)

	if succ == nil || succ.Addr == "" {
		return pred, nil
	}

	for !BetweenRightIncl(id, pred.Id, succ.Id) {
		var err error
		pred, err = node.ClosestPrecedingFingerRPC(succ, id)
		if err != nil {
			return nil, err
		}

		if pred.Addr == "" {
			return &node.remoteNode, nil
		}

		succ, err = node.GetSuccessorRPC(pred)
		if err != nil {
			return nil, err
		}

		if succ.Addr == "" {
			return &node.remoteNode, nil
		}
	}

	return pred, nil
}

// closestPrecedingFinger finds the closest preceding finger in the table.
// This implements pseudocode from figure 4 of chord paper.
func (node *Node) closestPrecedingFinger(id []byte) *gmajpb.RemoteNode {
	node.ftMtx.RLock()
	defer node.ftMtx.RUnlock()

	for i := cfg.KeySize - 1; i >= 0; i-- {
		n := node.fingerTable[i]
		if n.RemoteNode == nil {
			continue
		}

		// Check that the node we believe is the successor for
		// (node + 2^i) mod 2^m also precedes id.
		if Between(n.RemoteNode.Id, node.remoteNode.Id, id) {
			return n.RemoteNode
		}
	}

	return &node.remoteNode
}

// Shutdown shuts down the Chord node (gracefully).
func (node *Node) Shutdown() {
	close(node.shutdownCh)

	// Notify successor to change its predecessor pointer to our predecessor.
	// Do nothing if we are our own successor (i.e. we are the only node in the
	// ring).
	node.succMtx.RLock()
	if node.remoteNode.Addr != node.Successor.Addr {
		node.predMtx.Lock()
		_ = node.transferKeys(&gmajpb.TransferMsg{
			FromID: node.Predecessor.Id,
			ToNode: node.Successor,
		})
		_ = node.SetPredecessorRPC(node.Successor, node.Predecessor)
		_ = node.SetSuccessorRPC(node.Predecessor, node.Successor)
		node.predMtx.Unlock()
	}
	node.succMtx.RUnlock()

	node.connMtx.Lock()
	for _, cc := range node.clientConns {
		cc.conn.Close()
	}
	node.clientConns = nil
	node.connMtx.Unlock()
	node.grpcs.Stop()

	<-time.After(cfg.StabilizeInterval)
}
