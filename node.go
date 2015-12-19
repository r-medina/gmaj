package gmaj

import (
	"errors"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
)

// Node represents a node in the Chord mesh.
type Node struct {
	remoteNode RemoteNode

	grpcs *grpc.Server

	Predecessor *RemoteNode // This Node's predecessor
	predMtx     sync.RWMutex

	Successor *RemoteNode // This Node's successor
	succMtx   sync.RWMutex

	IsShutdown  bool // Is node in process of shutting down?
	shutdownMtx sync.RWMutex

	fingerTable fingerTable  // Finger table entries
	ftMtx       sync.RWMutex // RWLock for finger table

	dataStore map[string]string // Local datastore for this node
	dsMtx     sync.RWMutex      // RWLock for datastore
}

// NewNode creates a Chord node with random ID based on listener address.
func NewNode(parent *RemoteNode) (*Node, error) {
	return NewDefinedNode(parent, nil)
}

// NewDefinedNode creates a Chord node with a pre-defined ID (useful for
// testing) if a non-nil id is provided.
func NewDefinedNode(parent *RemoteNode, id []byte) (*Node, error) {
	lis, err := net.Listen("tcp", "")
	if err != nil {
		return nil, err
	}

	node := new(Node)
	node.grpcs = grpc.NewServer()
	RegisterNodeServer(node.grpcs, node)

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
	if parent != nil {
		// Ask if our id exists on the ring.
		remoteNode, _ := FindSuccessorRPC(parent, node.remoteNode.Id)
		if IDsEqual(remoteNode.Id, node.remoteNode.Id) {
			err = errors.New("Node with id already exists")
		} else {
			err = node.join(parent)
		}
	} else {
		err = node.join(&node.remoteNode)
	}
	if err != nil {
		return nil, err
	}

	// thread 2: kick off timer to stabilize periodically
	go func() {
		for {
			if node.stabilize() {
				break
			}
			<-time.After(StabilizeInterval)
		}
	}()

	// thread 3: kick off timer to fix finger table periodically
	go func() {
		next := 0
		for {
			next = node.fixNextFinger(next)
			node.shutdownMtx.RLock()
			if node.IsShutdown {
				node.shutdownMtx.RUnlock()
				break
			}
			node.shutdownMtx.RUnlock()
			<-time.After(FixNextFingerInterval)
		}
	}()

	return node, nil
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
func (node *Node) join(other *RemoteNode) error {
	succ, err := FindSuccessorRPC(other, node.remoteNode.Id)
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
func (node *Node) stabilize() bool {
	node.shutdownMtx.RLock()
	if node.IsShutdown {
		node.shutdownMtx.RUnlock()
		return true
	}
	node.shutdownMtx.RUnlock()

	// TODO(r-medina): figure out the funky mutex shit here

	node.succMtx.RLock()
	if node.Successor == nil {
		node.succMtx.RUnlock()
		return false
	}
	node.succMtx.RUnlock()

	// TODO(r-medina): handle error
	succ, err := GetPredecessorRPC(node.Successor)
	if succ == nil || err != nil {
		return false
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
	NotifyRPC(node.Successor, &node.remoteNode)

	return false
}

// notify is called when a remote node thinks its our predecessor. This is an
// implementation of the psuedocode from figure 7 of chord paper.
func (node *Node) notify(remoteNode *RemoteNode) {
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
	if node.Predecessor == nil {
		prevID = nil
	} else {
		prevID = node.Predecessor.Id
	}

	// Update predecessor and transfer keys.
	node.Predecessor = remoteNode

	if Between(node.Predecessor.Id, prevID, node.remoteNode.Id) {
		node.transferKeys(&TransferMsg{prevID, node.Predecessor})
	}
}

// findSuccessor finds the node's successor. This implements psuedocode from
// figure 4 of chord paper.
func (node *Node) findSuccessor(id []byte) (*RemoteNode, error) {
	pred, err := node.findPredecessor(id)
	if err != nil {
		return nil, err
	}

	// TODO(r-medina): make an error in the rpc stuff for empty responses?
	if pred.Addr == "" {
		return &node.remoteNode, nil
	}

	succ, err := GetSuccessorRPC(pred)
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
func (node *Node) findPredecessor(id []byte) (*RemoteNode, error) {
	pred := node.closestPrecedingFinger(id)
	// TODO(asubiotto): Handle error?
	succ, _ := GetSuccessorRPC(pred)

	if succ == nil || succ.Addr == "" {
		return &node.remoteNode, nil
	}

	var err error
	for !BetweenRightIncl(id, pred.Id, succ.Id) {
		pred, err = ClosestPrecedingFingerRPC(succ, id)
		if err != nil {
			return nil, err
		}

		if pred.Addr == "" {
			return &node.remoteNode, nil
		}

		succ, err = GetSuccessorRPC(pred)
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
func (node *Node) closestPrecedingFinger(id []byte) *RemoteNode {
	node.ftMtx.RLock()
	defer node.ftMtx.RUnlock()
	for i := KeyLength - 1; i >= 0; i-- {
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
	node.shutdownMtx.RLock()
	node.IsShutdown = true
	node.shutdownMtx.RUnlock()

	// Notify successor to change its predecessor pointer to our predecessor.
	// Do nothing if we are our own successor (i.e. we are the only node in the
	// ring).
	node.succMtx.RLock()
	if node.remoteNode.Addr != node.Successor.Addr {
		node.predMtx.Lock()
		node.transferKeys(&TransferMsg{node.Predecessor.Id, node.Successor})
		SetPredecessorRPC(node.Successor, node.Predecessor)
		SetSuccessorRPC(node.Predecessor, node.Successor)
		node.predMtx.Unlock()
	}
	node.succMtx.RUnlock()

	// Wait for go routines to quit, should be enough time.
	<-time.After(time.Second * 2)

	connMtx.Lock()
	for addr, cc := range connMap {
		cc.conn.Close()
		delete(connMap, addr)
	}
	connMtx.Unlock()

	node.grpcs.Stop()
}
