package gmaj

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/r-medina/gmaj/gmajpb"

	"google.golang.org/grpc"
)

// ErrBadIDLen indicates that the passed in ID is of the wrong length.
var ErrBadIDLen = errors.New("gmaj: ID length does not match length in configuration")

// Node represents a node in the Chord mesh.
type Node struct {
	*gmajpb.Node

	opts nodeOptions

	grpcs *grpc.Server

	Predecessor *gmajpb.Node // This Node's predecessor
	predMtx     sync.RWMutex

	Successor *gmajpb.Node // This Node's successor
	succMtx   sync.RWMutex

	shutdownCh chan struct{}

	fingerTable fingerTable  // Finger table entries
	ftMtx       sync.RWMutex // RWLock for finger table

	dataStore map[string]string // Local datastore for this node
	dsMtx     sync.RWMutex      // RWLock for datastore

	clientConns map[string]*clientConn
	connMtx     sync.RWMutex
}

type nodeOptions struct {
	id         []byte
	addr       string
	parent     *gmajpb.Node
	serverOpts []grpc.ServerOption
	dialOpts   []grpc.DialOption
}

// NodeOption is a function that customizes a Node.
type NodeOption func(o *nodeOptions)

// withID  sets a custom ID on the nodee. Useful for testing.
func withID(id []byte) NodeOption {
	return func(o *nodeOptions) {
		o.id = id
	}
}

// WithAddress specifies an address that the node will listen on.
func WithAddress(addr string) NodeOption {
	return func(o *nodeOptions) {
		o.addr = addr
	}
}

// WithParent specifies a parent to which the node should connect.
func WithParent(node *gmajpb.Node) NodeOption {
	return func(o *nodeOptions) {
		o.parent = node
	}
}

// WithGRPCServerOptions instantiates the gRPC server with the specified options.
func WithGRPCServerOptions(opts ...grpc.ServerOption) NodeOption {
	return func(o *nodeOptions) {
		o.serverOpts = opts
	}
}

// WithGRPCDialOptions makes the node dial other nodes with the specified gRPC
// dial options.
func WithGRPCDialOptions(opts ...grpc.DialOption) NodeOption {
	return func(o *nodeOptions) {
		o.dialOpts = opts
	}
}

var _ gmajpb.ChordServer = (*Node)(nil)

// NewNode creates a Chord node with a pre-defined ID (useful for
// testing) if a non-nil id is provided.
func NewNode(opts ...NodeOption) (*Node, error) {
	node := &Node{
		Node:        new(gmajpb.Node),
		shutdownCh:  make(chan struct{}),
		clientConns: make(map[string]*clientConn),
	}

	for _, opt := range opts {
		opt(&node.opts)
	}

	id := node.opts.id
	parent := node.opts.parent

	lis, err := net.Listen("tcp", node.opts.addr)
	if err != nil {
		return nil, err
	}

	node.grpcs = grpc.NewServer(node.opts.serverOpts...)
	gmajpb.RegisterChordServer(node.grpcs, node)

	if id != nil {
		if len(id) != config.IDLength {
			return nil, ErrBadIDLen
		}
		node.Id = id
	} else {
		node.Id = hashKey(lis.Addr().String())
	}
	node.Addr = lis.Addr().String()
	node.dataStore = make(map[string]string)

	// Populate finger table
	node.initFingerTable()

	// Start RPC server
	go node.grpcs.Serve(lis)

	// Join this node to the same chord ring as parent
	var joinNode *gmajpb.Node
	if parent != nil {
		// Ask if our id exists on the ring.
		remoteNode, err := node.FindSuccessorRPC(parent, node.Id)
		if err != nil {
			return nil, err
		}

		if idsEqual(remoteNode.Id, node.Id) {
			return nil, errors.New("node with id already exists")
		}

		joinNode = parent
	} else {
		joinNode = node.Node
	}

	if err := node.join(joinNode); err != nil {
		return nil, err
	}

	// thread 2: kick off timer to stabilize periodically
	go func() {
		for {
			ticker := time.NewTicker(config.StabilizeInterval)
			select {
			case <-ticker.C:
				node.stabilize()
			case <-node.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	// thread 3: kick off timer to fix finger table periodically
	go func() {
		next := 0
		for {
			ticker := time.NewTicker(config.FixNextFingerInterval)
			select {
			case <-ticker.C:
				next = node.fixNextFinger(next)
			case <-node.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	<-time.After(config.StabilizeInterval)

	return node, nil
}

// join allows this node to join an existing ring that a remote node
// is a part of (i.e., other).
func (node *Node) join(other *gmajpb.Node) error {
	succ, err := node.FindSuccessorRPC(other, node.Id)
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

	succ, err := node.GetPredecessorRPC(_succ)
	if succ == nil || err != nil {
		// TODO: handle failed client
		return
	}

	// If the predecessor of our successor is nil (succ), it means that our
	// successor has not had the chance to update their predecessor pointer. We
	// still want to notify them of our belief that we are its predecessor.
	if succ.Id != nil && between(succ.Id, node.Id, node.Successor.Id) {
		node.succMtx.Lock()
		node.Successor = succ
		node.succMtx.Unlock()
	}

	// TODO(r-medina): handle error (necessary?)
	_ = node.NotifyRPC(node.Successor, node.Node)

	return
}

// notify is called when a remote node thinks its our predecessor. This is an
// implementation of the psuedocode from figure 7 of chord paper.
func (node *Node) notify(remoteNode *gmajpb.Node) {
	node.predMtx.Lock()
	defer node.predMtx.Unlock()

	// We only update predecessor if it is not us (i.e. we are only one in the
	// circle) since we are guaranteed that each node's successor link is
	// correct.
	if !(node.Predecessor == nil ||
		between(remoteNode.Id, node.Predecessor.Id, node.Id)) {
		return
	}

	var prevID []byte
	if node.Predecessor != nil {
		prevID = node.Predecessor.Id
	}

	// Update predecessor and transfer keys.
	node.Predecessor = remoteNode

	if between(node.Predecessor.Id, prevID, node.Id) {
		node.transferKeys(
			&gmajpb.TransferKeysReq{FromId: prevID, ToNode: node.Predecessor},
		)
	}
}

// findSuccessor finds the node's successor. This implements psuedocode from
// figure 4 of chord paper.
func (node *Node) findSuccessor(id []byte) (*gmajpb.Node, error) {
	pred, err := node.findPredecessor(id)
	if err != nil {
		return nil, err
	}

	// TODO(r-medina): make an error in the rpc stuff for empty responses?
	if pred.Addr == "" {
		return node.Node, nil
	}

	succ, err := node.GetSuccessorRPC(pred)
	if err != nil {
		return nil, err
	}

	if succ.Addr == "" {
		return node.Node, nil
	}

	return succ, nil
}

// findPredecessor finds the node's predecessor. This implements psuedocode from
// figure 4 of chord paper.
func (node *Node) findPredecessor(id []byte) (*gmajpb.Node, error) {
	pred := node.Node
	node.succMtx.Lock()
	succ := node.Successor
	node.succMtx.Unlock()
	if succ == nil {
		return pred, nil
	}
	if !betweenRightIncl(id, pred.Id, succ.Id) {
		pred = node.closestPrecedingFinger(id)
	} else {
		return pred, nil
	}

	// TODO(asubiotto): Handle error?
	succ, _ = node.GetSuccessorRPC(pred)

	if succ == nil || succ.Addr == "" {
		return pred, nil
	}

	for !betweenRightIncl(id, pred.Id, succ.Id) {
		var err error
		pred, err = node.ClosestPrecedingFingerRPC(succ, id)
		if err != nil {
			return nil, err
		}

		if pred.Addr == "" {
			return node.Node, nil
		}

		succ, err = node.GetSuccessorRPC(pred)
		if err != nil {
			return nil, err
		}

		if succ.Addr == "" {
			return node.Node, nil
		}
	}

	return pred, nil
}

// closestPrecedingFinger finds the closest preceding finger in the table.
// This implements pseudocode from figure 4 of chord paper.
func (node *Node) closestPrecedingFinger(id []byte) *gmajpb.Node {
	node.ftMtx.RLock()
	defer node.ftMtx.RUnlock()

	for i := config.KeySize - 1; i >= 0; i-- {
		n := node.fingerTable[i]
		if n.RemoteNode == nil {
			continue
		}

		// Check that the node we believe is the successor for
		// (node + 2^i) mod 2^m also precedes id.
		if between(n.RemoteNode.Id, node.Id, id) {
			return n.RemoteNode
		}
	}

	return node.Node
}

// Shutdown shuts down the Chord node (gracefully).
func (node *Node) Shutdown() {
	close(node.shutdownCh)

	// Notify successor to change its predecessor pointer to our predecessor.
	// Do nothing if we are our own successor (i.e. we are the only node in the
	// ring).
	node.succMtx.RLock()
	node.predMtx.Lock()
	pred := node.Predecessor
	succ := node.Successor
	node.predMtx.Unlock()
	node.succMtx.RUnlock()

	if node.Addr != node.Successor.Addr {
		_ = node.transferKeys(&gmajpb.TransferKeysReq{
			FromId: pred.Id,
			ToNode: succ,
		})
		_ = node.SetPredecessorRPC(succ, pred)
		_ = node.SetSuccessorRPC(pred, succ)
	}

	node.connMtx.Lock()
	for _, cc := range node.clientConns {
		cc.conn.Close()
	}
	node.clientConns = nil
	node.connMtx.Unlock()
	node.grpcs.Stop()
	node.dsMtx.Lock()
	node.dataStore = nil
	node.dsMtx.Unlock()
}
