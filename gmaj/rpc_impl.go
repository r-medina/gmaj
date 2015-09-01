package gmaj

import (
	"errors"

	"golang.org/x/net/context"

	pb "github.com/r-medina/gmaj"
)

var (
	successResp = &pb.Response{true}
	failureResp = &pb.Response{false}
	emptyRemote = &pb.RemoteNode{}
)

// GetPredecessor gets the predecessor on the node.
func (node *Node) GetPredecessor(context.Context, *pb.Nil) (*pb.RemoteNode, error) {
	node.predMtx.RLock()
	pred := node.Predecessor
	node.predMtx.RUnlock()

	if pred == nil {
		return emptyRemote, nil
	}

	return pred, nil
}

// GetSuccessor gets the successor on the node..
func (node *Node) GetSuccessor(context.Context, *pb.Nil) (*pb.RemoteNode, error) {
	node.succMtx.RLock()
	succ := node.Successor
	node.succMtx.RUnlock()

	if succ == nil {
		return emptyRemote, nil
	}

	return succ, nil
}

// SetPredecessor sets the predecessor on the node.
func (node *Node) SetPredecessor(
	cntxt context.Context, pred *pb.RemoteNode,
) (*pb.Response, error) {
	node.predMtx.Lock()
	node.Predecessor = pred
	node.predMtx.Unlock()

	return successResp, nil
}

// SetSuccessor sets the successor on the node.
func (node *Node) SetSuccessor(
	cntx context.Context, succ *pb.RemoteNode,
) (*pb.Response, error) {
	node.succMtx.Lock()
	node.Successor = succ
	node.succMtx.Unlock()

	return successResp, nil
}

// Notify is called when remoteNode thinks its our successor
func (node *Node) Notify(
	cntxt context.Context, remoteNode *pb.RemoteNode,
) (*pb.Response, error) {
	if remoteNode == nil {
		return failureResp, errors.New("remoteNode cannot be nil")
	}

	node.notify(remoteNode)

	// If node.Predecessor is nil at this point, we were trying to notify
	// ourselves. Otherwise, to succeed, we must check that the predecessor
	// was correctly updated.
	node.predMtx.Lock()
	defer node.predMtx.Unlock()
	if node.Predecessor != nil &&
		!EqualIDs(node.Predecessor.Id, remoteNode.Id) {
		return failureResp, errors.New("remoteNode is not node's predecessor")
	}

	return successResp, nil
}

// ClosestPrecedingFinger will find the closes preceding entry in the finger
// table based on the id.
func (node *Node) ClosestPrecedingFinger(
	cntxt context.Context, id *pb.ID,
) (*pb.RemoteNode, error) {
	remoteNode := node.closestPrecedingFinger(id.Id)
	if remoteNode == nil {
		return emptyRemote, errors.New("Nil node closest preceding finger")
	}

	return remoteNode, nil
}

// FindSuccessor finds the successor, error if nil.
func (node *Node) FindSuccessor(
	cntxt context.Context, id *pb.ID,
) (*pb.RemoteNode, error) {
	succ, err := node.findSuccessor(id.Id)
	if err != nil {
		return emptyRemote, err
	}

	if succ == nil {
		return emptyRemote, errors.New("Node does not have a successor")
	}

	return succ, nil
}

// Get returns the value of the key requested at the node.
func (node *Node) Get(cntxt context.Context, k *pb.Key) (*pb.Val, error) {
	val, err := node.get(k)
	if err != nil {
		return &pb.Val{}, err
	}

	return &pb.Val{val}, nil
}

// Put stores a key value pair on the node.
func (node *Node) Put(cntxt context.Context, kv *pb.KeyVal) (*pb.Response, error) {
	if err := node.put(kv); err != nil {
		return failureResp, err
	}

	return successResp, nil
}

// TransferKeys transfers the appropriate keys on this node
// to the remote node specified in the request.
func (node *Node) TransferKeys(
	cntxt context.Context, tmsg *pb.TransferMsg,
) (*pb.Response, error) {
	if err := node.transferKeys(tmsg); err != nil {
		return failureResp, err
	}

	return successResp, nil
}
