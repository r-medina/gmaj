package gmaj

import (
	"errors"

	"golang.org/x/net/context"
)

var (
	emptyRemote = new(RemoteNode)
	mt          = new(MT)
)

// GetPredecessor gets the predecessor on the node.
func (node *Node) GetPredecessor(context.Context, *MT) (*RemoteNode, error) {
	node.predMtx.RLock()
	pred := node.Predecessor
	node.predMtx.RUnlock()

	if pred == nil {
		return emptyRemote, nil
	}

	return pred, nil
}

// GetSuccessor gets the successor on the node..
func (node *Node) GetSuccessor(context.Context, *MT) (*RemoteNode, error) {
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
	ctxt context.Context, pred *RemoteNode,
) (*MT, error) {
	node.predMtx.Lock()
	node.Predecessor = pred
	node.predMtx.Unlock()

	return mt, nil
}

// SetSuccessor sets the successor on the node.
func (node *Node) SetSuccessor(
	ctx context.Context, succ *RemoteNode,
) (*MT, error) {
	node.succMtx.Lock()
	node.Successor = succ
	node.succMtx.Unlock()

	return mt, nil
}

// Notify is called when remoteNode thinks its our successor
func (node *Node) Notify(
	ctx context.Context, remoteNode *RemoteNode,
) (*MT, error) {
	if remoteNode == nil {
		return mt, errors.New("remoteNode cannot be nil")
	}

	node.notify(remoteNode)

	// If node.Predecessor is nil at this point, we were trying to notify
	// ourselves. Otherwise, to succeed, we must check that the predecessor
	// was correctly updated.
	node.predMtx.Lock()
	defer node.predMtx.Unlock()
	if node.Predecessor != nil &&
		!IDsEqual(node.Predecessor.Id, remoteNode.Id) {
		return mt, errors.New("remoteNode is not node's predecessor")
	}

	return mt, nil
}

// ClosestPrecedingFinger will find the closes preceding entry in the finger
// table based on the id.
func (node *Node) ClosestPrecedingFinger(
	ctxt context.Context, id *ID,
) (*RemoteNode, error) {
	remoteNode := node.closestPrecedingFinger(id.Id)
	if remoteNode == nil {
		return emptyRemote, errors.New("MT node closest preceding finger")
	}

	return remoteNode, nil
}

// FindSuccessor finds the successor, error if nil.
func (node *Node) FindSuccessor(
	ctxt context.Context, id *ID,
) (*RemoteNode, error) {
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
func (node *Node) Get(ctxt context.Context, key *Key) (*Val, error) {
	val, err := node.get(key)
	if err != nil {
		return nil, err
	}

	return &Val{Val: val}, nil
}

// Put stores a key value pair on the node.
func (node *Node) Put(ctxt context.Context, keyVal *KeyVal) (*MT, error) {
	if err := node.put(keyVal); err != nil {
		return mt, err
	}

	return mt, nil
}

// TransferKeys transfers the appropriate keys on this node
// to the remote node specified in the request.
func (node *Node) TransferKeys(
	ctxt context.Context, tmsg *TransferMsg,
) (*MT, error) {
	if err := node.transferKeys(tmsg); err != nil {
		return mt, err
	}

	return mt, nil
}
