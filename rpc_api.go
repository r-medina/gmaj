package gmaj

import (
	"errors"

	"github.com/r-medina/gmaj/gmajpb"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

//
// RPC connection map cache
//

type clientConn struct {
	client gmajpb.NodeClient
	conn   *grpc.ClientConn
}

//
// Chord Node RPC API
//

// GetPredecessorRPC gets the predecessor ID of a remote node.
func (node *Node) GetPredecessorRPC(remoteNode *gmajpb.RemoteNode) (*gmajpb.RemoteNode, error) {
	client, err := node.getNodeClient(remoteNode)
	if err != nil {
		return nil, err
	}

	return client.GetPredecessor(context.Background(), mt)
}

// GetSuccessorRPC the successor ID of a remote node.
func (node *Node) GetSuccessorRPC(remoteNode *gmajpb.RemoteNode) (*gmajpb.RemoteNode, error) {
	client, err := node.getNodeClient(remoteNode)
	if err != nil {
		return nil, err
	}

	return client.GetSuccessor(context.Background(), mt)
}

// SetPredecessorRPC noties a remote node that we believe we are its predecessor.
func (node *Node) SetPredecessorRPC(remoteNode, newPred *gmajpb.RemoteNode) error {
	client, err := node.getNodeClient(remoteNode)
	if err != nil {
		return err
	}

	_, err = client.SetPredecessor(context.Background(), newPred)
	return err
}

// SetSuccessorRPC sets the successor ID of a remote node.
func (node *Node) SetSuccessorRPC(remoteNode, newSucc *gmajpb.RemoteNode) error {
	client, err := node.getNodeClient(remoteNode)
	if err != nil {
		return err
	}

	_, err = client.SetSuccessor(context.Background(), newSucc)
	return err
}

// NotifyRPC notifies a remote node that pred is its predecessor.
func (node *Node) NotifyRPC(remoteNode, pred *gmajpb.RemoteNode) error {
	client, err := node.getNodeClient(remoteNode)
	if err != nil {
		return err
	}

	_, err = client.Notify(context.Background(), pred)
	return err
}

// ClosestPrecedingFingerRPC finds the closest preceding finger from a remote
// node for an ID.
func (node *Node) ClosestPrecedingFingerRPC(
	remoteNode *gmajpb.RemoteNode, id []byte,
) (*gmajpb.RemoteNode, error) {
	client, err := node.getNodeClient(remoteNode)
	if err != nil {
		return nil, err
	}

	return client.ClosestPrecedingFinger(context.Background(), &gmajpb.ID{Id: id})
}

// FindSuccessorRPC finds the successor node of a given ID in the entire ring.
func (node *Node) FindSuccessorRPC(
	remoteNode *gmajpb.RemoteNode, id []byte,
) (*gmajpb.RemoteNode, error) {
	client, err := node.getNodeClient(remoteNode)
	if err != nil {
		return nil, err
	}

	return client.FindSuccessor(context.Background(), &gmajpb.ID{Id: id})
}

//
// Datastore RPC API
//

// GetRPC gets a value from a remote node's datastore for a given key.
func (node *Node) GetRPC(remoteNode *gmajpb.RemoteNode, key string) (string, error) {
	client, err := node.getNodeClient(remoteNode)
	if err != nil {
		return "", err
	}

	val, err := client.Get(context.Background(), &gmajpb.Key{Key: key})
	if err != nil {
		return "", err
	}

	return val.Val, nil
}

// PutRPC puts a key/value into a datastore on a remote node.
func (node *Node) PutRPC(remoteNode *gmajpb.RemoteNode, key string, val string) error {
	client, err := node.getNodeClient(remoteNode)
	if err != nil {
		return err
	}

	_, err = client.Put(context.Background(), &gmajpb.KeyVal{Key: key, Val: val})
	return err
}

// TransferKeysRPC informs a successor node that we should now take care of IDs
// between (node.Id : predId]. This should trigger the successor node to
// transfer the relevant keys back to node
func (node *Node) TransferKeysRPC(
	remoteNode *gmajpb.RemoteNode, fromID []byte, toNode *gmajpb.RemoteNode,
) error {
	client, err := node.getNodeClient(remoteNode)
	if err != nil {
		return err
	}

	_, err = client.TransferKeys(context.Background(), &gmajpb.TransferMsg{FromID: fromID, ToNode: toNode})
	return err
}

// getNodeClient is a helper function to make a call to a remote node.
func (node *Node) getNodeClient(
	remoteNode *gmajpb.RemoteNode,
) (gmajpb.NodeClient, error) {
	// Dial the server if we don't already have a connection to it
	remoteNodeAddr := remoteNode.Addr
	node.connMtx.RLock()
	cc, ok := node.clientConns[remoteNodeAddr]
	node.connMtx.RUnlock()
	if ok {
		return cc.client, nil
	}

	conn, err := grpc.Dial(
		remoteNodeAddr,
		// only way to do per-node credentials I can think of...
		append(cfg.DialOptions, node.dialOpts...)...,
	)
	if err != nil {
		return nil, err
	}

	client := gmajpb.NewNodeClient(conn)
	cc = &clientConn{client, conn}
	node.connMtx.Lock()
	if node.clientConns == nil {
		node.connMtx.Unlock()
		return nil, errors.New("must instantiate node before using")
	}
	node.clientConns[remoteNodeAddr] = cc
	node.connMtx.Unlock()

	return client, nil
}
