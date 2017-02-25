package gmaj

import (
	"errors"
	"time"

	"github.com/r-medina/gmaj/gmajpb"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

//
// Chord Node RPC API
//

// GetPredecessorRPC gets the predecessor ID of a remote node.
func (node *Node) GetPredecessorRPC(remoteNode *gmajpb.Node) (*gmajpb.Node, error) {
	client, err := node.getChordClient(remoteNode)
	if err != nil {
		return nil, err
	}

	return client.GetPredecessor(context.Background(), mt)
}

// GetSuccessorRPC the successor ID of a remote node.
func (node *Node) GetSuccessorRPC(remoteNode *gmajpb.Node) (*gmajpb.Node, error) {
	client, err := node.getChordClient(remoteNode)
	if err != nil {
		return nil, err
	}

	return client.GetSuccessor(context.Background(), mt)
}

// SetPredecessorRPC noties a remote node that we believe we are its predecessor.
func (node *Node) SetPredecessorRPC(remoteNode, newPred *gmajpb.Node) error {
	client, err := node.getChordClient(remoteNode)
	if err != nil {
		return err
	}

	_, err = client.SetPredecessor(context.Background(), newPred)
	return err
}

// SetSuccessorRPC sets the successor ID of a remote node.
func (node *Node) SetSuccessorRPC(remoteNode, newSucc *gmajpb.Node) error {
	client, err := node.getChordClient(remoteNode)
	if err != nil {
		return err
	}

	_, err = client.SetSuccessor(context.Background(), newSucc)
	return err
}

// NotifyRPC notifies a remote node that pred is its predecessor.
func (node *Node) NotifyRPC(remoteNode, pred *gmajpb.Node) error {
	client, err := node.getChordClient(remoteNode)
	if err != nil {
		return err
	}

	_, err = client.Notify(context.Background(), pred)
	return err
}

// ClosestPrecedingFingerRPC finds the closest preceding finger from a remote
// node for an ID.
func (node *Node) ClosestPrecedingFingerRPC(
	remoteNode *gmajpb.Node, id []byte,
) (*gmajpb.Node, error) {
	client, err := node.getChordClient(remoteNode)
	if err != nil {
		return nil, err
	}

	return client.ClosestPrecedingFinger(context.Background(), &gmajpb.ID{Id: id})
}

// FindSuccessorRPC finds the successor node of a given ID in the entire ring.
func (node *Node) FindSuccessorRPC(
	remoteNode *gmajpb.Node, id []byte,
) (*gmajpb.Node, error) {
	client, err := node.getChordClient(remoteNode)
	if err != nil {
		return nil, err
	}

	return client.FindSuccessor(context.Background(), &gmajpb.ID{Id: id})
}

//
// Datastore RPC API
//

// GetRPC gets a value from a remote node's datastore for a given key.
func (node *Node) GetRPC(remoteNode *gmajpb.Node, key string) (string, error) {
	client, err := node.getChordClient(remoteNode)
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
func (node *Node) PutRPC(remoteNode *gmajpb.Node, key string, val string) error {
	client, err := node.getChordClient(remoteNode)
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
	remoteNode *gmajpb.Node, fromID []byte, toNode *gmajpb.Node,
) error {
	client, err := node.getChordClient(remoteNode)
	if err != nil {
		return err
	}

	_, err = client.TransferKeys(
		context.Background(), &gmajpb.TransferKeysReq{FromId: fromID, ToNode: toNode},
	)
	return err
}

//
// RPC connection map cache
//

type clientConn struct {
	client gmajpb.ChordClient
	conn   *grpc.ClientConn
}

// getChordClient is a helper function to make a call to a remote node.
func (node *Node) getChordClient(
	remoteNode *gmajpb.Node,
) (gmajpb.ChordClient, error) {
	// Dial the server if we don't already have a connection to it
	addr := remoteNode.Addr
	node.connMtx.RLock()
	cc, ok := node.clientConns[addr]
	node.connMtx.RUnlock()
	if ok {
		return cc.client, nil
	}

	conn, err := dial(addr, node.opts.dialOpts...)
	if err != nil {
		return nil, err
	}

	client := gmajpb.NewChordClient(conn)
	cc = &clientConn{client, conn}
	node.connMtx.Lock()
	if node.clientConns == nil {
		node.connMtx.Unlock()
		return nil, errors.New("must instantiate node before using")
	}
	node.clientConns[addr] = cc
	node.connMtx.Unlock()

	return client, nil
}

func dial(addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, append(append(
		config.DialOptions,
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
		grpc.FailOnNonTempDialError(true)),
		opts...,
	)...)
}
