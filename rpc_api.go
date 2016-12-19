package gmaj

import (
	"sync"

	"github.com/r-medina/gmaj/gmajpb"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

//
// RPC connection map cache
//

var (
	clientConns = make(map[string]*clientConn)
	connMtx     = sync.RWMutex{}
)

type clientConn struct {
	client gmajpb.NodeClient
	conn   *grpc.ClientConn
}

//
// Chord Node RPC API
//

// GetPredecessorRPC gets the predecessor ID of a remote node.
func GetPredecessorRPC(remoteNode *gmajpb.RemoteNode, dialOpts ...grpc.DialOption) (*gmajpb.RemoteNode, error) {
	client, err := getNodeClient(remoteNode, dialOpts...)
	if err != nil {
		return nil, err
	}

	return client.GetPredecessor(context.Background(), mt)
}

// GetSuccessorRPC the successor ID of a remote node.
func GetSuccessorRPC(remoteNode *gmajpb.RemoteNode, dialOpts ...grpc.DialOption) (*gmajpb.RemoteNode, error) {
	client, err := getNodeClient(remoteNode, dialOpts...)
	if err != nil {
		return nil, err
	}

	return client.GetSuccessor(context.Background(), mt)
}

// SetPredecessorRPC noties a remote node that we believe we are its predecessor.
func SetPredecessorRPC(remoteNode, newPred *gmajpb.RemoteNode, dialOpts ...grpc.DialOption) error {
	client, err := getNodeClient(remoteNode, dialOpts...)
	if err != nil {
		return err
	}

	if _, err = client.SetPredecessor(context.Background(), newPred); err != nil {
		return err
	}

	return nil
}

// SetSuccessorRPC sets the successor ID of a remote node.
func SetSuccessorRPC(remoteNode, newSucc *gmajpb.RemoteNode, dialOpts ...grpc.DialOption) error {
	client, err := getNodeClient(remoteNode, dialOpts...)
	if err != nil {
		return err
	}

	if _, err = client.SetSuccessor(context.Background(), newSucc); err != nil {
		return err
	}

	return nil
}

// NotifyRPC notifies a remote node that pred is its predecessor.
func NotifyRPC(remoteNode, pred *gmajpb.RemoteNode, dialOpts ...grpc.DialOption) error {
	client, err := getNodeClient(remoteNode, dialOpts...)
	if err != nil {
		return err
	}

	if _, err = client.Notify(context.Background(), pred); err != nil {
		return err
	}

	return nil
}

// ClosestPrecedingFingerRPC finds the closest preceding finger from a remote
// node for an ID.
func ClosestPrecedingFingerRPC(
	remoteNode *gmajpb.RemoteNode, id []byte, dialOpts ...grpc.DialOption,
) (*gmajpb.RemoteNode, error) {
	client, err := getNodeClient(remoteNode, dialOpts...)
	if err != nil {
		return nil, err
	}

	return client.ClosestPrecedingFinger(context.Background(), &gmajpb.ID{Id: id})
}

// FindSuccessorRPC finds the successor node of a given ID in the entire ring.
func FindSuccessorRPC(
	remoteNode *gmajpb.RemoteNode, id []byte, dialOpts ...grpc.DialOption,
) (*gmajpb.RemoteNode, error) {
	client, err := getNodeClient(remoteNode, dialOpts...)
	if err != nil {
		return nil, err
	}

	return client.FindSuccessor(context.Background(), &gmajpb.ID{Id: id})
}

//
// Datastore RPC API
//

// GetRPC gets a value from a remote node's datastore for a given key.
func GetRPC(remoteNode *gmajpb.RemoteNode, key string, dialOpts ...grpc.DialOption) (string, error) {
	client, err := getNodeClient(remoteNode, dialOpts...)
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
func PutRPC(remoteNode *gmajpb.RemoteNode, key string, val string, dialOpts ...grpc.DialOption) error {
	client, err := getNodeClient(remoteNode, dialOpts...)
	if err != nil {
		return err
	}

	if _, err := client.Put(context.Background(), &gmajpb.KeyVal{Key: key, Val: val}); err != nil {
		return err
	}

	return nil
}

// TransferKeysRPC informs a successor node that we should now take care of IDs
// between (node.Id : predId]. This should trigger the successor node to
// transfer the relevant keys back to node
func TransferKeysRPC(
	remoteNode *gmajpb.RemoteNode, fromID []byte, toNode *gmajpb.RemoteNode, dialOpts ...grpc.DialOption,
) error {
	client, err := getNodeClient(remoteNode, dialOpts...)
	if err != nil {
		return err
	}

	_, err = client.TransferKeys(context.Background(), &gmajpb.TransferMsg{FromID: fromID, ToNode: toNode})
	if err != nil {
		return err
	}

	return nil
}

// getNodeClient is a helper function to make a call to a remote node.
func getNodeClient(remoteNode *gmajpb.RemoteNode, dialOpts ...grpc.DialOption) (gmajpb.NodeClient, error) {
	// Dial the server if we don't already have a connection to it
	remoteNodeAddr := remoteNode.Addr
	connMtx.RLock()
	cc, ok := clientConns[remoteNodeAddr]
	connMtx.RUnlock()
	if ok {
		return cc.client, nil
	}

	conn, err := grpc.Dial(
		remoteNodeAddr,
		// only way to do per-node credentials I can think of...
		append(cfg.DialOptions, dialOpts...)...,
	)
	if err != nil {
		return nil, err
	}

	client := gmajpb.NewNodeClient(conn)
	cc = &clientConn{client, conn}
	connMtx.Lock()
	clientConns[remoteNodeAddr] = cc
	connMtx.Unlock()

	return client, nil
}
