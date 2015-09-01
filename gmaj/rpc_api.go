package gmaj

import (
	"sync"

	pb "github.com/r-medina/gmaj"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

//
// RPC connection map cache
//

var (
	connMap = make(map[string]*clientConn)
	connMtx = sync.RWMutex{}
)

type clientConn struct {
	client pb.NodeClient
	conn   *grpc.ClientConn
}

//
// Chord Node RPC API
//

// GetPredecessorRPC gets the predecessor ID of a remote node.
func GetPredecessorRPC(remoteNode *pb.RemoteNode) (*pb.RemoteNode, error) {
	client, err := getNodeClient(remoteNode)
	if err != nil {
		return nil, err
	}

	return client.GetPredecessor(context.Background(), &pb.Nil{})
}

// GetSuccessorRPC the successor ID of a remote node.
func GetSuccessorRPC(remoteNode *pb.RemoteNode) (*pb.RemoteNode, error) {
	client, err := getNodeClient(remoteNode)
	if err != nil {
		return nil, err
	}

	return client.GetSuccessor(context.Background(), &pb.Nil{})
}

// SetPredecessorRPC noties a remote node that we believe we are its predecessor.
func SetPredecessorRPC(remoteNode, newPred *pb.RemoteNode) error {
	client, err := getNodeClient(remoteNode)
	if err != nil {
		return err
	}

	if _, err = client.SetPredecessor(context.Background(), newPred); err != nil {
		return err
	}

	return nil
}

// SetSuccessorRPC sets the successor ID of a remote node.
func SetSuccessorRPC(remoteNode, newSucc *pb.RemoteNode) error {
	client, err := getNodeClient(remoteNode)
	if err != nil {
		return err
	}

	if _, err = client.SetSuccessor(context.Background(), newSucc); err != nil {
		return err
	}

	return nil
}

// Notify a remote node that pred is its predecessor
func NotifyRPC(remoteNode, pred *pb.RemoteNode) error {
	client, err := getNodeClient(remoteNode)
	if err != nil {
		return err
	}

	if _, err = client.Notify(context.Background(), pred); err != nil {
		return err
	}

	return nil
}

// Find the closest preceding finger from a remote node for an ID
func ClosestPrecedingFingerRPC(
	remoteNode *pb.RemoteNode, id []byte,
) (*pb.RemoteNode, error) {
	client, err := getNodeClient(remoteNode)
	if err != nil {
		return nil, err
	}

	return client.ClosestPrecedingFinger(context.Background(), &pb.ID{id})
}

// Find the successor node of a given ID in the entire ring
func FindSuccessorRPC(
	remoteNode *pb.RemoteNode, id []byte,
) (*pb.RemoteNode, error) {
	client, err := getNodeClient(remoteNode)
	if err != nil {
		return nil, err
	}

	return client.FindSuccessor(context.Background(), &pb.ID{id})
}

//
// Datastore RPC API
//

// Get a value from a remote node's datastore for a given key
func GetRPC(remoteNode *pb.RemoteNode, key string) (string, error) {
	client, err := getNodeClient(remoteNode)
	if err != nil {
		return "", err
	}

	val, err := client.Get(context.Background(), &pb.Key{key})
	if err != nil {
		return "", err
	}

	return val.Val, nil
}

// Put a key/value into a datastore on a remote node
func PutRPC(remoteNode *pb.RemoteNode, key string, val string) error {
	client, err := getNodeClient(remoteNode)
	if err != nil {
		return err
	}

	if _, err := client.Put(context.Background(), &pb.KeyVal{key, val}); err != nil {
		return err
	}

	return nil
}

// Inform a successor node that we should now take care of
// IDs between (node.Id : predId]. This should trigger the
// successor node to transfer the relevant keys back to node
func TransferKeysRPC(
	remoteNode *pb.RemoteNode, fromID []byte, toNode *pb.RemoteNode,
) error {
	client, err := getNodeClient(remoteNode)
	if err != nil {
		return err
	}

	_, err = client.TransferKeys(context.Background(), &pb.TransferMsg{fromID, toNode})
	if err != nil {
		return err
	}

	return nil
}

// Helper function to make a call to a remote node
func getNodeClient(remoteNode *pb.RemoteNode) (pb.NodeClient, error) {
	// Dial the server if we don't already have a connection to it
	remoteNodeAddr := remoteNode.Addr
	connMtx.RLock()
	cc, ok := connMap[remoteNodeAddr]
	connMtx.RUnlock()
	if !ok {
		conn, err := grpc.Dial(remoteNodeAddr)
		if err != nil {
			return nil, err
		}

		client := pb.NewNodeClient(conn)
		cc = &clientConn{client, conn}
		connMtx.Lock()
		connMap[remoteNodeAddr] = cc
		connMtx.Unlock()
	}

	return cc.client, nil
}
