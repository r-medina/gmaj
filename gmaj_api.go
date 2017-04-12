package gmaj

import (
	"github.com/r-medina/gmaj/gmajpb"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

//
// public API
//

// GetID returns the ID of the node.
func (node *Node) GetID(ctx context.Context, _ *gmajpb.GetIDRequest) (*gmajpb.GetIDResponse, error) {
	Log.Println("calling GetID")

	return &gmajpb.GetIDResponse{Id: node.Id}, nil
}

// Locate finds where a key belongs.
func (node *Node) Locate(ctx context.Context, req *gmajpb.LocateRequest) (*gmajpb.LocateResponse, error) {
	Log.Println("calling Locate")

	location, err := node.locate(req.Key)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "could not locate key: %v", err)
	}

	return &gmajpb.LocateResponse{Node: location}, nil
}

// Get a value in the datastore, provided an abitrary node in the ring
func (node *Node) Get(ctx context.Context, req *gmajpb.GetRequest) (*gmajpb.GetResponse, error) {
	Log.Println("calling Get")

	val, err := node.get(req.Key)
	if err != nil {
		return nil, grpc.Errorf(codes.Internal, "could not get key: %v", err)
	}

	return &gmajpb.GetResponse{Value: val}, nil
}

// Put a key/value in the datastore, provided an abitrary node in the ring.
// This is useful for testing.
func (node *Node) Put(ctx context.Context, req *gmajpb.PutRequest) (*gmajpb.PutResponse, error) {
	Log.Println("calling Put")

	if err := node.put(req.Key, req.Value); err != nil {
		return nil, grpc.Errorf(codes.Internal, "could not put key value pair: %v", err)
	}

	return &gmajpb.PutResponse{}, nil
}
