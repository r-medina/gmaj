//
//  contains the API and interal functions to interact with the Key-Value store
//  that the Chord ring is providing
//

package gmaj

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/r-medina/gmaj/gmajpb"
)

var errNoDatastore = errors.New("gmaj: node does not have a datastore")

//
// External API Into Datastore
//

// Get a value in the datastore, provided an abitrary node in the ring
func Get(node *Node, key string) (string, error) {
	if node == nil {
		return "", errors.New("Node cannot be nil")
	}

	remoteNode, err := node.locate(key)
	if err != nil {
		return "", err
	}

	// TODO(asubiotto): Smart retries on not found error. Implement channel
	// that notifies when stabilize has been called.

	// Retry on error because it might be due to temporary unavailability
	// (e.g. write happened while transferring nodes).
	value, err := node.GetRPC(remoteNode, key)
	if err != nil {
		<-time.After(config.RetryInterval)
		remoteNode, err = node.locate(key)
		if err != nil {
			return "", err
		}

		return node.GetRPC(remoteNode, key)
	}

	return value, nil
}

// Put a key/value in the datastore, provided an abitrary node in the ring.
// This is useful for testing.
func Put(node *Node, key string, value string) error {
	if node == nil {
		return errors.New("Node cannot be nil")
	}

	remoteNode, err := node.locate(key)
	if err != nil {
		return err
	}

	return node.PutRPC(remoteNode, key, value)
}

// locate helps find the appropriate node in the ring.
func (node *Node) locate(key string) (*gmajpb.Node, error) {
	return node.findSuccessor(hashKey(key))
}

// obtainNewKeys is called when a node joins a ring and wants to request keys
// from its successor.
func (node *Node) obtainNewKeys() error {
	node.succMtx.RLock()
	succ := node.Successor
	node.succMtx.RUnlock()

	// TODO(asubiotto): Test the case where there are two nodes floating around
	// that need keys.
	// Assume new predecessor has been set.
	prevPredecessor, err := node.GetPredecessorRPC(succ)
	if err != nil {
		return err
	}

	return node.TransferKeysRPC(
		succ, node.Id, prevPredecessor,
	) // implicitly correct even when prevPredecessor.ID == nil
}

//
// RPCs to assist with interfacing with the datastore ring
//

func (node *Node) get(key *gmajpb.Key) (string, error) {
	node.dsMtx.RLock()
	val, ok := node.datastore[key.Key]
	node.dsMtx.RUnlock()
	if !ok {
		return "", errors.New("key does not exist")
	}

	return val, nil
}

func (node *Node) put(keyVal *gmajpb.KeyVal) error {
	key := keyVal.Key
	val := keyVal.Val

	node.dsMtx.RLock()
	_, exists := node.datastore[key]
	node.dsMtx.RUnlock()
	if exists {
		return errors.New("cannot modify an existing value")
	}

	node.dsMtx.Lock()
	node.datastore[key] = val
	node.dsMtx.Unlock()

	return nil
}

func (node *Node) transferKeys(tmsg *gmajpb.TransferKeysReq) error {
	toNode := tmsg.ToNode
	if idsEqual(toNode.Id, node.Id) {
		return nil
	}

	node.dsMtx.Lock()
	defer node.dsMtx.Unlock()

	toDelete := []string{}
	for key, val := range node.datastore {
		hashedKey := hashKey(key)

		// Check that the hashed_key lies in the correct range before putting
		// the value in our predecessor.
		if betweenRightIncl(hashedKey, tmsg.FromId, toNode.Id) {
			if err := node.PutRPC(toNode, key, val); err != nil {
				return err
			}

			toDelete = append(toDelete, key)
		}
	}

	for _, key := range toDelete {
		delete(node.datastore, key)
	}

	return nil
}

// DatastoreString write the contents of a node's data store to stdout.
func (node *Node) DatastoreString() string {
	buf := bytes.Buffer{}

	node.dsMtx.RLock()
	defer node.dsMtx.RUnlock()

	buf.WriteString(fmt.Sprintf(
		"Node-%v datastore: %v\n",
		IDToString(node.Id), node.datastore,
	))

	return buf.String()
}
