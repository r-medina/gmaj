//
//  contains the API and interal functions to interact with the Key-Value store
//  that the Chord ring is providing
//

package gmaj

import (
	"errors"
	"fmt"
	"time"
)

var errNoDatastore = errors.New("Node does not have a datastore")

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

	// TODO(asubiotto): Smart retries on error. Implement channel that notifies
	// when stabilize has been called.

	// Retry on error because it might be due to temporary unavailability
	// (e.g. write happened while transferring nodes).
	value, err := GetRPC(remoteNode, key)
	if err != nil {
		<-time.After(time.Second)
		remoteNode, err = node.locate(key)
		if err != nil {
			return "", err
		}
		return GetRPC(remoteNode, key)
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

	return PutRPC(remoteNode, key, value)
}

// locate helps find the appropriate node in the ring.
func (node *Node) locate(key string) (*RemoteNode, error) {
	return FindSuccessorRPC(node.remoteNode, HashKey(key))
}

// obtainNewKeys is called when a node joins a ring and wants to request keys
// from its successor.
func (node *Node) obtainNewKeys() error {
	node.succMtx.RLock()
	defer node.succMtx.RUnlock()

	// TODO(asubiotto): Test the case where there are two nodes floating around
	// that need keys.
	// Assume new predecessor has been set.
	prevPredecessor, err := GetPredecessorRPC(node.Successor)
	if err != nil {

		return err
	}

	return TransferKeysRPC(
		node.Successor,
		node.remoteNode.Id,
		prevPredecessor,
	) // implicitly correct even when prevPredecessor.ID == nil
}

//
// RPCs to assist with interfacing with the datastore ring
//

func (node *Node) get(k *Key) (string, error) {
	if node.dataStore == nil {
		return "", errNoDatastore
	}

	node.dsMtx.RLock()
	val, ok := node.dataStore[k.Key]
	node.dsMtx.RUnlock()
	if !ok {
		return "", errors.New("Key does not exist")
	}

	return val, nil
}

func (node *Node) put(kv *KeyVal) error {
	if node.dataStore == nil {
		return errNoDatastore
	}

	node.dsMtx.RLock()
	_, exists := node.dataStore[kv.Key]
	node.dsMtx.RUnlock()
	if exists {
		return errors.New("Cannot modify an existing value")
	}

	node.dsMtx.Lock()
	node.dataStore[kv.Key] = kv.Val
	node.dsMtx.Unlock()

	return nil
}

func (node *Node) transferKeys(tmsg *TransferMsg) error {
	node.dsMtx.Lock()
	defer node.dsMtx.Unlock()

	toNode := tmsg.ToNode
	for key := range node.dataStore {
		hashedKey := HashKey(key)

		// Check that the hashed_key lies in the correct range before putting
		// the value in our predecessor.
		if BetweenRightIncl(hashedKey, tmsg.FromID, toNode.Id) {
			// Only put if node is not ourselves.
			if toNode.Addr != node.remoteNode.Addr {
				err := PutRPC(toNode, key, node.dataStore[key])
				if err != nil {
					return err
				}
			}

			// Remove entry from dataStore
			delete(node.dataStore, key)
		}
	}

	return nil
}

// PrintDataStore write the contents of a node's data store to stdout.
func PrintDataStore(node *Node) {
	node.dsMtx.RLock()
	fmt.Printf("Node-%v datastore: %v\n", IDToString(node.remoteNode.Id), node.dataStore)
	node.dsMtx.RUnlock()
}
