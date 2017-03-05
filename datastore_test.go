package gmaj

import (
	"testing"
	"time"

	"github.com/r-medina/gmaj/gmajpb"
)

func TestGetNilNode(t *testing.T) {
	t.Parallel()

	_, err := Get(nil, "")
	if err == nil {
		t.Fatal("Unexpected success getting value from nil node")
	}
}

func TestGetNoDataStore(t *testing.T) {
	t.Parallel()

	node := &Node{Node: new(gmajpb.Node)}
	_, err := node.get(new(gmajpb.Key))
	if err == nil {
		t.Fatal("Unexpected success getting value from nil datastore")
	}
}

func TestGetNonExistentKey(t *testing.T) {
	t.Parallel()

	node, err := NewNode(nil, "")
	if err != nil {
		t.Fatalf("unexpected error making node: %v", err)
	}
	if _, err := Get(node, "test"); err == nil {
		t.Fatal("Unexpected success getting non-existent key")
	}

	// Make sure entry was not created.
	if _, exists := node.datastore["test"]; exists {
		t.Fatal("Unexpected entry in node datastore")
	}
}

func TestGetKey(t *testing.T) {
	t.Parallel()

	node, err := NewNode(nil, "")
	if err != nil {
		t.Fatalf("unexpected error making node: %v", err)
	}
	if err := Put(node, "test", "value"); err != nil {
		t.Fatalf("Unexpected error putting value: %v", err)
	}

	value, err := Get(node, "test")
	if err != nil {
		t.Fatalf("Unexpected error getting value: %v", err)
	}

	if value != "value" {
		t.Fatalf("Unexpected value returned. Expected 'value' got %q", value)
	}
}

func TestPutNilNode(t *testing.T) {
	t.Parallel()

	err := Put(nil, "", "")
	if err == nil {
		t.Fatal("Unexpected success putting value in nil node")
	}
}

func TestPutNoDataStore(t *testing.T) {
	t.Parallel()

	node := &Node{Node: new(gmajpb.Node)}
	err := Put(node, "", "")
	if err == nil {
		t.Fatal("Unexpected success putting value in nil datastore")
	}
}

func TestPutModifyExistingKey(t *testing.T) {
	t.Parallel()

	node, err := NewNode(nil, "")
	if err != nil {
		t.Fatalf("unexpected error making node: %v", err)
	}
	if err := Put(node, "test", "value"); err != nil {
		t.Fatalf("Unexpected failure putting value: %v", err)
	}

	if _, exists := node.datastore["test"]; !exists {
		t.Fatal("Unexpected error value not set")
	}

	if err = Put(node, "test", "value2"); err == nil {
		t.Fatal("Unexpected success modifying immutable key")
	}

	// Make sure value was not modified.
	if value, _ := Get(node, "test"); value == "value2" {
		t.Fatal("Unexpected entry in node datastore")
	}
}

func TestPutKey(t *testing.T) {
	t.Parallel()

	node, err := NewNode(nil, "")
	if err != nil {
		t.Fatalf("unexpected error making new node: %v", err)
	}

	key, want := "test", "value"
	if err := Put(node, key, want); err != nil {
		t.Fatalf("Unexpected error putting value %q with key %q: %v", want, key, err)
	}

	got, err := Get(node, "test")
	if err != nil {
		t.Fatalf("unexpected error getting value with key %q: %v", key, err)
	}

	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestTransferKeys(t *testing.T) {
	t.Parallel()
	key := "myKey"
	hashedKey := hashKey(key)

	// Make node that will be successor to hashed_key.
	hashedKey[0] += 2
	node1 := createDefinedNode(t, nil, hashedKey)
	if err := Put(node1, key, "spacetravel!"); err != nil {
		t.Fatalf("Unexpected error putting value: %v", err)
	}

	// Make node that should get the key transferred to it.
	hashedKey = hashKey(key)
	hashedKey[0]++
	node2 := createDefinedNode(t, node1.Node, hashedKey)

	<-time.After(testTimeout)

	// Make sure that "spacetravel!" is in node2.
	if val, err := node2.get(&gmajpb.Key{Key: key}); err != nil {
		t.Fatalf("Unexpected error getting value from node2: %v", err)
	} else if val != "spacetravel!" {
		t.Fatalf("Unexpected value")
	}
}

func TestTransferKeysAvailability(t *testing.T) {
	// Tests that key stays available during transfer.
	key := "myKey"
	hashedKey := hashKey(key)

	// Make node that will be successor to hashed_key.
	hashedKey[0] += 2
	node1 := createDefinedNode(t, nil, hashedKey)
	if err := Put(node1, key, "spacetravel!"); err != nil {
		t.Fatalf("Unexpected error putting value:%v\n", err)
	}

	done := make(chan struct{})

	// Start up goroutine to check key availability.
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				value, err := Get(node1, key)
				if err != nil {
					t.Fatalf("Unexpected error checking key availability: %v\n", err)
					return
				} else if value != "spacetravel!" {
					t.Fatalf("Unexpected value")
					return
				}
			}
		}
	}()

	// Make node that should get the key transferred to it.
	hashedKey = hashKey(key)
	hashedKey[0]++
	node2 := createDefinedNode(t, node1.Node, hashedKey)

	<-time.After(testTimeout)

	// Make sure that "spacetravel!" is in node2.
	if val, err := node2.get(&gmajpb.Key{Key: key}); err != nil {
		t.Fatalf("Unexpected error getting value from node2:%v\n", err)
	} else if val != "spacetravel!" {
		t.Fatalf("Unexpected value")
	}
	<-time.After(testTimeout)

	close(done)
}

func TestKeyTransferAfterShutdownSimple(t *testing.T) {
	t.Parallel()

	node1 := createSimpleNode(t, nil)

	Put(node1, "1", "1")
	Put(node1, "2", "2")
	Put(node1, "3", "3")
	Put(node1, "4", "4")
	Put(node1, "5", "5")
	Put(node1, "6", "6")
	Put(node1, "7", "7")

	// stores all the data in chord
	data := make(map[string]string)
	for k, v := range node1.datastore {
		data[k] = v
	}

	node2 := createSimpleNode(t, node1.Node)
	<-time.After(testTimeout)

	// makes sure that all the data is still available somewhere
	for k, vExp := range data {
		vRet, err := Get(node2, k)
		if err != nil {
			t.Fatalf("Unexpected error getting value from node2 getting key %v: %v\n", k, err)
		}

		if vRet != vExp {
			t.Fatalf("Unexpected return value. Expected %v, got %v\n", vExp, vRet)
		}
	}
}

// This tests a bug we were experiencing where values from the initial node were
// not being transfered over upon its shutdown.
func TestKeyTransferAfterShutdown(t *testing.T) {
	t.Parallel()

	node1, node2, node3 := create3SuccessiveNodes(t)

	<-time.After(testTimeout << 1)

	_ = Put(node1, "a", "1")
	_ = Put(node1, "b", "2")
	_ = Put(node1, "c", "3")
	_ = Put(node1, "d", "4")
	_ = Put(node2, "e", "5")
	_ = Put(node2, "f", "6")
	_ = Put(node3, "g", "7")

	// stores all the data in chord
	data := make(map[string]string)
	for k, v := range node1.datastore {
		data[k] = v
	}
	for k, v := range node2.datastore {
		data[k] = v
	}
	for k, v := range node3.datastore {
		data[k] = v
	}

	node1.Shutdown()

	// makes sure node1 transfered its keys
	if l := len(node1.datastore); l > 0 {
		t.Fatalf("node1 should not have anything left in its data, but there are %v items\n", l)
	}

	// makes sure that all the data is still available somewhere
	for k, vExp := range data {
		vRet, err := Get(node2, k)
		if err != nil {
			t.Fatalf("Unexpected error getting value from node2: %v\n", err)
		}

		if vRet != vExp {
			t.Fatalf("Unexpected return value. Expected %v, got %v\n", vExp, vRet)
		}
	}

	node2.Shutdown()

	// makes sure node3 transfered its keys
	if l := len(node2.datastore); l > 0 {
		t.Fatalf("node2 should not have anything left in its data, but there are %v items\n", l)
	}

	// makes sure that all the data is still available somewhere
	for k, vExp := range data {
		vRet, err := Get(node3, k)
		if err != nil {
			t.Fatalf("Unexpected error getting value from node3: %v\n", err)
		}

		if vRet != vExp {
			t.Fatalf("Unexpected return value. Expected %v, got %v\n", vExp, vRet)
		}
	}

	definedID := make([]byte, config.IDLength)
	node4 := createDefinedNode(t, node3.Node, definedID)

	<-time.After(testTimeout)

	node3.Shutdown()

	// makes sure node3 transfered its keys
	if l := len(node3.datastore); l > 0 {
		t.Fatalf(
			"node3 should not have anything left in its data, but there are %v items\n", l,
		)
	}

	// makes sure that all the data is still available somewhere
	for k, vExp := range data {
		vRet, err := Get(node4, k)
		if err != nil {
			t.Fatalf("Unexpected error getting value from node4: %v\n", err)
		}

		if vRet != vExp {
			t.Fatalf("Unexpected return value. Expected %v, got %v\n", vExp, vRet)
		}
	}
}
