package gmaj

import (
	"testing"
	"time"
)

func TestGetNilNode(t *testing.T) {
	_, err := Get(nil, "")
	if err == nil {
		t.Fatal("Unexpected success getting value from nil node")
	}
}

func TestGetNoDataStore(t *testing.T) {
	node := new(Node)
	_, err := node.get(new(Key))
	if err == nil {
		t.Fatal("Unexpected success getting value from nil datastore")
	}
}

func TestGetNonExistentKey(t *testing.T) {
	node, _ := NewNode(nil)
	_, err := Get(node, "test")
	if err == nil {
		t.Fatal("Unexpected success getting non-existent key")
	}

	// Make sure entry was not created.
	if _, exists := node.dataStore["test"]; exists {
		t.Fatal("Unexpected entry in node datastore")
	}
}

func TestGetKey(t *testing.T) {
	node, _ := NewNode(nil)

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
	err := Put(nil, "", "")
	if err == nil {
		t.Fatal("Unexpected success putting value in nil node")
	}
}

func TestPutNoDataStore(t *testing.T) {
	node := new(Node)
	err := Put(node, "", "")
	if err == nil {
		t.Fatal("Unexpected success putting value in nil datastore")
	}
}

func TestPutModifyExistingKey(t *testing.T) {
	node, _ := NewNode(nil)

	err := Put(node, "test", "value")
	if err != nil {
		t.Fatalf("Unexpected failure putting value: %v", err)
	}

	if _, exists := node.dataStore["test"]; !exists {
		t.Fatal("Unexpected error value not set")
	}

	err = Put(node, "test", "value2")

	if err == nil {
		t.Fatal("Unexpected success modifying immutable key")
	}

	// Make sure value was not modified.
	if value, _ := Get(node, "test"); value == "value2" {
		t.Fatal("Unexpected entry in node datastore")
	}
}

func TestPutKey(t *testing.T) {
	node, _ := NewNode(nil)

	if err := Put(node, "test", "value"); err != nil {
		t.Fatalf("Unexpected error putting value: %v", err)
	}

	if value, _ := Get(node, "test"); value != "value" {
		t.Fatalf("Unexpected entry in node datastore")
	}
}

func TestTransferKeys(t *testing.T) {
	key := "myKey"
	hashedKey := HashKey(key)

	// Make node that will be successor to hashed_key.
	hashedKey[0] += 2
	node1, _ := NewDefinedNode(nil, hashedKey)

	if err := Put(node1, key, "spacetravel!"); err != nil {
		t.Fatalf("Unexpected error putting value: %v", err)
	}

	// Make node that should get the key transferred to it.
	hashedKey = HashKey(key)
	hashedKey[0]++
	node2, _ := NewDefinedNode(node1.RemoteNode(), hashedKey)

	<-time.After(666 * time.Millisecond)

	// Make sure that "spacetravel!" is in node2.
	if val, err := node2.get(&Key{Key: key}); err != nil {
		t.Fatalf("Unexpected error getting value from node2: %v", err)
	} else if val != "spacetravel!" {
		t.Fatalf("Unexpected value")
	}
}

func TestTransferKeysAvailability(t *testing.T) {
	// Tests that key stays available during transfer.
	key := "myKey"
	hashedKey := HashKey(key)

	// Make node that will be successor to hashed_key.
	hashedKey[0] += 2
	node1, _ := NewDefinedNode(nil, hashedKey)

	if err := Put(node1, key, "spacetravel!"); err != nil {
		t.Fatalf("Unexpected error putting value:%v\n", err)
	}

	// Start up goroutine to check key availability.
	go func() {
		for {
			value, err := Get(node1, key)
			if err != nil {
				t.Fatalf("Unexpected error checking key availability:%v\n", err)
				return
			} else if value != "spacetravel!" {
				t.Fatalf("Unexpected value")
				return
			}
		}
	}()

	// Make node that should get the key transferred to it.
	hashedKey = HashKey(key)
	hashedKey[0]++
	node2, _ := NewDefinedNode(node1.RemoteNode(), hashedKey)

	<-time.After(666 * time.Millisecond)

	// Make sure that "spacetravel!" is in node2.
	if val, err := node2.get(&Key{Key: key}); err != nil {
		t.Fatalf("Unexpected error getting value from node2:%v\n", err)
	} else if val != "spacetravel!" {
		t.Fatalf("Unexpected value")
	}
	<-time.After(5 * time.Second)
}

// This tests a bug we were experiencing where values from the initial node were note
// being transfered over upon its shutdown.
func TestKeyTransferAfterShutdown(t *testing.T) {
	node1, node2, node3 := create3SuccessiveNodes(t)

	time.Sleep(time.Millisecond * 200)

	Put(node1, "a", "1")
	Put(node1, "b", "2")
	Put(node1, "c", "3")
	Put(node1, "d", "4")
	Put(node2, "e", "5")
	Put(node2, "f", "6")
	Put(node3, "g", "7")

	// stores all the data in chord
	data := make(map[string]string)
	for k, v := range node1.dataStore {
		data[k] = v
	}
	for k, v := range node2.dataStore {
		data[k] = v
	}
	for k, v := range node3.dataStore {
		data[k] = v
	}

	node1.Shutdown()

	// makes sure node1 transfered its keys
	if l := len(node1.dataStore); l > 0 {
		t.Fatalf(
			"node1 should not have anything left in its data, but there are %v items\n", l,
		)
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
	if l := len(node2.dataStore); l > 0 {
		t.Fatalf(
			"node2 should not have anything left in its data, but there are %v items\n", l,
		)
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

	definedID := make([]byte, cfg.IDLength)
	node4 := createDefinedNode(t, node3.RemoteNode(), definedID)

	<-time.After(200 * time.Millisecond)

	node3.Shutdown()

	// makes sure node3 transfered its keys
	if l := len(node3.dataStore); l > 0 {
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
