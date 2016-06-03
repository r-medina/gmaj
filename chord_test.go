package gmaj

import (
	"testing"
)

func TestSimple(t *testing.T) {
	_, err := NewNode(nil)
	if err != nil {
		t.Errorf("Unable to create node, received error:%v\n", err)
	}
}

func TestErrorCreationNodeExistingID(t *testing.T) {
	node := createSimpleNode(t, nil)
	if _, err := NewDefinedNode(node.RemoteNode(), node.ID()); err == nil {
		t.Errorf("Unexpected success creating a node with invalid id")
	}
}
