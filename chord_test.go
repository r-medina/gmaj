package gmaj

import "testing"

func TestSimple(t *testing.T) {
	t.Parallel()

	_, err := NewNode(nil)
	if err != nil {
		t.Errorf("Unable to create node, received error:%v\n", err)
	}
}

func TestErrorCreationNodeExistingID(t *testing.T) {
	t.Parallel()

	node := createSimpleNode(t, nil)
	if _, err := NewNode(node.Node, WithID(node.Id)); err == nil {
		t.Errorf("Unexpected success creating a node with invalid id")
	}
}
