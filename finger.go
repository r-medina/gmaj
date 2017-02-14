package gmaj

import (
	"bytes"
	"fmt"
	"math/big"

	"github.com/r-medina/gmaj/gmajpb"
)

type fingerTable []*fingerEntry

// fingerEntry represents a single finger table entry
type fingerEntry struct {
	StartID    []byte       // ID hash of (n + 2^i) mod (2^m)
	RemoteNode *gmajpb.Node // RemoteNode that Start points to
}

// newFingerEntry returns an allocated new finger entry with the attributes set
func newFingerEntry(startID []byte, remoteNode *gmajpb.Node) *fingerEntry {
	return &fingerEntry{
		StartID:    startID,
		RemoteNode: remoteNode,
	}
}

// initFingerTable creates initial finger table that only points to itself.
// The table will be fixed later.
func (node *Node) initFingerTable() {
	node.ftMtx.Lock()
	defer node.ftMtx.Unlock()

	node.fingerTable = make([]*fingerEntry, cfg.KeySize)
	for i := range node.fingerTable {
		node.fingerTable[i] = newFingerEntry(
			fingerMath(node.Id, i, cfg.KeySize),
			node.Node,
		)
	}
}

// fixNextFinger runs periodically (in a seperate go routine)
// to fix entries in our finger table.
func (node *Node) fixNextFinger(next int) int {
	nextHash := fingerMath(node.Id, next, cfg.KeySize)
	successorNode, err := node.findSuccessor(nextHash)
	if err != nil {
		return next
	}

	finger := newFingerEntry(nextHash, successorNode)
	node.ftMtx.Lock()
	node.fingerTable[next] = finger
	node.ftMtx.Unlock()

	return (next + 1) % cfg.KeySize
}

// fingerMath does the `(n + 2^i) mod (2^m)` operation
// needed to update finger table entries.
func fingerMath(n []byte, i int, m int) []byte {
	iInt := big.NewInt(2)
	iInt.Exp(iInt, big.NewInt(int64(i)), max)
	mInt := big.NewInt(2)
	mInt.Exp(mInt, big.NewInt(int64(m)), max)

	res := &big.Int{} // res will pretty much be an accumulator
	res.SetBytes(n).Add(res, iInt).Mod(res, mInt)

	return res.Bytes()
}

// FingerTableToString takes a node and converts it's finger table into a string.
func FingerTableToString(node *Node) string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("[%v] FingerTable:", IDToString(node.Id)))

	node.ftMtx.RLock()
	defer node.ftMtx.RUnlock()

	for _, val := range node.fingerTable {
		buf.WriteString(fmt.Sprintf(
			"\n\t{start:%v\tnodeID:%v %v}",
			IDToString(val.StartID),
			IDToString(val.RemoteNode.Id),
			val.RemoteNode.Addr,
		))
	}

	return buf.String()
}
