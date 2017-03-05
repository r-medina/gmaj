package gmaj

import (
	"bytes"
	"fmt"
	"math/big"

	"github.com/r-medina/gmaj/gmajpb"
)

type fingerTable []*fingerEntry

func newFingerTable(node *gmajpb.Node) fingerTable {
	ft := make([]*fingerEntry, config.KeySize)
	for i := range ft {
		ft[i] = newFingerEntry(fingerMath(node.Id, i, config.KeySize), node)
	}

	return ft
}

func (ft fingerTable) String() string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("FingerTable:"))

	for _, val := range ft {
		buf.WriteString(fmt.Sprintf(
			"\n\t{start:%v\tnodeID:%v %v}",
			IDToString(val.StartID),
			IDToString(val.RemoteNode.Id),
			val.RemoteNode.Addr,
		))
	}

	return buf.String()
}

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

// fixNextFinger runs periodically (in a seperate go routine)
// to fix entries in our finger table.
func (node *Node) fixNextFinger(next int) int {
	nextHash := fingerMath(node.Id, next, config.KeySize)
	succ, err := node.findSuccessor(nextHash)
	if err != nil {
		// TODO: handle failed client here
		return next
	}

	finger := newFingerEntry(nextHash, succ)
	node.ftMtx.Lock()
	node.fingerTable[next] = finger
	node.ftMtx.Unlock()

	return (next + 1) % config.KeySize
}

// FingerTableString takes a node and converts it's finger table into a string.
func (node *Node) FingerTableString() string {
	node.ftMtx.RLock()
	defer node.ftMtx.RUnlock()

	return node.fingerTable.String()
}

// fingerMath does the `(n + 2^i) mod (2^m)` operation
// needed to update finger table entries.
func fingerMath(n []byte, i int, m int) []byte {
	iInt := big.NewInt(2)
	iInt.Exp(iInt, big.NewInt(int64(i)), config.max)
	mInt := big.NewInt(2)
	mInt.Exp(mInt, big.NewInt(int64(m)), config.max)

	res := &big.Int{} // res will pretty much be an accumulator
	res.SetBytes(n).Add(res, iInt).Mod(res, mInt)

	return padID(res.Bytes())
}
