package gmaj

import (
	"bytes"
	"fmt"
	"math"
	"math/big"
)

type fingerTable []*fingerEntry

// fingerEntry represents a single finger table entry
type fingerEntry struct {
	StartID    []byte      // ID hash of (n + 2^i) mod (2^m)
	RemoteNode *RemoteNode // RemoteNode that Start points to
}

// NewFingerEntry returns an allocated new finger entry with the attributes set
func NewFingerEntry(startID []byte, remoteNode *RemoteNode) *fingerEntry {
	return &fingerEntry{startID, remoteNode}
}

// initFingerTable creates initial finger table that only points to itself.
// The table will be fixed later.
func (node *Node) initFingerTable() {
	node.fingerTable = make([]*fingerEntry, KeyLength)
	for i := range node.fingerTable {
		node.ftMtx.Lock()
		node.fingerTable[i] =
			NewFingerEntry(fingerMath(node.remoteNode.Id, i, KeyLength), node.remoteNode)
		node.ftMtx.Unlock()
	}
}

// fixNextFinger runs periodically (in a seperate go routine)
// to fix entries in our finger table.
func (node *Node) fixNextFinger(next int) int {
	nextHash := fingerMath(node.remoteNode.Id, next, KeyLength)
	successorNode, err := node.findSuccessor(nextHash)
	if err != nil {
		return next
	}

	node.ftMtx.Lock()
	node.fingerTable[next] = NewFingerEntry(nextHash, successorNode)
	node.ftMtx.Unlock()

	next++
	if next > KeyLength-1 {
		return 0
	}

	return next
}

// fingerMath does the `(n + 2^i) mod (2^m)` operation
// needed to update finger table entries.
func fingerMath(n []byte, i int, m int) []byte {
	iInt := big.NewInt(int64(math.Pow(2, float64(i))))
	mInt := big.NewInt(int64(math.Pow(2, float64(m))))

	res := &big.Int{} // res will pretty much be an accumulator
	res.SetBytes(n)
	res = res.Add(res, iInt)
	res = res.Mod(res, mInt)

	return res.Bytes()
}

// FingerTableToString takes a node and converts it's finger table into a string.
func FingerTableToString(node *Node) string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("[%v] FingerTable:", IDToString(node.remoteNode.Id)))
	node.ftMtx.RLock()
	defer node.ftMtx.RUnlock()
	for _, val := range node.fingerTable {
		buf.WriteString(fmt.Sprintf(
			"\n\t{start:%v\tnodeLoc:%v %v}",
			IDToString(val.StartID),
			IDToString(val.RemoteNode.Id),
			val.RemoteNode.Addr,
		))
	}

	return buf.String()
}
