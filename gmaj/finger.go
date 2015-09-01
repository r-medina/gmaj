package gmaj

import (
	"fmt"
	"math"
	"math/big"

	pb "github.com/r-medina/gmaj"
)

// FingerEntry represents a single finger table entry
type FingerEntry struct {
	StartID    []byte         // ID hash of (n + 2^i) mod (2^m)
	RemoteNode *pb.RemoteNode // RemoteNode that Start points to
}

// NewFingerEntry returns an allocated new finger entry with the attributes set
func NewFingerEntry(startID []byte, remoteNode *pb.RemoteNode) *FingerEntry {
	return &FingerEntry{startID, remoteNode}
}

// initFingerTable creates initial finger table that only points to itself.
// The table will be fixed later.
func (node *Node) initFingerTable() {
	node.FingerTable = make([]*FingerEntry, KeyLength)
	for i := range node.FingerTable {
		node.ftMtx.Lock()
		node.FingerTable[i] =
			NewFingerEntry(fingerMath(node.Id, i, KeyLength), node.RemoteNode)
		node.ftMtx.Unlock()
	}
}

// fixNextFinger runs periodically (in a seperate go routine)
// to fix entries in our finger table.
func (node *Node) fixNextFinger(next int) int {
	nextHash := fingerMath(node.Id, next, KeyLength)
	successorNode, err := node.findSuccessor(nextHash)
	if err != nil {
		return next
	}

	node.ftMtx.Lock()
	node.FingerTable[next] = NewFingerEntry(nextHash, successorNode)
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

// PrintFingerTable writes the contents of a node's finger table to stdout.
func PrintFingerTable(node *Node) {
	fmt.Printf("[%v] FingerTable:\n", IDToString(node.Id))
	node.ftMtx.RLock()
	defer node.ftMtx.RUnlock()
	for _, val := range node.FingerTable {
		fmt.Printf(
			"\t{start:%v\tnodeLoc:%v %v}\n",
			IDToString(val.StartID), IDToString(val.RemoteNode.Id), val.RemoteNode.Addr,
		)
	}
}
