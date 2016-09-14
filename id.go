//
//  utility functions to help with dealing with ID hashes in Chord
//

package gmaj

import (
	"bytes"
	"crypto/sha1"
	"math/big"
)

// HashKey hashes a string to its appropriate size.
func HashKey(key string) []byte {
	h := sha1.New()
	h.Write([]byte(key))
	v := h.Sum(nil)

	return v[:cfg.IDLength]
}

// IDToString converts a []byte to a big.Int string, useful for debugging/logging.
func IDToString(id []byte) string {
	keyInt := big.Int{}
	keyInt.SetBytes(id)

	return keyInt.String()
}

// IDsEqual returns if a and b are equal.
func IDsEqual(a, b []byte) bool {
	return bytes.Equal(a, b)
}

// AddIDs adds two IDs.
func AddIDs(a, b []byte) []byte {
	aInt := big.Int{}
	aInt.SetBytes(a)

	bInt := big.Int{}
	bInt.SetBytes(b)

	sum := big.Int{}
	sum.Add(&aInt, &bInt)
	return sum.Bytes()
}

// Between returns if x is between a and b.
// On this crude ascii Chord ring, x is between (a : b)
//        ___
//       /   \-a
//      |     |
//     b-\   /-x
//        ---
func Between(x, a, b []byte) bool {
	xInt := (&big.Int{}).SetBytes(x)
	aInt := (&big.Int{}).SetBytes(a)
	bInt := (&big.Int{}).SetBytes(b)

	// Allow for wraparounds by checking that
	//  1) x > a and x < b when a < b or
	//  2) x < a and x < b when a > b or
	//  3) x > a and x > b when a > b or
	//  4) x < a or x > a when a == b
	switch aInt.Cmp(bInt) {
	case -1:
		return (xInt.Cmp(aInt) > 0) && (xInt.Cmp(bInt) < 0)
	case 1:
		return (xInt.Cmp(aInt) < 0) && (xInt.Cmp(bInt) > 0)
	case 0:
		return xInt.Cmp(aInt) != 0
	}

	return false
}

// BetweenRightIncl is like Between, but includes the right boundary.
// That is, is x between (a : b]
func BetweenRightIncl(x, a, b []byte) bool {
	return Between(x, a, b) || bytes.Equal(x, b)
}
