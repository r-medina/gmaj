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

	return v[:IDLen]
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
	// Allow for wraparounds by checking that
	//  1) x > a and x < b when a < b or
	//  2) x < a and x < b when a > b or
	//  3) x > a and x > b when a > b or
	//  4) x < a or x > a when a == b
	return (bytes.Compare(x, a) > 0 &&
		bytes.Compare(x, b) < 0 &&
		bytes.Compare(a, b) < 0) ||
		(bytes.Compare(x, a) < 0 &&
			bytes.Compare(x, b) < 0 &&
			bytes.Compare(a, b) > 0) ||
		(bytes.Compare(x, a) > 0 &&
			bytes.Compare(x, b) > 0 &&
			bytes.Compare(a, b) > 0) ||
		((bytes.Compare(x, a) < 0 ||
			bytes.Compare(x, a) > 0) &&
			bytes.Equal(a, b))
}

// BetweenRightIncl is like Between, but includes the right boundary.
// That is, is x between (a : b]
func BetweenRightIncl(x, a, b []byte) bool {
	isBetween := Between(x, a, b)
	if !isBetween && bytes.Equal(x, b) {
		return true
	}

	return isBetween
}
