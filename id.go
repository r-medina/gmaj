//
//  utility functions to help with dealing with ID hashes in Chord
//

package gmaj

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"math/big"
)

// hashKey hashes a string to its appropriate size.
func hashKey(key string) ([]byte, error) {
	h := sha1.New()
	if _, err := h.Write([]byte(key)); err != nil {
		return nil, err
	}
	v := h.Sum(nil)

	return v[:config.IDLength], nil
}

// NewID takes a string representing
func NewID(str string) ([]byte, error) {
	i := big.NewInt(0)
	i.SetString(str, 0)
	id := i.Bytes()
	if len(id) == 0 {
		return nil, errors.New("gmaj: invalid ID")
	}

	return padID(id), nil
}

func padID(id []byte) []byte {
	n := config.IDLength - len(id)
	if n < 0 {
		n = 0
	}

	_id := make([]byte, n)
	id = append(_id, id...)

	return id[:config.IDLength]
}

// IDToString converts a []byte to a big.Int string, useful for debugging/logging.
func IDToString(id []byte) string {
	keyInt := big.Int{}
	keyInt.SetBytes(id)

	return keyInt.String()
}

// idsEqual returns if a and b are equal.
func idsEqual(a, b []byte) bool {
	return bytes.Equal(a, b)
}

// addIDs adds two IDs.
func addIDs(a, b []byte) []byte {
	aInt := big.Int{}
	aInt.SetBytes(a)

	bInt := big.Int{}
	bInt.SetBytes(b)

	sum := big.Int{}
	sum.Add(&aInt, &bInt)
	return sum.Bytes()
}

// between returns if x is between a and b.
// On this crude ascii Chord ring, x is between (a : b)
//        ___
//       /   \-a
//      |     |
//     b-\   /-x
//        ---
func between(x, a, b []byte) bool {
	xInt := (&big.Int{}).SetBytes(x)
	aInt := (&big.Int{}).SetBytes(a)
	bInt := (&big.Int{}).SetBytes(b)

	// Allow for wraparounds by checking that
	//  1) x > a and x < b when a < b or
	//  2) x < a or x < b when a > b or
	//  3) x > a and x > b when a > b or
	//  4) x < a or x > a when a == b
	switch aInt.Cmp(bInt) {
	case -1:
		return (xInt.Cmp(aInt) > 0) && (xInt.Cmp(bInt) < 0)
	case 1:
		return (xInt.Cmp(aInt) > 0) || (xInt.Cmp(bInt) < 0)
	case 0:
		return xInt.Cmp(aInt) != 0
	}

	return false
}

// betweenRightIncl is like Between, but includes the right boundary.
// That is, is x between (a : b]
func betweenRightIncl(x, a, b []byte) bool {
	return between(x, a, b) || bytes.Equal(x, b)
}
