package gmaj

import (
	"log"
	"time"
)

const (
	// KeyLength is the number of bits (i.e. M value),
	// assumes <= 128 an divisible by 8
	KeyLength = 8

	// IDLen is the length of IDs
	IDLen = KeyLength / 8

	FixNextFingerInterval = time.Millisecond * 90

	StabilizeInterval = time.Millisecond * 100
)

func init() {
	if KeyLength > 128 || KeyLength%8 != 0 {
		log.Fatalf(
			"keyLength of %v is not supported! Must be <= 128 and divisible by 8",
			KeyLength,
		)
	}
}
