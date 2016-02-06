package gmaj

import (
	"log"
	"time"
)

// configuration constants
const (
	KeyLength = 8 // the number of bits (i.e. M value), assumes <= 128 an divisible by 8

	IDLen = KeyLength / 8

	FixNextFingerInterval = time.Millisecond * 90

	StabilizeInterval = time.Millisecond * 100

	ConnTimeout = time.Second

	RetryWait = time.Second
)

func init() {
	if KeyLength > 128 || KeyLength%8 != 0 {
		log.Fatalf(
			"keyLength of %v is not supported! Must be <= 128 and divisible by 8",
			KeyLength,
		)
	}
}
