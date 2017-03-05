package gmaj

import (
	"math/big"
	"testing"
)

func TestBetween(t *testing.T) {
	t.Parallel()

	tests := []struct {
		x   int64
		a   int64
		b   int64
		exp bool
	}{
		{x: 20, a: 15, b: 21, exp: true},
		{x: 47, a: 93, b: 93, exp: true},
		{x: 532, a: 527, b: 789, exp: true},
		{x: 169224980, a: 100797713, b: 220867348, exp: true},
		{x: 22086, a: 1007, b: 22086, exp: false},

		// wrapping around

		{x: 20, a: 527, b: 277, exp: true},
		{x: 788, a: 527, b: 277, exp: true},
		{x: 20, a: 5, b: 2, exp: true},
		{x: 1, a: 5, b: 2, exp: true},
		{x: 3, a: 5, b: 2, exp: false},
		{x: 20, a: 2, b: 5, exp: false},
	}

	for i, test := range tests {
		x := big.NewInt(test.x).Bytes()
		a := big.NewInt(test.a).Bytes()
		b := big.NewInt(test.b).Bytes()
		if want, got := test.exp, between(x, a, b); got != want {
			t.Logf("running test [%02d]", i)
			t.Fatalf("expected %t for Between(%d, %d, %d), got %t",
				want, test.x, test.a, test.b, got,
			)
		}
	}
}

func TestBetweenRightIncl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		x   int64
		a   int64
		b   int64
		exp bool
	}{
		{x: 788, a: 527, b: 277, exp: true},
		{x: 12347, a: 234, b: 93484, exp: true},
		{x: 384732, a: 527, b: 384732, exp: true},
		{x: 384733, a: 527, b: 384732, exp: false},
		{x: 527, a: 527, b: 384732, exp: false},
		{x: 128, a: 64, b: 128, exp: true},

		// wrapping around

		{x: 20, a: 5, b: 2, exp: true},
		{x: 1, a: 5, b: 2, exp: true},
		{x: 2, a: 5, b: 2, exp: true},
		{x: 3, a: 5, b: 2, exp: false},
		{x: 20, a: 2, b: 5, exp: false},
	}

	for i, test := range tests {
		x := big.NewInt(test.x).Bytes()
		a := big.NewInt(test.a).Bytes()
		b := big.NewInt(test.b).Bytes()
		if want, got := test.exp, betweenRightIncl(x, a, b); got != want {
			t.Logf("running test [%02d]", i)
			t.Fatalf("expected %t for BetweenRightIncl(%d, %d, %d), got %t",
				want, test.x, test.a, test.b, got,
			)
		}
	}
}
