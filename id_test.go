package gmaj

import (
	"testing"
)

func TestBetween(t *testing.T) {
	assertBetweenFunc(t, Between, 20, 15, 21)
	assertBetweenFunc(t, Between, 47, 93, 93)

	x := []byte{20}
	a := []byte{2, 15}
	b := []byte{1, 21}
	assertBetweenFuncSlices(t, Between, x, a, b)

	x = []byte{3, 20}
	a = []byte{2, 15}
	b = []byte{1, 21}
	assertBetweenFuncSlices(t, Between, x, a, b)

	x = []byte{10, 22, 43, 20}
	a = []byte{06, 02, 13, 17}
	b = []byte{13, 42, 43, 20}
	assertBetweenFuncSlices(t, Between, x, a, b)

	x = []byte{13, 42, 43, 20}
	a = []byte{06, 02, 13, 17}
	b = []byte{13, 42, 43, 20}
	assertNotBetweenFuncSlices(t, Between, x, a, b)
}

func TestBetweenWrapAround(t *testing.T) {
	assertBetweenFunc(t, Between, 20, 5, 2)
	assertBetweenFunc(t, Between, 1, 5, 2)

	assertNotBetweenFunc(t, Between, 3, 5, 2)
	assertNotBetweenFunc(t, Between, 20, 2, 5)
}

func TestBetweenRightIncl(t *testing.T) {
	x := []byte{3, 20}
	a := []byte{2, 15}
	b := []byte{1, 21}
	assertBetweenFuncSlices(t, BetweenRightIncl, x, a, b)

	x = []byte{10, 22, 43, 20}
	a = []byte{06, 02, 13, 17}
	b = []byte{13, 42, 43, 20}
	assertBetweenFuncSlices(t, BetweenRightIncl, x, a, b)

	x = []byte{13, 42, 43, 20}
	a = []byte{06, 02, 13, 17}
	b = []byte{13, 42, 43, 20}
	assertBetweenFuncSlices(t, BetweenRightIncl, x, a, b)
}

func TestBetweenRightInclWrapAround(t *testing.T) {
	assertBetweenFunc(t, BetweenRightIncl, 20, 5, 2)
	assertBetweenFunc(t, BetweenRightIncl, 1, 5, 2)
	assertBetweenFunc(t, BetweenRightIncl, 2, 5, 2)

	assertNotBetweenFunc(t, BetweenRightIncl, 3, 5, 2)
	assertNotBetweenFunc(t, BetweenRightIncl, 20, 2, 5)
}
