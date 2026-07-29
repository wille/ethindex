package event

import (
	"math/big"
	"testing"
)

func TestFormatUnits(t *testing.T) {
	cases := []struct {
		value    string
		decimals uint8
		want     string
	}{
		{"0", 6, "0"},
		{"1", 6, "0.000001"},
		{"1500000", 6, "1.5"},
		{"1000000", 6, "1"},
		{"123456789", 6, "123.456789"},
		{"1000000000000000000", 18, "1"},
		{"1", 18, "0.000000000000000001"},
		{"1230000000000000000", 18, "1.23"},
		{"42", 0, "42"},
		{"-1500000", 6, "-1.5"},
		{"-1", 6, "-0.000001"},
	}
	for _, c := range cases {
		v, ok := new(big.Int).SetString(c.value, 10)
		if !ok {
			t.Fatalf("bad fixture %q", c.value)
		}
		if got := FormatUnits(v, c.decimals); got != c.want {
			t.Errorf("FormatUnits(%s, %d) = %q, want %q", c.value, c.decimals, got, c.want)
		}
	}
}
