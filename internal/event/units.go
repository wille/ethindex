package event

import (
	"math/big"
	"strings"
)

// FormatUnits renders a base-unit integer amount as a decimal string,
// e.g. FormatUnits(1500000, 6) == "1.5". It never uses floating point.
func FormatUnits(v *big.Int, decimals uint8) string {
	if decimals == 0 {
		return v.String()
	}
	div := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	whole, frac := new(big.Int).QuoRem(v, div, new(big.Int))

	neg := frac.Sign() < 0
	frac.Abs(frac)
	if frac.Sign() == 0 {
		return whole.String()
	}

	fracStr := frac.String()
	if pad := int(decimals) - len(fracStr); pad > 0 {
		fracStr = strings.Repeat("0", pad) + fracStr
	}
	fracStr = strings.TrimRight(fracStr, "0")

	wholeStr := whole.String()
	if neg && whole.Sign() == 0 {
		wholeStr = "-" + wholeStr
	}
	return wholeStr + "." + fracStr
}
