// Package hdwallet derives watched addresses from BIP32 extended
// public keys.
package hdwallet

import (
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/btcsuite/btcd/btcutil/hdkeychain"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/sync/errgroup"
)

// Wallet derives watched addresses from a BIP32 extended public key.
// Only non-hardened derivation is possible from an xpub, so the path
// must not contain hardened markers - export the xpub at the account
// level (e.g. m/44'/60'/0') and derive the rest here.
type Wallet struct {
	XPub string `yaml:"xpub"`
	// Path is the derivation path relative to the xpub, with "*" as
	// the index placeholder in the last segment. Default "0/*" (the
	// external/receive chain of a standard account-level xpub).
	Path  string `yaml:"path"`
	Start uint32 `yaml:"start"` // first index to derive (default 0)
	Count uint32 `yaml:"count"` // number of addresses, required
}

// nonHardenedMax is the BIP32 boundary: indexes at or above 2^31 are
// hardened and cannot be derived from a public key.
const nonHardenedMax = uint64(1) << 31

// Derive expands the wallet into concrete addresses for indexes
// [Start, Start+Count).
func (w Wallet) Derive() ([]common.Address, error) {
	key, err := hdkeychain.NewKeyFromString(w.XPub)
	if err != nil {
		return nil, fmt.Errorf("parsing xpub: %w", err)
	}
	if key.IsPrivate() {
		return nil, errors.New("extended key is PRIVATE (xprv) - never put private keys in the config; export the public xpub instead")
	}
	if w.Count == 0 {
		return nil, errors.New("count is required")
	}
	if uint64(w.Start)+uint64(w.Count) > nonHardenedMax {
		return nil, fmt.Errorf("index range %d+%d exceeds the non-hardened maximum (2^31)", w.Start, w.Count)
	}

	path := w.Path
	if path == "" {
		path = "0/*"
	}
	segments := strings.Split(path, "/")
	if segments[len(segments)-1] != "*" {
		return nil, fmt.Errorf("path %q must end with the index placeholder \"*\"", path)
	}

	// Derive the fixed prefix once.
	prefix := key
	for _, seg := range segments[:len(segments)-1] {
		if strings.ContainsAny(seg, "'h") {
			return nil, fmt.Errorf("path %q contains a hardened segment %q - hardened children cannot be derived from an xpub", path, seg)
		}
		n, err := strconv.ParseUint(seg, 10, 32)
		if err != nil || n >= nonHardenedMax {
			return nil, fmt.Errorf("path %q: invalid segment %q", path, seg)
		}
		if prefix, err = prefix.Derive(uint32(n)); err != nil {
			return nil, fmt.Errorf("deriving path segment %q: %w", seg, err)
		}
	}

	// Force any lazy initialization before the concurrent section;
	// derivation from a public parent is read-only afterwards.
	if _, err := prefix.ECPubKey(); err != nil {
		return nil, err
	}

	// EC point derivation costs tens of microseconds per address -
	// serial derivation of very large windows (HD wallets with a
	// million addresses) would stall startup, so split across cores.
	out := make([]common.Address, w.Count)
	var g errgroup.Group
	workers := runtime.NumCPU()
	per := (int(w.Count) + workers - 1) / workers
	for lo := 0; lo < int(w.Count); lo += per {
		hi := min(lo+per, int(w.Count))
		g.Go(func() error {
			for off := lo; off < hi; off++ {
				child, err := prefix.Derive(w.Start + uint32(off))
				if err != nil {
					return fmt.Errorf("deriving index %d: %w", w.Start+uint32(off), err)
				}
				pub, err := child.ECPubKey()
				if err != nil {
					return fmt.Errorf("index %d: %w", w.Start+uint32(off), err)
				}
				out[off] = crypto.PubkeyToAddress(*pub.ToECDSA())
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}
