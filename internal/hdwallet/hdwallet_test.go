package hdwallet

import (
	"crypto/sha512"
	"strings"
	"testing"

	"github.com/btcsuite/btcd/btcutil/hdkeychain"
	"github.com/btcsuite/btcd/chaincfg"
	"golang.org/x/crypto/pbkdf2"
)

// testAccountXpub builds the account-level xpub (m/44'/60'/0') of the
// well-known anvil/hardhat test mnemonic, whose derived addresses are
// publicly documented - a real-world vector for the whole pipeline.
func testAccountXpub(t *testing.T) string {
	t.Helper()
	mnemonic := "test test test test test test test test test test test junk"
	seed := pbkdf2.Key([]byte(mnemonic), []byte("mnemonic"), 2048, 64, sha512.New)
	master, err := hdkeychain.NewMaster(seed, &chaincfg.MainNetParams)
	if err != nil {
		t.Fatal(err)
	}
	key := master
	for _, n := range []uint32{
		hdkeychain.HardenedKeyStart + 44,
		hdkeychain.HardenedKeyStart + 60,
		hdkeychain.HardenedKeyStart + 0,
	} {
		if key, err = key.Derive(n); err != nil {
			t.Fatal(err)
		}
	}
	pub, err := key.Neuter()
	if err != nil {
		t.Fatal(err)
	}
	return pub.String()
}

func TestHDWalletKnownVector(t *testing.T) {
	w := Wallet{XPub: testAccountXpub(t), Path: "0/*", Count: 3}
	got, err := w.Derive()
	if err != nil {
		t.Fatal(err)
	}
	// The first three anvil/hardhat accounts (m/44'/60'/0'/0/0..2).
	want := []string{
		"0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
		"0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
		"0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC",
	}
	for i, addr := range got {
		if addr.Hex() != want[i] {
			t.Errorf("index %d = %s, want %s", i, addr.Hex(), want[i])
		}
	}
}

func TestHDWalletRange(t *testing.T) {
	xpub := testAccountXpub(t)
	all, err := Wallet{XPub: xpub, Count: 10}.Derive()
	if err != nil {
		t.Fatal(err)
	}
	window, err := Wallet{XPub: xpub, Start: 5, Count: 2}.Derive()
	if err != nil {
		t.Fatal(err)
	}
	if len(window) != 2 || window[0] != all[5] || window[1] != all[6] {
		t.Errorf("window = %v, want indexes 5-6 of %v", window, all[5:7])
	}
}

func TestHDWalletLargeWindowParallel(t *testing.T) {
	xpub := testAccountXpub(t)
	got, err := Wallet{XPub: xpub, Count: 5000}.Derive()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5000 {
		t.Fatalf("derived %d addresses, want 5000", len(got))
	}
	// Spot-check the parallel result against single-index windows.
	for _, idx := range []uint32{0, 1234, 4999} {
		single, err := Wallet{XPub: xpub, Start: idx, Count: 1}.Derive()
		if err != nil {
			t.Fatal(err)
		}
		if got[idx] != single[0] {
			t.Errorf("index %d: %s != %s", idx, got[idx], single[0])
		}
	}
	// No zero-value holes from worker partitioning.
	var zero [20]byte
	for i, addr := range got {
		if addr == zero {
			t.Fatalf("hole at index %d", i)
		}
	}
}

func TestHDWalletDefaultPath(t *testing.T) {
	xpub := testAccountXpub(t)
	explicit, err := Wallet{XPub: xpub, Path: "0/*", Count: 1}.Derive()
	if err != nil {
		t.Fatal(err)
	}
	defaulted, err := Wallet{XPub: xpub, Count: 1}.Derive()
	if err != nil {
		t.Fatal(err)
	}
	if explicit[0] != defaulted[0] {
		t.Errorf("default path mismatch: %s vs %s", defaulted[0], explicit[0])
	}
}

func TestHDWalletErrors(t *testing.T) {
	xpub := testAccountXpub(t)
	cases := map[string]Wallet{
		"zero count":         {XPub: xpub, Count: 0},
		"garbage xpub":       {XPub: "xpub-not-a-key", Count: 1},
		"no placeholder":     {XPub: xpub, Path: "0/1", Count: 1},
		"placeholder middle": {XPub: xpub, Path: "*/0", Count: 1},
		"hardened segment":   {XPub: xpub, Path: "0'/*", Count: 1},
		"range overflow":     {XPub: xpub, Start: 1<<31 - 1, Count: 2},
	}
	for name, w := range cases {
		if _, err := w.Derive(); err == nil {
			t.Errorf("%s: expected error, got none", name)
		}
	}
}

func TestHDWalletRejectsPrivateKey(t *testing.T) {
	mnemonicSeed := pbkdf2.Key([]byte("test test test test test test test test test test test junk"), []byte("mnemonic"), 2048, 64, sha512.New)
	master, err := hdkeychain.NewMaster(mnemonicSeed, &chaincfg.MainNetParams)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Wallet{XPub: master.String(), Count: 1}.Derive()
	if err == nil || !strings.Contains(err.Error(), "PRIVATE") {
		t.Fatalf("xprv accepted or wrong error: %v", err)
	}
}
