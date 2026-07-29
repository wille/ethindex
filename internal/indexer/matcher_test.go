package indexer

import (
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/wille/ethindex/internal/config"
	"github.com/wille/ethindex/internal/event"
)

var (
	testChainID = big.NewInt(1)
	watchedAddr = common.HexToAddress("0x1111111111111111111111111111111111111111")
	otherAddr   = common.HexToAddress("0x2222222222222222222222222222222222222222")
	tokenAddr   = common.HexToAddress("0x3333333333333333333333333333333333333333")
)

func testKey(t *testing.T) (*ecdsa.PrivateKey, common.Address) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return key, crypto.PubkeyToAddress(key.PublicKey)
}

func newMatcher() *Matcher {
	return NewMatcher(
		[]common.Address{watchedAddr},
		[]config.ParsedToken{{Address: tokenAddr, Symbol: "TEST", Decimals: 6}},
		testChainID,
		"",
	)
}

func signedTx(t *testing.T, key *ecdsa.PrivateKey, to common.Address, value *big.Int, data []byte) *types.Transaction {
	t.Helper()
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   testChainID,
		Nonce:     1,
		GasTipCap: big.NewInt(1),
		GasFeeCap: big.NewInt(100),
		Gas:       100000,
		To:        &to,
		Value:     value,
		Data:      data,
	})
	signed, err := types.SignTx(tx, types.LatestSignerForChainID(testChainID), key)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

// transferCalldata builds transfer(address,uint256) calldata.
func transferCalldata(to common.Address, amount *big.Int) []byte {
	data := make([]byte, 4+64)
	copy(data[:4], methodTransfer[:])
	copy(data[4+12:4+32], to.Bytes())
	amount.FillBytes(data[4+32 : 4+64])
	return data
}

// transferFromCalldata builds transferFrom(address,address,uint256) calldata.
func transferFromCalldata(from, to common.Address, amount *big.Int) []byte {
	data := make([]byte, 4+96)
	copy(data[:4], methodTransferFrom[:])
	copy(data[4+12:4+32], from.Bytes())
	copy(data[4+44:4+64], to.Bytes())
	amount.FillBytes(data[4+64 : 4+96])
	return data
}

func TestMatchNativeTransfer(t *testing.T) {
	key, sender := testKey(t)
	m := newMatcher()

	tx := signedTx(t, key, watchedAddr, big.NewInt(1e18), nil)
	matches := m.MatchTx(tx)
	if len(matches) != 1 {
		t.Fatalf("got %d matches, want 1", len(matches))
	}
	got := matches[0]
	if got.Asset != "ETH" || got.To != watchedAddr || got.From != sender {
		t.Errorf("unexpected match: %+v", got)
	}
	if got.Value.Cmp(big.NewInt(1e18)) != 0 || got.Decimals != 18 {
		t.Errorf("value = %s decimals = %d", got.Value, got.Decimals)
	}
}

func TestNativeNonMatches(t *testing.T) {
	key, _ := testKey(t)
	m := newMatcher()

	if got := m.MatchTx(signedTx(t, key, otherAddr, big.NewInt(1), nil)); got != nil {
		t.Errorf("unwatched recipient matched: %+v", got)
	}
	if got := m.MatchTx(signedTx(t, key, watchedAddr, big.NewInt(0), nil)); got != nil {
		t.Errorf("zero-value tx matched: %+v", got)
	}
}

func TestMatchERC20TransferCalldata(t *testing.T) {
	key, sender := testKey(t)
	m := newMatcher()

	amount := big.NewInt(2_500_000)
	tx := signedTx(t, key, tokenAddr, big.NewInt(0), transferCalldata(watchedAddr, amount))
	matches := m.MatchTx(tx)
	if len(matches) != 1 {
		t.Fatalf("got %d matches, want 1", len(matches))
	}
	got := matches[0]
	if got.Asset != "TEST" || got.To != watchedAddr || got.From != sender {
		t.Errorf("unexpected match: %+v", got)
	}
	if got.Token == nil || *got.Token != tokenAddr {
		t.Errorf("token = %v", got.Token)
	}
	if got.Value.Cmp(amount) != 0 || got.Decimals != 6 {
		t.Errorf("value = %s decimals = %d", got.Value, got.Decimals)
	}
}

func TestMatchERC20TransferFromCalldata(t *testing.T) {
	key, _ := testKey(t)
	m := newMatcher()

	tx := signedTx(t, key, tokenAddr, big.NewInt(0), transferFromCalldata(otherAddr, watchedAddr, big.NewInt(77)))
	matches := m.MatchTx(tx)
	if len(matches) != 1 {
		t.Fatalf("got %d matches, want 1", len(matches))
	}
	if matches[0].To != watchedAddr || matches[0].Value.Cmp(big.NewInt(77)) != 0 {
		t.Errorf("unexpected match: %+v", matches[0])
	}
}

func TestERC20NonMatches(t *testing.T) {
	key, _ := testKey(t)
	m := newMatcher()

	// Transfer to an unwatched recipient.
	if got := m.MatchTx(signedTx(t, key, tokenAddr, big.NewInt(0), transferCalldata(otherAddr, big.NewInt(1)))); got != nil {
		t.Errorf("unwatched recipient matched: %+v", got)
	}
	// Watched recipient but unwatched token contract.
	if got := m.MatchTx(signedTx(t, key, otherAddr, big.NewInt(0), transferCalldata(watchedAddr, big.NewInt(1)))); got != nil {
		t.Errorf("unwatched token matched: %+v", got)
	}
	// Malformed calldata: truncated args.
	if got := m.MatchTx(signedTx(t, key, tokenAddr, big.NewInt(0), transferCalldata(watchedAddr, big.NewInt(1))[:40])); got != nil {
		t.Errorf("truncated calldata matched: %+v", got)
	}
	// Unknown method ID.
	bad := transferCalldata(watchedAddr, big.NewInt(1))
	bad[0] = 0xff
	if got := m.MatchTx(signedTx(t, key, tokenAddr, big.NewInt(0), bad)); got != nil {
		t.Errorf("unknown method matched: %+v", got)
	}
}

func TestMatchDirections(t *testing.T) {
	key, sender := testKey(t)
	// Watch the tx signer so its sends match as outgoing.
	m := NewMatcher(
		[]common.Address{sender, watchedAddr},
		[]config.ParsedToken{{Address: tokenAddr, Symbol: "TEST", Decimals: 6}},
		testChainID,
		"",
	)

	// Native out: watched signer pays an unwatched address.
	matches := m.MatchTx(signedTx(t, key, otherAddr, big.NewInt(1e18), nil))
	if len(matches) != 1 || matches[0].Direction != event.DirectionOut {
		t.Fatalf("native out matches = %+v", matches)
	}
	if matches[0].From != sender || matches[0].To != otherAddr {
		t.Errorf("native out match = %+v", matches[0])
	}

	// Native self: watched signer pays another watched address.
	matches = m.MatchTx(signedTx(t, key, watchedAddr, big.NewInt(1), nil))
	if len(matches) != 1 || matches[0].Direction != event.DirectionSelf {
		t.Fatalf("native self matches = %+v", matches)
	}

	// ERC20 calldata out: watched signer transfers tokens away.
	matches = m.MatchTx(signedTx(t, key, tokenAddr, big.NewInt(0), transferCalldata(otherAddr, big.NewInt(5))))
	if len(matches) != 1 || matches[0].Direction != event.DirectionOut || matches[0].Asset != "TEST" {
		t.Fatalf("erc20 out matches = %+v", matches)
	}

	// transferFrom moving a watched owner's tokens, signed by an
	// unwatched spender: the owner is the token sender, so this is out.
	key2, _ := testKey(t)
	matches = m.MatchTx(signedTx(t, key2, tokenAddr, big.NewInt(0), transferFromCalldata(watchedAddr, otherAddr, big.NewInt(9))))
	if len(matches) != 1 || matches[0].Direction != event.DirectionOut {
		t.Fatalf("transferFrom out matches = %+v", matches)
	}
	if matches[0].From != watchedAddr {
		t.Errorf("transferFrom out from = %s, want owner %s", matches[0].From, watchedAddr)
	}

	// Native leg + token calldata leg in one tx: watched signer sends
	// value to the token contract while transferring tokens to a
	// watched address.
	matches = m.MatchTx(signedTx(t, key, tokenAddr, big.NewInt(3), transferCalldata(watchedAddr, big.NewInt(5))))
	if len(matches) != 2 {
		t.Fatalf("dual-leg matches = %+v", matches)
	}
	if matches[0].Asset != "ETH" || matches[0].Direction != event.DirectionOut {
		t.Errorf("native leg = %+v", matches[0])
	}
	if matches[1].Asset != "TEST" || matches[1].Direction != event.DirectionSelf {
		t.Errorf("token leg = %+v", matches[1])
	}

	// Log out and self.
	lg, ok := m.MatchLog(transferLog(tokenAddr, watchedAddr, otherAddr, big.NewInt(1), 1))
	if !ok || lg.Direction != event.DirectionOut {
		t.Fatalf("log out = %+v ok=%v", lg, ok)
	}
	lg, ok = m.MatchLog(transferLog(tokenAddr, sender, watchedAddr, big.NewInt(1), 2))
	if !ok || lg.Direction != event.DirectionSelf {
		t.Fatalf("log self = %+v ok=%v", lg, ok)
	}
}

func TestNativeSymbol(t *testing.T) {
	key, sender := testKey(t)
	m := NewMatcher([]common.Address{watchedAddr}, nil, testChainID, "BNB")

	matches := m.MatchTx(signedTx(t, key, watchedAddr, big.NewInt(1e18), nil))
	if len(matches) != 1 || matches[0].Asset != "BNB" {
		t.Fatalf("matches = %+v", matches)
	}
	if matches[0].From != sender || matches[0].Decimals != 18 {
		t.Errorf("match = %+v", matches[0])
	}
}

func TestContractCreationIgnored(t *testing.T) {
	key, _ := testKey(t)
	m := newMatcher()
	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID: testChainID, Nonce: 1, GasTipCap: big.NewInt(1), GasFeeCap: big.NewInt(100),
		Gas: 100000, To: nil, Value: big.NewInt(1), Data: []byte{0x60},
	})
	signed, err := types.SignTx(tx, types.LatestSignerForChainID(testChainID), key)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.MatchTx(signed); got != nil {
		t.Errorf("contract creation matched: %+v", got)
	}
}

func transferLog(token, from, to common.Address, amount *big.Int, index uint) types.Log {
	data := make([]byte, 32)
	amount.FillBytes(data)
	return types.Log{
		Address: token,
		Topics: []common.Hash{
			transferTopic,
			common.BytesToHash(from.Bytes()),
			common.BytesToHash(to.Bytes()),
		},
		Data:   data,
		TxHash: common.HexToHash("0xdead"),
		Index:  index,
	}
}

func TestMatchLog(t *testing.T) {
	m := newMatcher()

	match, ok := m.MatchLog(transferLog(tokenAddr, otherAddr, watchedAddr, big.NewInt(5_000_000), 7))
	if !ok {
		t.Fatal("expected match")
	}
	if match.Asset != "TEST" || match.From != otherAddr || match.To != watchedAddr {
		t.Errorf("unexpected match: %+v", match)
	}
	if match.Direction != event.DirectionIn {
		t.Errorf("direction = %s, want in", match.Direction)
	}
	if match.LogIndex == nil || *match.LogIndex != 7 {
		t.Errorf("log index = %v", match.LogIndex)
	}
	if match.Value.Cmp(big.NewInt(5_000_000)) != 0 {
		t.Errorf("value = %s", match.Value)
	}
}

// Zero-value transfers are address-poisoning spam: warned about, never
// matched.
func TestZeroValueTransfersIgnored(t *testing.T) {
	m := newMatcher()
	key, sender := testKey(t)
	m.watched[sender] = struct{}{}

	// ERC20 Transfer log with amount 0 (e.g. USDT transferFrom poison).
	if _, ok := m.MatchLog(transferLog(tokenAddr, watchedAddr, otherAddr, big.NewInt(0), 3)); ok {
		t.Error("zero value transfer log matched")
	}
	// transfer(to, 0) calldata on a watched token.
	if got := m.MatchTx(signedTx(t, key, tokenAddr, big.NewInt(0), transferCalldata(watchedAddr, big.NewInt(0)))); len(got) != 0 {
		t.Errorf("zero value calldata transfer matched: %+v", got)
	}
	// Plain native send of 0 from a watched address.
	if got := m.MatchTx(signedTx(t, key, otherAddr, big.NewInt(0), nil)); len(got) != 0 {
		t.Errorf("zero value native send matched: %+v", got)
	}
}

func TestMatchLogNonMatches(t *testing.T) {
	m := newMatcher()

	// Neither side watched.
	if _, ok := m.MatchLog(transferLog(tokenAddr, otherAddr, otherAddr, big.NewInt(1), 0)); ok {
		t.Error("unwatched transfer matched")
	}
	// Unwatched token contract.
	if _, ok := m.MatchLog(transferLog(otherAddr, otherAddr, watchedAddr, big.NewInt(1), 0)); ok {
		t.Error("unwatched token matched")
	}
	// Wrong topic count (e.g. ERC721 Transfer has 4 topics).
	lg := transferLog(tokenAddr, otherAddr, watchedAddr, big.NewInt(1), 0)
	lg.Topics = append(lg.Topics, common.Hash{})
	if _, ok := m.MatchLog(lg); ok {
		t.Error("4-topic log matched")
	}
	// Wrong event signature.
	lg = transferLog(tokenAddr, otherAddr, watchedAddr, big.NewInt(1), 0)
	lg.Topics[0] = common.HexToHash("0x01")
	if _, ok := m.MatchLog(lg); ok {
		t.Error("wrong signature matched")
	}
}
