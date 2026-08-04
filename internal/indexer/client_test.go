package indexer

import (
	"encoding/json"
	"testing"
)

// A fullTx=false eth_getBlockByNumber response: transactions are hash
// STRINGS. The reorg ancestor walk fetches headers in this mode, so
// its decode type must not expect transaction objects - decoding this
// payload into Block used to fail with "cannot unmarshal string into
// Go struct field Block.transactions" on the first live reorg.
const headerModeBlockJSON = `{
	"hash": "0x02d2a68ba2e1ae58b48c859c530e079f06a45fe9b18b357db3200baccf50ba7f",
	"parentHash": "0x8400000000000000000000000000000000000000000000000000000000000000",
	"number": "0x5dc",
	"timestamp": "0x688f4b60",
	"transactions": [
		"0x2fb5fda7c0b6d336d156e5cbaf82a3b40a3f0293a2745cbaf82a3b40a3f0293a"
	]
}`

func TestBlockHeaderDecodesHashModeTransactions(t *testing.T) {
	var h BlockHeader
	if err := json.Unmarshal([]byte(headerModeBlockJSON), &h); err != nil {
		t.Fatalf("decoding fullTx=false block into BlockHeader: %v", err)
	}
	if uint64(h.Number) != 1500 || h.Hash.Hex() != "0x02d2a68ba2e1ae58b48c859c530e079f06a45fe9b18b357db3200baccf50ba7f" {
		t.Errorf("decoded header = %+v", h)
	}

	// Block (fullTx=true shape) must NOT be used for this payload -
	// this is the regression: it rejects transaction hash strings.
	var b Block
	if err := json.Unmarshal([]byte(headerModeBlockJSON), &b); err == nil {
		t.Error("Block unexpectedly decodes hash-mode transactions; if this now works, HeaderRefsByNumbers may be simplified")
	}
}
