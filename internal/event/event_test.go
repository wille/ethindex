package event

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestEmitterNDJSON(t *testing.T) {
	var buf bytes.Buffer
	e := NewEmitter(&buf)
	e.now = func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

	idx := uint(3)
	events := []Event{
		{Type: TypePending, TxHash: "0xaa", From: "0x01", To: "0x02", Asset: "ETH", ValueRaw: "1000", Value: "0.000000000000001"},
		{Type: TypeMined, TxHash: "0xbb", LogIndex: &idx, Asset: "USDC", TokenAddress: "0x03", BlockNumber: 100, BlockHash: "0xcc", Confirmations: 1},
	}
	for _, ev := range events {
		if err := e.Emit(ev); err != nil {
			t.Fatal(err)
		}
	}

	lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}

	var first map[string]any
	if err := json.Unmarshal(lines[0], &first); err != nil {
		t.Fatalf("line 1 is not valid JSON: %v", err)
	}
	if first["timestamp"] != "2026-01-02T03:04:05.000000000Z" {
		t.Errorf("timestamp = %v", first["timestamp"])
	}
	if _, present := first["log_index"]; present {
		t.Error("log_index should be omitted when nil")
	}
	if _, present := first["block_number"]; present {
		t.Error("block_number should be omitted when zero")
	}

	var second map[string]any
	if err := json.Unmarshal(lines[1], &second); err != nil {
		t.Fatalf("line 2 is not valid JSON: %v", err)
	}
	if second["log_index"] != float64(3) {
		t.Errorf("log_index = %v, want 3", second["log_index"])
	}
}
