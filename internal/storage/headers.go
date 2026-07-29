package storage

import (
	"encoding/json"
	"fmt"
)

// encodeHeaders serializes the recent-header window for indexer_state.
func encodeHeaders(headers []HeaderRef) (string, error) {
	b, err := json.Marshal(headers)
	if err != nil {
		return "", fmt.Errorf("encoding headers: %w", err)
	}
	return string(b), nil
}

func decodeHeaders(s string) ([]HeaderRef, error) {
	var out []HeaderRef
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, fmt.Errorf("decoding headers: %w", err)
	}
	return out, nil
}
