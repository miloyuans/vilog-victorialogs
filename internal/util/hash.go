package util

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

func HashJSON(v any) (string, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
