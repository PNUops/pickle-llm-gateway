package server

import (
	"encoding/json"
	"net/http"
	"strconv"
)

func writeJSON(w http.ResponseWriter, v any) {
	enc := json.NewEncoder(w)
	_ = enc.Encode(v)
}

func itoa64(n int64) string { return strconv.FormatInt(n, 10) }
