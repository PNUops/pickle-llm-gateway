package server

import (
	"encoding/json"
	"net/http"
)

func writeJSON(w http.ResponseWriter, v any) {
	enc := json.NewEncoder(w)
	_ = enc.Encode(v)
}
