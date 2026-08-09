// Command llm-keygen issues one gateway API key: it generates the plaintext,
// prints it exactly once, and emits the snapshot entry carrying only the
// hash. With -snapshot it inserts the entry into the document directly and
// bumps the generation, so the running gateway picks it up on its next poll.
package main

import (
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

const (
	tokenPrefix = "pickle-"
	tokenLen    = 43 // base62 chars after the prefix, ~256 bits
	alphabet    = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

func main() {
	id := flag.String("id", "", "key identifier stored in the snapshot (default: derived from the token)")
	expiresDays := flag.Int("expires-days", 0, "days until the key expires (0 = no expiry)")
	rpm := flag.Int("rpm", 0, "requests-per-minute limit (0 = gateway default)")
	tpm := flag.Int("tpm", 0, "tokens-per-minute limit (0 = gateway default)")
	conc := flag.Int("concurrency", 0, "concurrent-request limit (0 = gateway default)")
	models := flag.String("models", "", "comma-separated public model names the key may use (empty = all)")
	snapPath := flag.String("snapshot", "", "snapshot document to insert the key into (omit to print the entry only)")
	flag.Parse()

	token, err := newToken()
	if err != nil {
		fatal(err)
	}
	hash := snapshot.HashToken(token)
	keyID := *id
	if keyID == "" {
		keyID = "key-" + hash[:12]
	}
	entry := snapshot.Key{
		KeyID:     keyID,
		TokenHash: hash,
		Status:    snapshot.KeyActive,
		Limits:    snapshot.Limits{Rpm: *rpm, Tpm: *tpm, Concurrency: *conc},
	}
	if *expiresDays > 0 {
		t := time.Now().AddDate(0, 0, *expiresDays).UTC().Truncate(time.Second)
		entry.ExpiresAt = &t
	}
	if *models != "" {
		for _, m := range strings.Split(*models, ",") {
			if m = strings.TrimSpace(m); m != "" {
				entry.AllowedModels = append(entry.AllowedModels, m)
			}
		}
	}

	if *snapPath != "" {
		if err := insert(*snapPath, entry); err != nil {
			fatal(err)
		}
	}

	entryJSON, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		fatal(err)
	}
	fmt.Printf("API Key (지금 한 번만 표시됩니다. 분실하면 재발급하세요):\n%s\n\n", token)
	fmt.Printf("keyId: %s\n", keyID)
	if *snapPath != "" {
		fmt.Printf("스냅샷에 추가되었습니다: %s\n", *snapPath)
	} else {
		fmt.Printf("스냅샷 keys 배열에 추가할 항목:\n%s\n", entryJSON)
	}
}

func newToken() (string, error) {
	var b strings.Builder
	b.WriteString(tokenPrefix)
	max := big.NewInt(int64(len(alphabet)))
	for range tokenLen {
		n, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", err
		}
		b.WriteByte(alphabet[n.Int64()])
	}
	return b.String(), nil
}

// insert adds the entry to the document, bumps the generation, and replaces
// the file atomically so the polling gateway never reads a half-written
// document. The replacement keeps the original file's mode and owner: the
// tool is typically run as root while the gateway reads as its service user,
// and a root-owned replacement would silently freeze the gateway on its last
// good state.
func insert(path string, entry snapshot.Key) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc snapshot.Document
	if err := json.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	for _, k := range doc.Keys {
		if k.KeyID == entry.KeyID {
			return fmt.Errorf("keyId %s already exists in %s", entry.KeyID, path)
		}
	}
	doc.Keys = append(doc.Keys, entry)
	doc.Generation++
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".snapshot-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(append(out, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(fi.Mode().Perm()); err != nil {
		tmp.Close()
		return err
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		if err := os.Chown(tmp.Name(), int(st.Uid), int(st.Gid)); err != nil {
			tmp.Close()
			return fmt.Errorf("keeping the snapshot owner: %w", err)
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "llm-keygen:", err)
	os.Exit(1)
}
