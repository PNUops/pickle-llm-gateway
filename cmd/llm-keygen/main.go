// Command llm-keygen maintains the gateway's authorization document from the
// command line. It issues a key — generating the plaintext, printing it
// exactly once, and storing only the hash — and it also performs the two
// operations an incident needs: revoking a key and taking the whole service
// out of use. All three go through the same writer, which locks the document,
// replaces it atomically and keeps its owner, so the running gateway never
// reads a half-written file and never loses its ability to read it at all.
//
// Those last two exist because the alternative was a hand-edited JSON file.
// An emergency is the worst moment to be editing the document that decides who
// may call the service, with a text editor, as root, without the lock.
package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
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
	revoke := flag.String("revoke", "", "revoke this keyId instead of issuing a key (requires -snapshot)")
	serviceEnabled := flag.String("service", "", "set the kill switch: on|off (requires -snapshot)")
	flag.Parse()

	// The maintenance modes take the document somewhere else entirely; doing
	// one of them and issuing a key in the same run would be a surprise.
	if *revoke != "" || *serviceEnabled != "" {
		if *snapPath == "" {
			fatal(errors.New("-revoke and -service need -snapshot: there is nothing to change without a document"))
		}
		if *revoke != "" && *serviceEnabled != "" {
			fatal(errors.New("-revoke and -service are separate operations; run them one at a time"))
		}
		if *revoke != "" {
			if err := revokeKey(*snapPath, *revoke); err != nil {
				fatal(err)
			}
			fmt.Printf("%s를 폐기했습니다. 게이트웨이는 다음 폴링에서 반영합니다: %s\n", *revoke, *snapPath)
			return
		}
		on, err := parseSwitch(*serviceEnabled)
		if err != nil {
			fatal(err)
		}
		if err := setService(*snapPath, on); err != nil {
			fatal(err)
		}
		state := "점검 모드로 전환했습니다(모든 요청 거부)"
		if on {
			state = "정상 운영으로 되돌렸습니다"
		}
		fmt.Printf("%s 게이트웨이는 다음 폴링에서 반영합니다: %s\n", state, *snapPath)
		return
	}

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

func parseSwitch(v string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "on", "true", "yes":
		return true, nil
	case "off", "false", "no":
		return false, nil
	}
	return false, fmt.Errorf("-service takes on or off, not %q", v)
}

// revokeKey marks one key REVOKED. The entry stays in the document rather than
// disappearing from it, so the gateway can answer "this key was revoked"
// instead of "no such key" — the difference between a student who knows what
// happened and one who files a ticket.
func revokeKey(path, keyID string) error {
	return mutate(path, func(doc *snapshot.Document) error {
		for i := range doc.Keys {
			if doc.Keys[i].KeyID != keyID {
				continue
			}
			if doc.Keys[i].Status == snapshot.KeyRevoked {
				return fmt.Errorf("keyId %s is already revoked", keyID)
			}
			doc.Keys[i].Status = snapshot.KeyRevoked
			return nil
		}
		return fmt.Errorf("keyId %s is not in %s", keyID, path)
	})
}

// setService flips the kill switch. Off refuses every request with the
// maintenance error while leaving the keys and models untouched.
func setService(path string, on bool) error {
	return mutate(path, func(doc *snapshot.Document) error {
		if doc.ServiceEnabled == on {
			return fmt.Errorf("serviceEnabled is already %t", on)
		}
		doc.ServiceEnabled = on
		return nil
	})
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
	return mutate(path, func(doc *snapshot.Document) error {
		for _, k := range doc.Keys {
			if k.KeyID == entry.KeyID {
				return fmt.Errorf("keyId %s already exists in %s", entry.KeyID, path)
			}
		}
		doc.Keys = append(doc.Keys, entry)
		return nil
	})
}

// mutate applies change to the document under an exclusive lock and writes the
// result back atomically, bumping the generation. Every command-line change to
// the document goes through here: the locking, the generation bump and the
// ownership-preserving replace are properties of the document, not of any one
// operation, and the one time they were open-coded elsewhere the lock was the
// part that got left out.
func mutate(path string, change func(*snapshot.Document) error) error {
	// Serialize the read-modify-write against a concurrent keygen or a hand
	// edit: without the lock, two writers each read the pre-edit document and
	// the second rename silently discards the first's change — a lost key, or a
	// revocation undone. flock on a sidecar (not the document itself, which is
	// replaced by rename) is advisory but every writer here takes it.
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("acquiring the snapshot lock: %w", err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("locking the snapshot: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)

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
	if err := change(&doc); err != nil {
		return err
	}
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
