package main

import (
	"archive/zip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yinhm/twitter-import/internal/state"
)

func testArchive(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "archive.zip")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	account, _ := writer.Create("data/account.js")
	_, _ = account.Write([]byte(`window.YTD.account.part0 = [{"account":{"accountId":"12345"}}]`))
	tweets, _ := writer.Create("data/tweets.js")
	_, _ = tweets.Write([]byte(`window.YTD.tweets.part0 = [{"tweet":{"id_str":"42","full_text":"hello","created_at":"Mon Aug 17 12:34:56 +0000 2020"}}]`))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func testArchiveWithReply(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "archive-with-reply.zip")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	account, _ := writer.Create("data/account.js")
	_, _ = account.Write([]byte(`window.YTD.account.part0 = [{"account":{"accountId":"12345"}}]`))
	tweets, _ := writer.Create("data/tweets.js")
	_, _ = tweets.Write([]byte(`window.YTD.tweets.part0 = [
		{"tweet":{"id_str":"42","full_text":"published","created_at":"Mon Aug 17 12:34:56 +0000 2020"}},
		{"tweet":{"id_str":"43","full_text":"reply","created_at":"Mon Aug 17 12:35:56 +0000 2020","in_reply_to_status_id_str":"41"}}
	]`))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func testCollectedBundle(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "collected.zip")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	source, _ := writer.Create("source.json")
	_, _ = source.Write([]byte(`{"version":1,"format":"twitter-import/v1","collector":"twikit","collected_at":"2026-09-01T00:00:00Z","collection":{"kind":"list","id":"99"}}`))
	items, _ := writer.Create("items.jsonl")
	_, _ = items.Write([]byte(`{"id":"42","account_id":"12345","created_at":"2026-09-01T00:00:00Z","text":"hello","url":"https://x.com/alice/status/42","media":[]}` + "\n"))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunImportUsesCollectedItemAuthorAndURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/feed":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{"uuid": "feed"}})
		case "/api/v1/feed/entries":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}, "pagination": map[string]string{"next_cursor": ""}})
		case "/api/v1/feed/imports":
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Fatal(err)
			}
			var metadata struct {
				Source struct {
					AccountID string `json:"account_id"`
					URL       string `json:"url"`
				} `json:"source"`
			}
			if err := json.Unmarshal([]byte(r.FormValue("metadata")), &metadata); err != nil {
				t.Fatal(err)
			}
			if metadata.Source.AccountID != "12345" || metadata.Source.URL != "https://x.com/alice/status/42" {
				t.Fatalf("source=%+v", metadata.Source)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"created":true,"data":{"id":"entry"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	t.Setenv("FF_FEED_API_KEY", "secret")
	dir := t.TempDir()
	args := []string{"--endpoint", server.URL, "--state", filepath.Join(dir, "state.db"), "--report", filepath.Join(dir, "report.jsonl")}
	if err := runImport(testCollectedBundle(t), args); err != nil {
		t.Fatal(err)
	}
}

func TestRunImportCheckpointsSuccessfulItems(t *testing.T) {
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Fatal("missing credential")
		}
		switch r.URL.Path {
		case "/api/v1/feed":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{"uuid": "feed"}})
		case "/api/v1/feed/entries":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}, "pagination": map[string]string{"next_cursor": ""}})
		case "/api/v1/feed/imports":
			posts++
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"created":true,"data":{"id":"entry"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	t.Setenv("FF_FEED_API_KEY", "secret")
	dir := t.TempDir()
	args := []string{"--endpoint", server.URL, "--state", filepath.Join(dir, "state.db"), "--report", filepath.Join(dir, "report.jsonl")}
	archive := testArchive(t)
	if err := runImport(archive, args); err != nil {
		t.Fatal(err)
	}
	if err := runImport(archive, args); err != nil {
		t.Fatal(err)
	}
	if posts != 1 {
		t.Fatalf("posts=%d", posts)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "report.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) == 0 {
		t.Fatal("empty report")
	}
}

func TestRunBatchDefaultsToDryRunAndAppliesExplicitTargets(t *testing.T) {
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/feed/imports" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.Header.Get("X-FF-Import-Target") != "alice" {
			t.Fatalf("target=%q", r.Header.Get("X-FF-Import-Target"))
		}
		posts++
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"created":true,"data":{"id":"entry"}}`))
	}))
	defer server.Close()

	dir := t.TempDir()
	archivePath := testArchive(t)
	keyPath := filepath.Join(dir, "operator-key")
	if err := os.WriteFile(keyPath, []byte("secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(dir, "manifest.json")
	manifest := map[string]any{"version": 1, "imports": []map[string]any{{
		"archive": archivePath, "target_feed": "alice", "state": "alice.db", "report": "alice.jsonl",
	}}}
	raw, _ := json.Marshal(manifest)
	if err := os.WriteFile(manifestPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"--endpoint", server.URL, "--key-file", keyPath}
	if err := runBatch(manifestPath, args); err != nil {
		t.Fatal(err)
	}
	if posts != 0 {
		t.Fatalf("dry-run posted %d entries", posts)
	}
	if err := runBatch(manifestPath, append(args, "--apply")); err != nil {
		t.Fatal(err)
	}
	if posts != 1 {
		t.Fatalf("posts=%d", posts)
	}
}

func TestRunImportSkipsRepliesUnlessForced(t *testing.T) {
	posts := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/feed":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{"uuid": "feed"}})
		case "/api/v1/feed/entries":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}, "pagination": map[string]string{"next_cursor": ""}})
		case "/api/v1/feed/imports":
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Fatal(err)
			}
			var metadata struct {
				Source struct {
					ItemID string `json:"item_id"`
				} `json:"source"`
			}
			if err := json.Unmarshal([]byte(r.FormValue("metadata")), &metadata); err != nil {
				t.Fatal(err)
			}
			posts = append(posts, metadata.Source.ItemID)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"created":true,"data":{"id":"entry"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	t.Setenv("FF_FEED_API_KEY", "secret")
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.db")
	reportPath := filepath.Join(dir, "report.jsonl")
	args := []string{"--endpoint", server.URL, "--state", statePath, "--report", reportPath}
	archive := testArchiveWithReply(t)

	if err := runImport(archive, args); err != nil {
		t.Fatal(err)
	}
	if len(posts) != 1 || posts[0] != "42" {
		t.Fatalf("default posts=%v", posts)
	}
	if err := runImport(archive, append(args, "--include-replies")); err != nil {
		t.Fatal(err)
	}
	if len(posts) != 2 || posts[1] != "43" {
		t.Fatalf("forced posts=%v", posts)
	}
	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"item_id":"43","result":"reply_skipped"`) {
		t.Fatalf("report=%s", raw)
	}
}

func TestRunImportSkipsFriendFeedLegacyStatusesURL(t *testing.T) {
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/feed":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{"uuid": "feed"}})
		case "/api/v1/feed/entries":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data":       []map[string]string{{"id": "legacy", "source_url": "http://twitter.com/epaulin/statuses/42"}},
				"pagination": map[string]string{"next_cursor": ""},
			})
		case "/api/v1/feed/imports":
			posts++
			w.WriteHeader(http.StatusCreated)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	t.Setenv("FF_FEED_API_KEY", "secret")
	dir := t.TempDir()
	args := []string{"--endpoint", server.URL, "--state", filepath.Join(dir, "state.db"), "--report", filepath.Join(dir, "report.jsonl")}

	if err := runImport(testArchive(t), args); err != nil {
		t.Fatal(err)
	}
	if posts != 0 {
		t.Fatalf("legacy tweet was imported again: posts=%d", posts)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "report.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"result":"legacy_skipped"`) {
		t.Fatalf("report=%s", raw)
	}
}

func TestTweetIDFromURLAcceptsHistoricalStatusesPath(t *testing.T) {
	if got := tweetIDFromURL("http://twitter.com/epaulin/statuses/482389322"); got != "482389322" {
		t.Fatalf("tweet ID=%q", got)
	}
}

func TestRunImportDoesNotCheckpointRevokedCapabilityFailure(t *testing.T) {
	posts := 0
	allowImport := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/feed":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{"uuid": "feed"}})
		case "/api/v1/feed/entries":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []any{}, "pagination": map[string]string{"next_cursor": ""}})
		case "/api/v1/feed/imports":
			posts++
			if !allowImport {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"created":true,"data":{"id":"entry"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	t.Setenv("FF_FEED_API_KEY", "secret")
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.db")
	args := []string{"--endpoint", server.URL, "--state", statePath, "--report", filepath.Join(dir, "report.jsonl")}
	archive := testArchive(t)

	if err := runImport(archive, args); err == nil {
		t.Fatal("revoked capability did not stop the import")
	}
	db, err := state.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if db.HasDone("42") {
		t.Fatal("capability failure was checkpointed")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	allowImport = true
	if err := runImport(archive, args); err != nil {
		t.Fatal(err)
	}
	if posts != 2 {
		t.Fatalf("posts=%d, want retry after capability recovery", posts)
	}
}
