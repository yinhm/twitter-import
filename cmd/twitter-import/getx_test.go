package main

import (
	"archive/zip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/yinhm/twitter-import/internal/getxapi"
)

func TestCollectHonorsDefaultShapeAndWritesManifest(t *testing.T) {
	getX := fakeGetX(t, []string{"103", "102"})
	defer getX.Close()
	directory := t.TempDir()
	accounts := filepath.Join(directory, "accounts.tsv")
	if err := os.WriteFile(accounts, []byte("feed_id\tfeed_uuid\ttwitter_username\ttwitter_user_id\n"+
		"alice\t9e43d39c-2358-40a4-80ab-08a79a7b21e2\talice\t42\n"), 0600); err != nil {
		t.Fatal(err)
	}
	key := filepath.Join(directory, "getx-key")
	if err := os.WriteFile(key, []byte("getx\n"), 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(directory, "output")
	if err := runCollect([]string{"--accounts-file", accounts, "--output", output, "--getxapi-key-file", key, "--getxapi-endpoint", getX.URL, "--limit", "1", "--no-media"}); err != nil {
		t.Fatal(err)
	}
	archivePath := filepath.Join(output, "user-42.zip")
	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()
	var manifest struct {
		Version int `json:"version"`
		Imports []struct {
			Target string `json:"target_feed"`
		} `json:"imports"`
	}
	raw, err := os.ReadFile(filepath.Join(output, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 1 || len(manifest.Imports) != 1 || manifest.Imports[0].Target != "9e43d39c-2358-40a4-80ab-08a79a7b21e2" {
		t.Fatalf("manifest=%+v", manifest)
	}
}

func TestSyncStopsCurrentUserAtReplay(t *testing.T) {
	getX := fakeGetX(t, []string{"103", "102", "101"})
	defer getX.Close()
	output := t.TempDir()
	var imported []string
	ff := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/feed/imports" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("X-FF-Import-Target") != "feed-uuid" {
			t.Fatal("missing target")
		}
		if _, err := os.Stat(filepath.Join(output, "42", "page-103-101.zip")); err != nil {
			t.Fatalf("archive was not saved before import: %v", err)
		}
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
		imported = append(imported, metadata.Source.ItemID)
		_ = json.NewEncoder(w).Encode(map[string]any{"created": true, "data": map[string]string{"id": "entry"}})
	}))
	defer ff.Close()
	statePath := filepath.Join(output, "state.json")
	state := syncState{Version: 1, Positions: make(map[string][]syncPosition)}
	api := &getxapi.Client{Endpoint: getX.URL, Key: "getx", HTTP: getX.Client()}
	counts, err := syncAccount(context.Background(), api, getxapi.Account{FeedUUID: "feed-uuid", UserID: "42", BoundaryTweetID: "102"}, ff.URL, "operator", output, statePath, &state, nil, 100, false, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Created != 1 || counts.Replayed != 1 || counts.Capped || len(imported) != 1 || imported[0] != "103" || len(state.Positions) != 0 {
		t.Fatalf("counts=%+v imported=%v state=%v", counts, imported, state.Positions)
	}
	if _, err := os.Stat(filepath.Join(output, "42", "page-103-101.zip")); err != nil {
		t.Fatal(err)
	}
}

func TestLatestSyncPreservesOlderContinuation(t *testing.T) {
	getX := fakeGetX(t, []string{"101"})
	defer getX.Close()
	ff := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"created": false, "data": map[string]string{"id": "entry"}})
	}))
	defer ff.Close()
	output := t.TempDir()
	statePath := filepath.Join(output, "state.json")
	key := "feed-uuid\x0042"
	older := syncPosition{Archive: "42/older.zip", Skip: 7}
	state := syncState{Version: 1, Positions: map[string][]syncPosition{key: {older}}}
	api := &getxapi.Client{Endpoint: getX.URL, Key: "getx", HTTP: getX.Client()}
	if _, err := syncAccount(context.Background(), api, getxapi.Account{FeedUUID: "feed-uuid", UserID: "42"}, ff.URL, "operator", output, statePath, &state, nil, 100, false, false, true); err != nil {
		t.Fatal(err)
	}
	if positions := state.Positions[key]; len(positions) != 1 || positions[0] != older {
		t.Fatalf("positions=%+v", positions)
	}
}

func TestResumeWithoutPendingStartsAtNewestPage(t *testing.T) {
	getX := fakeGetX(t, []string{"101"})
	defer getX.Close()
	posts := 0
	ff := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		posts++
		_ = json.NewEncoder(w).Encode(map[string]any{"created": true, "data": map[string]string{"id": "entry"}})
	}))
	defer ff.Close()
	output := t.TempDir()
	state := syncState{Version: 1, Positions: make(map[string][]syncPosition)}
	api := &getxapi.Client{Endpoint: getX.URL, Key: "getx", HTTP: getX.Client()}
	counts, err := syncAccount(context.Background(), api, getxapi.Account{FeedUUID: "feed-uuid", UserID: "42", Username: "alice"}, ff.URL, "operator", output, filepath.Join(output, "state.json"), &state, nil, 100, false, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Created != 1 || posts != 1 {
		t.Fatalf("counts=%+v posts=%d", counts, posts)
	}
}

func TestSyncPageIsImmutable(t *testing.T) {
	output := t.TempDir()
	account := getxapi.Account{UserID: "42"}
	tweets := []getxapi.Tweet{{ID: "102"}, {ID: "101"}}
	relative, existed, err := writeSyncPage(output, account, tweets, nil)
	if err != nil || existed {
		t.Fatalf("relative=%q existed=%t err=%v", relative, existed, err)
	}
	before, err := os.ReadFile(filepath.Join(output, relative))
	if err != nil {
		t.Fatal(err)
	}
	tweets[0].Text = "changed"
	if _, existed, err = writeSyncPage(output, account, tweets, nil); err != nil || !existed {
		t.Fatalf("existed=%t err=%v", existed, err)
	}
	after, err := os.ReadFile(filepath.Join(output, relative))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("existing page archive was overwritten")
	}
}

func TestReplayManifestListsRetainedPages(t *testing.T) {
	output := t.TempDir()
	account := getxapi.Account{FeedUUID: "feed-uuid", UserID: "42"}
	if _, _, err := writeSyncPage(output, account, []getxapi.Tweet{{ID: "102", AccountID: "42"}, {ID: "101", AccountID: "42"}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "42", "ignore.txt"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := writeReplayManifest(output, []getxapi.Account{account}); err != nil {
		t.Fatal(err)
	}
	var manifest batchManifest
	raw, err := os.ReadFile(filepath.Join(output, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Imports) != 1 {
		t.Fatalf("imports=%+v", manifest.Imports)
	}
	item := manifest.Imports[0]
	if item.Archive != "42/page-102-101.zip" || item.TargetFeed != "feed-uuid" || item.State != "replay-state/42.db" || item.Report != "production-replay.jsonl" {
		t.Fatalf("import=%+v", item)
	}
	if err := runBatch(filepath.Join(output, "manifest.json"), []string{"--endpoint", "https://friendfeed.example"}); err != nil {
		t.Fatal(err)
	}
}

func TestSyncResumesInsidePaidPageAfterLimit(t *testing.T) {
	getX := fakeGetX(t, []string{"103", "102", "101"})
	defer getX.Close()
	var mu sync.Mutex
	var imported []string
	ff := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseMultipartForm(1 << 20)
		var metadata struct {
			Source struct {
				ItemID string `json:"item_id"`
			} `json:"source"`
		}
		_ = json.Unmarshal([]byte(r.FormValue("metadata")), &metadata)
		mu.Lock()
		imported = append(imported, metadata.Source.ItemID)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"created": true, "data": map[string]string{"id": "entry"}})
	}))
	defer ff.Close()
	output := t.TempDir()
	statePath := filepath.Join(output, "state.json")
	state := syncState{Version: 1, Positions: make(map[string][]syncPosition)}
	api := &getxapi.Client{Endpoint: getX.URL, Key: "getx", HTTP: getX.Client()}
	counts, err := syncAccount(context.Background(), api, getxapi.Account{FeedUUID: "feed-uuid", UserID: "42"}, ff.URL, "operator", output, statePath, &state, nil, 2, false, false, true)
	if err != nil || !counts.Capped {
		t.Fatalf("counts=%+v err=%v", counts, err)
	}
	position := state.Positions["feed-uuid\x0042"][0]
	if position.Skip != 2 {
		t.Fatalf("position=%+v", position)
	}
	getX.Close()
	counts, err = syncAccount(context.Background(), api, getxapi.Account{FeedUUID: "feed-uuid", UserID: "42"}, ff.URL, "operator", output, statePath, &state, nil, 2, false, true, true)
	if err != nil || counts.Capped || len(state.Positions) != 0 {
		t.Fatalf("counts=%+v err=%v state=%v", counts, err, state.Positions)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(imported) != 3 || imported[0] != "103" || imported[2] != "101" {
		t.Fatalf("imported=%v", imported)
	}
}

func TestSyncSkipsUnexpectedReplyWithoutPostingIt(t *testing.T) {
	getX := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"userId": "42", "has_more": false, "tweets": []any{
			map[string]any{"id": "102", "url": "https://x.com/a/status/102", "text": "reply", "createdAt": "Mon Jan 12 13:44:55 +0000 2026", "isReply": true, "inReplyToId": "1", "author": map[string]string{"id": "42"}},
			map[string]any{"id": "101", "url": "https://x.com/a/status/101", "text": "post", "createdAt": "Mon Jan 12 13:44:55 +0000 2026", "author": map[string]string{"id": "42"}},
		}})
	}))
	defer getX.Close()
	posts := 0
	ff := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		posts++
		_ = json.NewEncoder(w).Encode(map[string]any{"created": true, "data": map[string]string{"id": "entry"}})
	}))
	defer ff.Close()
	output := t.TempDir()
	statePath := filepath.Join(output, "state.json")
	state := syncState{Version: 1, Positions: make(map[string][]syncPosition)}
	api := &getxapi.Client{Endpoint: getX.URL, Key: "getx", HTTP: getX.Client()}
	counts, err := syncAccount(context.Background(), api, getxapi.Account{FeedUUID: "feed-uuid", UserID: "42"}, ff.URL, "operator", output, statePath, &state, nil, 100, false, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Created != 1 || counts.Replies != 1 || posts != 1 {
		t.Fatalf("counts=%+v posts=%d", counts, posts)
	}
}

func TestSyncRecordsPermanentContentFailureAndContinues(t *testing.T) {
	getX := fakeGetX(t, []string{"102", "101"})
	defer getX.Close()
	posts := 0
	ff := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		posts++
		if posts == 1 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"created": true, "data": map[string]string{"id": "entry"}})
	}))
	defer ff.Close()
	output := t.TempDir()
	statePath := filepath.Join(output, "state.json")
	state := syncState{Version: 1, Positions: make(map[string][]syncPosition)}
	api := &getxapi.Client{Endpoint: getX.URL, Key: "getx", HTTP: getX.Client()}
	counts, err := syncAccount(context.Background(), api, getxapi.Account{FeedUUID: "feed-uuid", UserID: "42"}, ff.URL, "operator", output, statePath, &state, nil, 100, false, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Created != 1 || counts.Rejected != 1 || posts != 2 {
		t.Fatalf("counts=%+v posts=%d", counts, posts)
	}
}

func TestSyncContinuesAfterUnavailableAccount(t *testing.T) {
	directory := t.TempDir()
	accounts := filepath.Join(directory, "accounts.tsv")
	if err := os.WriteFile(accounts, []byte("feed_id\tfeed_uuid\ttwitter_username\ttwitter_user_id\n"+
		"blocked\t9e43d39c-2358-40a4-80ab-08a79a7b21e2\tblocked\t42\n"+
		"working\tae67a1f6-11ee-459e-b64f-b41e7a8f2238\tworking\t43\n"), 0600); err != nil {
		t.Fatal(err)
	}
	getXKey, operatorKey := filepath.Join(directory, "getx-key"), filepath.Join(directory, "operator-key")
	for _, path := range []string{getXKey, operatorKey} {
		if err := os.WriteFile(path, []byte("secret\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	getX := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID := r.URL.Query().Get("userId")
		if userID == "42" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"userId": userID, "has_more": false, "tweets": []any{
			map[string]any{"id": "101", "url": "https://x.com/a/status/101", "text": "post", "createdAt": "Mon Jan 12 13:44:55 +0000 2026", "author": map[string]string{"id": userID}},
		}})
	}))
	defer getX.Close()
	posts := 0
	ff := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		posts++
		_ = json.NewEncoder(w).Encode(map[string]any{"created": true, "data": map[string]string{"id": "entry"}})
	}))
	defer ff.Close()
	output := filepath.Join(directory, "output")
	err := runSync([]string{"--accounts-file", accounts, "--endpoint", ff.URL, "--getxapi-key-file", getXKey,
		"--operator-key-file", operatorKey, "--getxapi-endpoint", getX.URL, "--output", output, "--no-media"})
	if err != nil {
		t.Fatal(err)
	}
	if posts != 1 {
		t.Fatalf("posts=%d", posts)
	}
	report, err := os.ReadFile(filepath.Join(output, "twitter-sync.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(report), `"account_id":"42"`) || !strings.Contains(string(report), `"result":"account_unavailable"`) {
		t.Fatalf("report=%s", report)
	}
}

func fakeGetX(t *testing.T, ids []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer getx" {
			t.Fatal("missing GetXAPI key")
		}
		tweets := make([]map[string]any, 0, len(ids))
		for _, id := range ids {
			tweets = append(tweets, map[string]any{"id": id, "url": "https://x.com/a/status/" + id, "text": id, "createdAt": "Mon Jan 12 13:44:55 +0000 2026", "author": map[string]string{"id": "42"}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"userId": "42", "has_more": false, "tweets": tweets})
	}))
}
