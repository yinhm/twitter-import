package client

import (
	"archive/zip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientListsLegacySourcesAndImportsMetadata(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Error("missing bearer credential")
		}
		switch r.URL.Path {
		case "/api/v1/feed":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]string{"uuid": "feed-uuid"}})
		case "/api/v1/feed/entries":
			_ = json.NewEncoder(w).Encode(map[string]any{"data": []map[string]string{{"id": "old", "source_url": "https://x.com/a/status/1"}}, "pagination": map[string]string{"next_cursor": ""}})
		case "/api/v1/feed/imports":
			requests++
			if r.Header.Get("X-FF-Import-Target") != "archive-target" {
				t.Errorf("target header=%q", r.Header.Get("X-FF-Import-Target"))
			}
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Fatal(err)
			}
			var metadata ImportMetadata
			if err := json.Unmarshal([]byte(r.FormValue("metadata")), &metadata); err != nil {
				t.Fatal(err)
			}
			if metadata.Source.ItemID != "2" || metadata.Source.AccountID != "9" {
				t.Errorf("metadata=%+v", metadata)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"created":true,"data":{"id":"entry"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := &Client{Endpoint: server.URL, Key: "secret", HTTP: server.Client()}
	feed, err := client.GetFeed(context.Background())
	if err != nil || feed.UUID != "feed-uuid" {
		t.Fatalf("feed=%+v err=%v", feed, err)
	}
	seen := ""
	if err := client.ListEntries(context.Background(), func(entry Entry) error { seen = entry.SourceURL; return nil }); err != nil {
		t.Fatal(err)
	}
	if seen == "" {
		t.Fatal("source URL not returned")
	}
	metadata := ImportMetadata{PublishedAt: "2020-01-01T00:00:00Z", BodyHTML: "body"}
	metadata.Source.Kind = "twitter"
	metadata.Source.AccountID = "9"
	metadata.Source.ItemID = "2"
	metadata.Source.URL = "https://x.com/a/status/2"
	client.Target = "archive-target"
	result, err := client.Import(context.Background(), metadata, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Created || result.Data.ID != "entry" || requests != 1 {
		t.Fatalf("result=%+v requests=%d", result, requests)
	}
}

func TestImportRejectsOversizedArchiveMediaBeforeHTTP(t *testing.T) {
	client := &Client{Endpoint: "http://127.0.0.1:1", Key: "secret", HTTP: http.DefaultClient}
	metadata := ImportMetadata{PublishedAt: "2020-01-01T00:00:00Z", BodyHTML: "body"}
	metadata.Source.ItemID = "2"
	_, err := client.Import(context.Background(), metadata, []*zip.File{{FileHeader: zip.FileHeader{UncompressedSize64: maxFileBytes + 1}}})
	if err == nil {
		t.Fatal("oversized media accepted")
	}
	if !IsPermanent(err) {
		t.Fatalf("error is not permanent: %v", err)
	}
}

func TestImportClassifiesCapabilityFailuresAsFatal(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(code)
			}))
			defer server.Close()
			api := &Client{Endpoint: server.URL, Key: "secret", HTTP: server.Client()}
			metadata := ImportMetadata{PublishedAt: "2020-01-01T00:00:00Z", BodyHTML: "body"}
			metadata.Source.ItemID = "2"

			_, err := api.Import(context.Background(), metadata, nil)
			if !IsFatal(err) {
				t.Fatalf("HTTP %d error is not fatal: %v", code, err)
			}
			if IsPermanent(err) {
				t.Fatalf("HTTP %d error was classified as content-level permanent: %v", code, err)
			}
		})
	}
}

func TestImportTreatsEveryServerErrorAsTemporary(t *testing.T) {
	for _, code := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(code)
			}))
			defer server.Close()
			api := &Client{Endpoint: server.URL, Key: "secret", HTTP: server.Client()}
			metadata := ImportMetadata{PublishedAt: "2020-01-01T00:00:00Z", BodyHTML: "body"}
			metadata.Source.ItemID = "2"

			_, retryAfter, err := api.importOnce(context.Background(), metadata, nil)
			if err == nil || retryAfter < 0 || IsPermanent(err) || IsFatal(err) {
				t.Fatalf("HTTP %d retry=%v err=%v", code, retryAfter, err)
			}
		})
	}
}
