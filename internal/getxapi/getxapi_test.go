package getxapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/yinhm/twitter-import/internal/archive"
	"github.com/yinhm/twitter-import/internal/getxapi"
)

func TestReadAccountsAcceptsFixedSyncBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.tsv")
	data := "feed_id\tfeed_uuid\ttwitter_username\ttwitter_user_id\tboundary_tweet_id\tboundary_at\n" +
		"alice\t9e43d39c-2358-40a4-80ab-08a79a7b21e2\talice\t42\t100\t2026-01-12T13:44:55Z\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	accounts, err := getxapi.ReadAccounts(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 1 || accounts[0].BoundaryTweetID != "100" || accounts[0].BoundaryAt.IsZero() {
		t.Fatalf("accounts=%+v", accounts)
	}
}

func TestUserTweetsMapsAndWritesImportBundle(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" || r.URL.Query().Get("userId") != "42" {
			t.Fatal("missing GetXAPI identity")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"userId": "42", "has_more": false, "tweets": []any{map[string]any{
				"id": "100", "url": "https://x.com/alice/status/100", "text": "see https://t.co/x",
				"createdAt": "Mon Jan 12 13:44:55 +0000 2026", "author": map[string]string{"id": "42"},
				"entities": map[string]any{"urls": []any{map[string]string{"url": "https://t.co/x", "expanded_url": "https://example.com/post"}}},
			}},
		})
	}))
	defer server.Close()
	api := &getxapi.Client{Endpoint: server.URL, Key: "secret", HTTP: server.Client()}
	page, err := api.UserTweets(context.Background(), "42", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Tweets) != 1 || page.Tweets[0].Text != "see https://example.com/post" {
		t.Fatalf("tweets=%+v", page.Tweets)
	}
	path := filepath.Join(t.TempDir(), "bundle.zip")
	account := getxapi.Account{FeedUUID: "9e43d39c-2358-40a4-80ab-08a79a7b21e2", Username: "alice", UserID: "42"}
	if err := getxapi.WriteBundle(path, account, page.Tweets, map[string][]getxapi.Media{"100": {{Name: "100.jpg", Data: []byte("image")}}}); err != nil {
		t.Fatal(err)
	}
	reader, err := archive.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	_, err = reader.Iterate(func(tweet archive.Tweet) error {
		if tweet.ID != "100" || tweet.AccountID != "42" || len(tweet.Media) != 1 {
			t.Fatalf("tweet=%+v", tweet)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestUserTweetsRejectsWrongAuthor(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"userId":"42","tweets":[{"id":"100","url":"https://x.com/x/status/100","text":"x","createdAt":"Mon Jan 12 13:44:55 +0000 2026","author":{"id":"99"}}]}`))
	}))
	defer server.Close()
	api := &getxapi.Client{Endpoint: server.URL, Key: "secret", HTTP: server.Client()}
	if _, err := api.UserTweets(context.Background(), "42", ""); err == nil {
		t.Fatal("expected author mismatch")
	}
}

func TestUserTweetsClassifiesUnavailableAccount(t *testing.T) {
	for _, statusCode := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		t.Run(http.StatusText(statusCode), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(statusCode)
			}))
			defer server.Close()
			api := &getxapi.Client{Endpoint: server.URL, Key: "secret", HTTP: server.Client()}
			_, err := api.UserTweets(context.Background(), "42", "")
			if !getxapi.IsAccountUnavailable(err) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}
