package archive

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"
)

func TestReaderStreamsArchiveAndAssociatesMedia(t *testing.T) {
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
	media, _ := writer.Create("data/tweets_media/42-photo.jpg")
	_, _ = media.Write([]byte("jpeg"))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.AccountID() != "12345" {
		t.Fatalf("account=%q", reader.AccountID())
	}
	count := 0
	invalid, err := reader.Iterate(func(tweet Tweet) error {
		count++
		if tweet.ID != "42" || tweet.Text != "hello" || len(tweet.Media) != 1 {
			t.Fatalf("tweet=%+v", tweet)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if invalid != 0 {
		t.Fatalf("invalid=%d", invalid)
	}
	if count != 1 {
		t.Fatalf("count=%d", count)
	}
}

func TestReaderCountsInvalidTweetsAndContinues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "archive.zip")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	tweets, _ := writer.Create("data/tweets.js")
	_, _ = tweets.Write([]byte(`window.YTD.tweets.part0 = [
		{"tweet":{"id_str":"","full_text":"missing id","created_at":"Mon Aug 17 12:34:56 +0000 2020"}},
		{"tweet":{"id_str":"41","full_text":"bad time","created_at":"bad"}},
		{"tweet":{"id_str":"42","full_text":"valid","created_at":"Mon Aug 17 12:34:56 +0000 2020"}}
	]`))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	seen := 0
	invalid, err := reader.Iterate(func(Tweet) error { seen++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if invalid != 2 || seen != 1 {
		t.Fatalf("invalid=%d seen=%d", invalid, seen)
	}
}

func TestReaderStreamsTwitterImportBundleWithPerItemAuthors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bundle.zip")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	source, _ := writer.Create("source.json")
	_, _ = source.Write([]byte(`{"version":1,"format":"twitter-import/v1","collector":"twikit","collected_at":"2026-09-01T00:00:00Z","collection":{"kind":"list","id":"230704954"}}`))
	items, _ := writer.Create("items.jsonl")
	_, _ = items.Write([]byte(`{"id":"42","account_id":"12345","created_at":"2020-08-17T12:34:56Z","text":"hello","url":"https://x.com/a/status/42","media":["media/42-photo.jpg"]}` + "\n"))
	media, _ := writer.Create("media/42-photo.jpg")
	_, _ = media.Write([]byte("jpeg"))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.ScopeID() != "list:230704954" {
		t.Fatalf("scope=%q", reader.ScopeID())
	}
	invalid, err := reader.Iterate(func(tweet Tweet) error {
		if tweet.ID != "42" || tweet.AccountID != "12345" || tweet.SourceURL != "https://x.com/a/status/42" || len(tweet.Media) != 1 {
			t.Fatalf("tweet=%+v", tweet)
		}
		return nil
	})
	if err != nil || invalid != 0 {
		t.Fatalf("invalid=%d err=%v", invalid, err)
	}
}
