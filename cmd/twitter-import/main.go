package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yinhm/twitter-import/internal/archive"
	"github.com/yinhm/twitter-import/internal/client"
	"github.com/yinhm/twitter-import/internal/state"
)

type reportRecord struct {
	ItemID  string `json:"item_id"`
	Result  string `json:"result"`
	EntryID string `json:"entry_id,omitempty"`
	At      string `json:"at"`
	Error   string `json:"error,omitempty"`
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: twitter-import inspect ARCHIVE | import ARCHIVE [flags] | batch MANIFEST [flags] | collect [flags] | sync [flags]")
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "inspect":
		if len(os.Args) < 3 {
			usage()
			os.Exit(2)
		}
		err = inspect(os.Args[2])
	case "import":
		if len(os.Args) < 3 {
			usage()
			os.Exit(2)
		}
		err = runImport(os.Args[2], os.Args[3:])
	case "batch":
		if len(os.Args) < 3 {
			usage()
			os.Exit(2)
		}
		err = runBatch(os.Args[2], os.Args[3:])
	case "collect":
		err = runCollect(os.Args[2:])
	case "sync":
		err = runSync(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type importOptions struct {
	archive, endpoint, key, state, report, target string
	limit                                         int
	includeReplies                                bool
}

type batchManifest struct {
	Version int           `json:"version"`
	Imports []batchImport `json:"imports"`
}

type batchImport struct {
	SourceType     string `json:"source_type"`
	Archive        string `json:"archive"`
	TargetFeed     string `json:"target_feed"`
	State          string `json:"state"`
	Report         string `json:"report"`
	IncludeReplies bool   `json:"include_replies"`
}

func inspect(path string) error {
	reader, err := archive.Open(path)
	if err != nil {
		return err
	}
	defer reader.Close()
	var total, replies, retweets, quotes, media int
	var mediaBytes uint64
	var earliest, latest time.Time
	invalid, err := reader.Iterate(func(tweet archive.Tweet) error {
		total++
		media += len(tweet.Media)
		for _, file := range tweet.Media {
			mediaBytes += file.UncompressedSize64
		}
		if tweet.ReplyTo != "" {
			replies++
		}
		if tweet.RetweetOf != "" {
			retweets++
		}
		if tweet.QuoteOf != "" {
			quotes++
		}
		if earliest.IsZero() || tweet.CreatedAt.Before(earliest) {
			earliest = tweet.CreatedAt
		}
		if latest.IsZero() || tweet.CreatedAt.After(latest) {
			latest = tweet.CreatedAt
		}
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("source_scope=%s account_id=%s tweets=%d replies=%d retweets=%d quotes=%d media=%d media_bytes=%d invalid=%d earliest=%s latest=%s\n", reader.ScopeID(), reader.AccountID(), total, replies, retweets, quotes, media, mediaBytes, invalid, earliest.Format(time.RFC3339), latest.Format(time.RFC3339))
	return nil
}

func readKey(path string) (string, error) {
	if path == "" {
		key := strings.TrimSpace(os.Getenv("FF_FEED_API_KEY"))
		if key == "" {
			return "", errors.New("FF_FEED_API_KEY or --key-file is required")
		}
		return key, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if info.Mode().Perm()&0077 != 0 {
		return "", errors.New("key file permissions must be 0600")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	key := strings.TrimSpace(string(raw))
	if key == "" {
		return "", errors.New("key file is empty")
	}
	return key, nil
}

func tweetIDFromURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	host := strings.ToLower(parsed.Hostname())
	if host != "twitter.com" && host != "www.twitter.com" && host != "x.com" && host != "www.x.com" && host != "mobile.twitter.com" {
		return ""
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) < 3 {
		return ""
	}
	segment := strings.ToLower(parts[len(parts)-2])
	if segment != "status" && segment != "statuses" {
		return ""
	}
	for _, r := range parts[len(parts)-1] {
		if r < '0' || r > '9' {
			return ""
		}
	}
	return parts[len(parts)-1]
}

func appendReport(writer *bufio.Writer, record reportRecord) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err := writer.Write(append(raw, '\n')); err != nil {
		return err
	}
	return writer.Flush()
}

func runImport(archivePath string, args []string) error {
	flags := flag.NewFlagSet("import", flag.ContinueOnError)
	endpoint := flags.String("endpoint", "", "FriendFeed base URL")
	keyFile := flags.String("key-file", "", "0600 Feed API key file")
	statePath := flags.String("state", "twitter-import.db", "checkpoint database")
	reportPath := flags.String("report", "twitter-import.jsonl", "append-only result report")
	limit := flags.Int("limit", 0, "maximum archive items, 0 means all")
	includeReplies := flags.Bool("include-replies", false, "include reply tweets normally excluded from Feed imports")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *endpoint == "" {
		return errors.New("--endpoint is required")
	}
	key, err := readKey(*keyFile)
	if err != nil {
		return err
	}
	return executeImport(importOptions{
		archive: archivePath, endpoint: *endpoint, key: key, state: *statePath,
		report: *reportPath, limit: *limit, includeReplies: *includeReplies,
	})
}

func runBatch(manifestPath string, args []string) error {
	flags := flag.NewFlagSet("batch", flag.ContinueOnError)
	endpoint := flags.String("endpoint", "", "FriendFeed base URL")
	keyFile := flags.String("key-file", "", "0600 import operator token file")
	limit := flags.Int("limit", 0, "maximum archive items per import, 0 means all")
	apply := flags.Bool("apply", false, "perform imports; default is manifest/archive validation only")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *endpoint == "" {
		return errors.New("--endpoint is required")
	}
	key := ""
	if *apply {
		var err error
		key, err = readKey(*keyFile)
		if err != nil {
			return err
		}
	}
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	var manifest batchManifest
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return fmt.Errorf("decode manifest: %w", err)
	}
	if manifest.Version != 1 || len(manifest.Imports) == 0 {
		return errors.New("manifest version 1 with at least one import is required")
	}
	base := filepath.Dir(manifestPath)
	resolve := func(path string) string {
		if filepath.IsAbs(path) {
			return path
		}
		return filepath.Join(base, path)
	}
	for i, item := range manifest.Imports {
		if item.Archive == "" || item.TargetFeed == "" || item.State == "" || item.Report == "" {
			return fmt.Errorf("manifest import %d requires archive, target_feed, state and report", i+1)
		}
		if item.SourceType != "" && item.SourceType != "twitter-official" && item.SourceType != "twitter-import-v1" {
			return fmt.Errorf("manifest import %d has unsupported source_type %q", i+1, item.SourceType)
		}
		fmt.Printf("import=%d/%d target=%s archive=%s\n", i+1, len(manifest.Imports), item.TargetFeed, item.Archive)
		if !*apply {
			eligible, replies, err := previewImport(resolve(item.Archive), item.IncludeReplies, *limit)
			if err != nil {
				return fmt.Errorf("preview import %d target %q: %w", i+1, item.TargetFeed, err)
			}
			fmt.Printf("would_process=%d replies_skipped=%d\n", eligible, replies)
			continue
		}
		if err := executeImport(importOptions{
			archive: resolve(item.Archive), endpoint: *endpoint, key: key,
			state: resolve(item.State), report: resolve(item.Report), target: item.TargetFeed,
			limit: *limit, includeReplies: item.IncludeReplies,
		}); err != nil {
			return fmt.Errorf("import %d target %q: %w", i+1, item.TargetFeed, err)
		}
	}
	if !*apply {
		fmt.Println("dry-run only; rerun with --apply to import")
	}
	return nil
}

func previewImport(archivePath string, includeReplies bool, limit int) (int, int, error) {
	reader, err := archive.Open(archivePath)
	if err != nil {
		return 0, 0, err
	}
	defer reader.Close()
	if reader.ScopeID() == "" {
		return 0, 0, errors.New("source scope is missing")
	}
	eligible, replies := 0, 0
	_, err = reader.Iterate(func(tweet archive.Tweet) error {
		if tweet.ReplyTo != "" && !includeReplies {
			replies++
			return nil
		}
		if limit == 0 || eligible < limit {
			eligible++
		}
		return nil
	})
	return eligible, replies, err
}

func executeImport(options importOptions) error {
	reader, err := archive.Open(options.archive)
	if err != nil {
		return err
	}
	defer reader.Close()
	if reader.ScopeID() == "" {
		return errors.New("source scope is missing")
	}
	db, err := state.Open(options.state)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.ClearLegacy(); err != nil {
		return err
	}
	api := &client.Client{Endpoint: options.endpoint, Key: options.key, Target: options.target, HTTP: &http.Client{Timeout: 2 * time.Minute}}
	ctx := context.Background()
	identity := options.target
	if identity == "" {
		feed, err := api.GetFeed(ctx)
		if err != nil {
			return err
		}
		identity = feed.UUID
	}
	if err := db.Bind(strings.TrimRight(options.endpoint, "/") + "\x00" + identity + "\x00" + reader.ScopeID()); err != nil {
		return err
	}
	if options.target == "" {
		if err := api.ListEntries(ctx, func(entry client.Entry) error {
			if id := tweetIDFromURL(entry.SourceURL); id != "" {
				return db.MarkLegacy(id)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("scan legacy entries: %w", err)
		}
	}
	report, err := os.OpenFile(options.report, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer report.Close()
	writer := bufio.NewWriter(report)
	processed := 0
	_, err = reader.Iterate(func(tweet archive.Tweet) error {
		if options.limit > 0 && processed >= options.limit {
			return nil
		}
		if tweet.ReplyTo != "" && !options.includeReplies {
			return appendReport(writer, reportRecord{
				ItemID: tweet.ID, Result: "reply_skipped", At: time.Now().UTC().Format(time.RFC3339Nano),
			})
		}
		if db.HasDone(tweet.ID) {
			return nil
		}
		processed++
		now := time.Now().UTC().Format(time.RFC3339Nano)
		if db.HasLegacy(tweet.ID) {
			if err := db.MarkDone(tweet.ID, "legacy_skipped"); err != nil {
				return err
			}
			return appendReport(writer, reportRecord{ItemID: tweet.ID, Result: "legacy_skipped", At: now})
		}
		metadata := client.ImportMetadata{PublishedAt: tweet.CreatedAt.Format(time.RFC3339Nano), BodyHTML: strings.ReplaceAll(html.EscapeString(tweet.Text), "\n", "<br>")}
		metadata.Source.Kind = "twitter"
		metadata.Source.AccountID = tweet.AccountID
		if metadata.Source.AccountID == "" {
			metadata.Source.AccountID = reader.AccountID()
		}
		metadata.Source.ItemID = tweet.ID
		metadata.Source.URL = tweet.SourceURL
		if metadata.Source.URL == "" {
			metadata.Source.URL = "https://x.com/i/status/" + tweet.ID
		}
		result, importErr := api.Import(ctx, metadata, tweet.Media)
		if importErr != nil {
			if client.IsFatal(importErr) {
				_ = appendReport(writer, reportRecord{ItemID: tweet.ID, Result: "fatal", At: now, Error: importErr.Error()})
				return importErr
			}
			if client.IsPermanent(importErr) {
				_ = appendReport(writer, reportRecord{ItemID: tweet.ID, Result: "rejected", At: now, Error: importErr.Error()})
				return db.MarkDone(tweet.ID, "rejected")
			}
			_ = appendReport(writer, reportRecord{ItemID: tweet.ID, Result: "retry_exhausted", At: now, Error: importErr.Error()})
			return importErr
		}
		status := "replayed"
		if result.Created {
			status = "created"
		}
		if err := db.MarkDone(tweet.ID, status); err != nil {
			return err
		}
		return appendReport(writer, reportRecord{ItemID: tweet.ID, Result: status, EntryID: result.Data.ID, At: now})
	})
	if err != nil {
		return err
	}
	fmt.Printf("processed=%d state=%s report=%s\n", processed, filepath.Clean(options.state), filepath.Clean(options.report))
	return nil
}
