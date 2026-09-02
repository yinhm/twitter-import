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
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yinhm/twitter-import/internal/archive"
	"github.com/yinhm/twitter-import/internal/client"
	"github.com/yinhm/twitter-import/internal/getxapi"
)

type syncPosition struct {
	Archive    string `json:"archive,omitempty"`
	Cursor     string `json:"cursor,omitempty"`
	NextCursor string `json:"next_cursor,omitempty"`
	Skip       int    `json:"skip,omitempty"`
	Keep       bool   `json:"keep,omitempty"`
}

type syncState struct {
	Version   int                       `json:"version"`
	Positions map[string][]syncPosition `json:"positions"`
}

type syncCounts struct {
	Created, Replayed, Replies, Rejected, MediaMissing int
	Capped                                             bool
}

var (
	stopAtReplay = errors.New("stop at replay")
	stopAtLimit  = errors.New("stop at limit")
)

func readSecret(path, environment, label string) (string, error) {
	if path == "" {
		value := strings.TrimSpace(os.Getenv(environment))
		if value == "" {
			return "", fmt.Errorf("%s or %s is required", environment, label)
		}
		return value, nil
	}
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return "", fmt.Errorf("%s must be a regular mode-0600 file", label)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return "", fmt.Errorf("%s is empty", label)
	}
	return value, nil
}

func getXHTTP() *http.Client { return &http.Client{Timeout: 30 * time.Second} }

func mediaHTTP() *http.Client {
	return &http.Client{Timeout: 2 * time.Minute, CheckRedirect: func(request *http.Request, via []*http.Request) error {
		host := strings.ToLower(request.URL.Hostname())
		if len(via) >= 5 || request.URL.Scheme != "https" || (host != "pbs.twimg.com" && host != "video.twimg.com") {
			return errors.New("Twitter media redirect is not allowed")
		}
		return nil
	}}
}

func runCollect(args []string) error {
	flags := flag.NewFlagSet("collect", flag.ContinueOnError)
	accountsPath := flags.String("accounts-file", "", "TSV mapping Twitter users to target Feed UUIDs")
	output := flags.String("output", "output", "output directory")
	keyFile := flags.String("getxapi-key-file", "", "0600 GetXAPI key file")
	getXEndpoint := flags.String("getxapi-endpoint", getxapi.DefaultEndpoint, "GetXAPI base URL")
	limit := flags.Int("limit", 100, "maximum tweets per user")
	noMedia := flags.Bool("no-media", false, "do not download media")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *accountsPath == "" || *limit < 1 {
		return errors.New("--accounts-file and a positive --limit are required")
	}
	key, err := readSecret(*keyFile, "GETXAPI_KEY", "--getxapi-key-file")
	if err != nil {
		return err
	}
	accounts, err := getxapi.ReadAccounts(*accountsPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*output, 0700); err != nil {
		return err
	}
	api := &getxapi.Client{Endpoint: *getXEndpoint, Key: key, HTTP: getXHTTP()}
	ctx := context.Background()
	imports := make([]map[string]any, 0, len(accounts))
	for index, account := range accounts {
		tweets, err := collectTweets(ctx, api, account.UserID, *limit)
		if err != nil {
			return fmt.Errorf("collect @%s: %w", account.Username, err)
		}
		objects, _, err := downloadPageMedia(ctx, tweets, nil, *noMedia)
		if err != nil {
			return err
		}
		archiveName := "user-" + account.UserID + ".zip"
		if err := getxapi.WriteBundle(filepath.Join(*output, archiveName), account, tweets, objects); err != nil {
			return err
		}
		imports = append(imports, map[string]any{"source_type": "twitter-import-v1", "archive": archiveName, "target_feed": account.FeedUUID, "state": "state/" + account.UserID + ".db", "report": "state/" + account.UserID + ".jsonl"})
		fmt.Printf("collected=%d/%d user=@%s tweets=%d archive=%s\n", index+1, len(accounts), account.Username, len(tweets), archiveName)
	}
	if err := os.MkdirAll(filepath.Join(*output, "state"), 0700); err != nil {
		return err
	}
	return writePrivateJSON(filepath.Join(*output, "manifest.json"), map[string]any{"version": 1, "imports": imports})
}

func collectTweets(ctx context.Context, api *getxapi.Client, userID string, limit int) ([]getxapi.Tweet, error) {
	var tweets []getxapi.Tweet
	seen, cursors := make(map[string]bool), make(map[string]bool)
	cursor := ""
	for len(tweets) < limit {
		if cursors[cursor] {
			return nil, errors.New("GetXAPI repeated a pagination cursor")
		}
		cursors[cursor] = true
		page, err := api.UserTweets(ctx, userID, cursor)
		if err != nil {
			return nil, err
		}
		for _, tweet := range page.Tweets {
			if !seen[tweet.ID] {
				seen[tweet.ID] = true
				tweets = append(tweets, tweet)
				if len(tweets) == limit {
					break
				}
			}
		}
		if !page.HasMore || page.NextCursor == "" || len(page.Tweets) == 0 {
			break
		}
		cursor = page.NextCursor
	}
	return tweets, nil
}

func runSync(args []string) error {
	flags := flag.NewFlagSet("sync", flag.ContinueOnError)
	accountsPath := flags.String("accounts-file", "", "TSV mapping Twitter users to target Feed UUIDs")
	endpoint := flags.String("endpoint", "", "FriendFeed base URL")
	getXKeyFile := flags.String("getxapi-key-file", "", "0600 GetXAPI key file")
	operatorKeyFile := flags.String("operator-key-file", "", "0600 FriendFeed import operator token file")
	getXEndpoint := flags.String("getxapi-endpoint", getxapi.DefaultEndpoint, "GetXAPI base URL")
	output := flags.String("output", "output", "ZIP, continuation and report directory")
	limit := flags.Int("limit", 100, "maximum tweets examined per user per run")
	full := flags.Bool("full", false, "ignore replay boundaries and limits; may incur substantial GetXAPI charges")
	resume := flags.Bool("resume", false, "resume saved local ZIP continuations instead of checking the latest page")
	noMedia := flags.Bool("no-media", false, "do not download media")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *accountsPath == "" || *endpoint == "" || *limit < 1 {
		return errors.New("--accounts-file, --endpoint and a positive --limit are required")
	}
	if *full && *resume {
		return errors.New("--full and --resume are mutually exclusive")
	}
	if err := os.MkdirAll(*output, 0700); err != nil {
		return err
	}
	getXKey, err := readSecret(*getXKeyFile, "GETXAPI_KEY", "--getxapi-key-file")
	if err != nil {
		return err
	}
	operatorKey, err := readSecret(*operatorKeyFile, "FF_IMPORT_OPERATOR_KEY", "--operator-key-file")
	if err != nil {
		return err
	}
	accounts, err := getxapi.ReadAccounts(*accountsPath)
	if err != nil {
		return err
	}
	statePath, reportPath := filepath.Join(*output, "twitter-sync.json"), filepath.Join(*output, "twitter-sync.jsonl")
	state, err := loadSyncState(statePath)
	if err != nil {
		return err
	}
	if *full {
		fmt.Fprintln(os.Stderr, "warning: --full ignores replay boundaries and the per-user limit; GetXAPI charges may be substantial")
	}
	report, err := openPrivateAppend(reportPath)
	if err != nil {
		return err
	}
	defer report.Close()
	reporter := bufio.NewWriter(report)
	getX := &getxapi.Client{Endpoint: *getXEndpoint, Key: getXKey, HTTP: getXHTTP()}
	ctx := context.Background()
	unavailable, failed := 0, 0
	for index, account := range accounts {
		counts, err := syncAccount(ctx, getX, account, *endpoint, operatorKey, *output, statePath, &state, reporter, *limit, *full, *resume, *noMedia)
		if err != nil {
			if getxapi.IsAccountUnavailable(err) {
				unavailable++
				if reportErr := appendSyncReport(reporter, reportRecord{AccountID: account.UserID, Result: "account_unavailable", At: time.Now().UTC().Format(time.RFC3339Nano), Error: err.Error()}); reportErr != nil {
					return reportErr
				}
				if manifestErr := writeReplayManifest(*output, accounts); manifestErr != nil {
					return manifestErr
				}
				fmt.Printf("synced=%d/%d user=@%s account_unavailable=true\n", index+1, len(accounts), account.Username)
				continue
			}
			failed++
			if reportErr := appendSyncReport(reporter, reportRecord{AccountID: account.UserID, Result: "sync_failed", At: time.Now().UTC().Format(time.RFC3339Nano), Error: err.Error()}); reportErr != nil {
				return reportErr
			}
			if manifestErr := writeReplayManifest(*output, accounts); manifestErr != nil {
				return fmt.Errorf("sync @%s: %v; rebuild replay manifest: %w", account.Username, err, manifestErr)
			}
			fmt.Printf("synced=%d/%d user=@%s failed=true error=%q\n", index+1, len(accounts), account.Username, err)
			continue
		}
		if err := writeReplayManifest(*output, accounts); err != nil {
			return err
		}
		fmt.Printf("synced=%d/%d user=@%s created=%d replayed=%d replies_skipped=%d rejected=%d media_missing=%d capped=%t resumed=%t\n", index+1, len(accounts), account.Username, counts.Created, counts.Replayed, counts.Replies, counts.Rejected, counts.MediaMissing, counts.Capped, *resume)
	}
	if unavailable == len(accounts) {
		return errors.New("GetXAPI denied every account; check the credential, plan, and account availability")
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d accounts failed; see %s", failed, len(accounts), reportPath)
	}
	return nil
}

func syncAccount(ctx context.Context, getX *getxapi.Client, account getxapi.Account, endpoint, operatorKey, output, statePath string, state *syncState, reporter *bufio.Writer, limit int, full, resume, noMedia bool) (syncCounts, error) {
	var counts syncCounts
	key := account.FeedUUID + "\x00" + account.UserID
	positions := state.Positions[key]
	if full {
		positions = nil
	}
	if len(positions) == 0 || (!resume && (positions[0].Archive != "" || positions[0].Cursor != "")) {
		positions = append([]syncPosition{{}}, positions...)
	}
	state.Positions[key] = positions
	if err := saveSyncState(statePath, *state); err != nil {
		return counts, err
	}
	api := &client.Client{Endpoint: endpoint, Key: operatorKey, Target: account.FeedUUID, HTTP: &http.Client{Timeout: 2 * time.Minute}}
	cursors, examined := make(map[string]bool), 0
	for {
		positions = state.Positions[key]
		if len(positions) == 0 {
			return counts, nil
		}
		position := positions[0]
		if position.Archive == "" {
			if cursors[position.Cursor] {
				return counts, errors.New("GetXAPI repeated a pagination cursor")
			}
			cursors[position.Cursor] = true
			page, err := getX.UserTweets(ctx, account.UserID, position.Cursor)
			if err != nil {
				return counts, err
			}
			if len(page.Tweets) == 0 {
				state.Positions[key] = positions[1:]
				if full || len(state.Positions[key]) == 0 {
					delete(state.Positions, key)
				}
				if err := saveSyncState(statePath, *state); err != nil {
					return counts, err
				}
				if resume && len(state.Positions[key]) != 0 {
					continue
				}
				return counts, nil
			}
			objects, missing, err := downloadPageMedia(ctx, page.Tweets, reporter, noMedia)
			if err != nil {
				return counts, err
			}
			counts.MediaMissing += missing
			archivePath, existed, err := writeSyncPage(output, account, page.Tweets, objects)
			if err != nil {
				return counts, err
			}
			position.Archive, position.Keep = archivePath, full || existed
			if page.HasMore {
				position.NextCursor = page.NextCursor
			}
			positions[0], state.Positions[key] = position, positions
			if err := saveSyncState(statePath, *state); err != nil {
				return counts, err
			}
		}

		archivePath, err := syncArchivePath(output, position.Archive)
		if err != nil {
			return counts, err
		}
		processed, complete, replay, err := processSyncArchive(ctx, archivePath, position.Skip, api, account, reporter, full, limit-examined, &counts, func(skip int, keep bool) error {
			current := state.Positions[key]
			current[0].Skip, current[0].Keep = skip, current[0].Keep || keep
			state.Positions[key] = current
			return saveSyncState(statePath, *state)
		})
		examined += processed
		positions, position = state.Positions[key], state.Positions[key][0]
		if err != nil {
			return counts, err
		}
		if replay {
			if !position.Keep {
				_ = os.Remove(archivePath)
			}
			state.Positions[key] = positions[1:]
			if len(state.Positions[key]) == 0 {
				delete(state.Positions, key)
			}
			return counts, saveSyncState(statePath, *state)
		}
		if !complete {
			counts.Capped = true
			return counts, nil
		}
		if !position.Keep {
			_ = os.Remove(archivePath)
		}
		if position.NextCursor == "" {
			state.Positions[key] = positions[1:]
			if full || len(state.Positions[key]) == 0 {
				delete(state.Positions, key)
			}
			if err := saveSyncState(statePath, *state); err != nil {
				return counts, err
			}
			if resume && len(state.Positions[key]) != 0 && examined < limit {
				continue
			}
			return counts, nil
		}
		positions[0] = syncPosition{Cursor: position.NextCursor}
		state.Positions[key] = positions
		if err := saveSyncState(statePath, *state); err != nil {
			return counts, err
		}
		if !full && examined >= limit {
			counts.Capped = true
			return counts, nil
		}
	}
}

func downloadPageMedia(ctx context.Context, tweets []getxapi.Tweet, reporter *bufio.Writer, noMedia bool) (map[string][]getxapi.Media, int, error) {
	objects, missing := make(map[string][]getxapi.Media), 0
	if noMedia {
		return objects, missing, nil
	}
	for _, tweet := range tweets {
		media, err := getxapi.DownloadMedia(ctx, mediaHTTP(), tweet)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, missing, err
			}
			if reporter == nil {
				return nil, missing, err
			}
			missing++
			if err := appendSyncReport(reporter, reportRecord{ItemID: tweet.ID, Result: "media_missing", At: time.Now().UTC().Format(time.RFC3339Nano), Error: err.Error()}); err != nil {
				return nil, missing, err
			}
			continue
		}
		objects[tweet.ID] = media
	}
	return objects, missing, nil
}

func writeSyncPage(output string, account getxapi.Account, tweets []getxapi.Tweet, objects map[string][]getxapi.Media) (string, bool, error) {
	directory := filepath.Join(output, account.UserID)
	name := "page-" + tweets[0].ID + "-" + tweets[len(tweets)-1].ID + ".zip"
	path := filepath.Join(directory, name)
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return "", false, errors.New("existing sync archive is not a regular file")
		}
		relative, err := filepath.Rel(output, path)
		return relative, true, err
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", false, err
	}
	if err := getxapi.WriteBundle(path, account, tweets, objects); err != nil {
		return "", false, err
	}
	relative, err := filepath.Rel(output, path)
	return relative, false, err
}

func syncArchivePath(output, relative string) (string, error) {
	clean := filepath.Clean(relative)
	if relative == "" || filepath.IsAbs(relative) || clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("invalid archive path in sync state")
	}
	return filepath.Join(output, clean), nil
}

func writeReplayManifest(output string, accounts []getxapi.Account) error {
	if err := os.MkdirAll(filepath.Join(output, "replay-state"), 0700); err != nil {
		return err
	}
	manifest := batchManifest{Version: 1}
	for _, account := range accounts {
		directory := filepath.Join(output, account.UserID)
		entries, err := os.ReadDir(directory)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !entry.Type().IsRegular() || !strings.HasPrefix(entry.Name(), "page-") || !strings.HasSuffix(entry.Name(), ".zip") {
				continue
			}
			relative := filepath.Join(account.UserID, entry.Name())
			reader, err := archive.Open(filepath.Join(output, relative))
			if err != nil {
				return fmt.Errorf("open replay archive %s: %w", relative, err)
			}
			accountID := reader.AccountID()
			closeErr := reader.Close()
			if closeErr != nil {
				return closeErr
			}
			if accountID != account.UserID {
				return fmt.Errorf("replay archive %s belongs to Twitter user %s, want %s", relative, accountID, account.UserID)
			}
			manifest.Imports = append(manifest.Imports, batchImport{
				SourceType: "twitter-import-v1",
				Archive:    relative, TargetFeed: account.FeedUUID,
				State: filepath.Join("replay-state", account.UserID+".db"), Report: "production-replay.jsonl",
				BoundaryTweetID: account.BoundaryTweetID,
				BoundaryAt:      formatOptionalTime(account.BoundaryAt),
			})
		}
	}
	if len(manifest.Imports) == 0 {
		if err := os.Remove(filepath.Join(output, "manifest.json")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	return writePrivateJSON(filepath.Join(output, "manifest.json"), manifest)
}

func formatOptionalTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func processSyncArchive(ctx context.Context, path string, skip int, api *client.Client, account getxapi.Account, reporter *bufio.Writer, full bool, remaining int, counts *syncCounts, progress func(int, bool) error) (processed int, complete, replay bool, err error) {
	reader, err := archive.Open(path)
	if err != nil {
		return 0, false, false, err
	}
	defer reader.Close()
	index := 0
	_, err = reader.Iterate(func(tweet archive.Tweet) error {
		if index < skip {
			index++
			return nil
		}
		if !full && processed >= remaining {
			return stopAtLimit
		}
		index++
		processed++
		if !full && (tweet.ID == account.BoundaryTweetID || (!account.BoundaryAt.IsZero() && !tweet.CreatedAt.After(account.BoundaryAt))) {
			counts.Replayed++
			if err := appendSyncReport(reporter, reportRecord{ItemID: tweet.ID, Result: "boundary_reached", At: time.Now().UTC().Format(time.RFC3339Nano)}); err != nil {
				return err
			}
			if err := progress(index, false); err != nil {
				return err
			}
			return stopAtReplay
		}
		if tweet.ReplyTo != "" {
			counts.Replies++
			return progress(index, false)
		}
		result, importErr := api.Import(ctx, archiveMetadata(tweet, reader.AccountID()), tweet.Media)
		if importErr != nil {
			if !client.IsPermanent(importErr) {
				return importErr
			}
			counts.Rejected++
			if err := appendSyncReport(reporter, reportRecord{ItemID: tweet.ID, Result: "rejected", At: time.Now().UTC().Format(time.RFC3339Nano), Error: importErr.Error()}); err != nil {
				return err
			}
			return progress(index, false)
		}
		if result.Created {
			counts.Created++
			return progress(index, true)
		}
		counts.Replayed++
		if err := progress(index, full); err != nil {
			return err
		}
		if !full && account.BoundaryTweetID == "" && account.BoundaryAt.IsZero() {
			return stopAtReplay
		}
		return nil
	})
	switch {
	case errors.Is(err, stopAtReplay):
		return processed, false, true, nil
	case errors.Is(err, stopAtLimit):
		return processed, false, false, nil
	case err != nil:
		return processed, false, false, err
	default:
		return processed, true, false, nil
	}
}

func archiveMetadata(tweet archive.Tweet, accountID string) client.ImportMetadata {
	metadata := client.ImportMetadata{PublishedAt: tweet.CreatedAt.Format(time.RFC3339Nano), BodyHTML: strings.ReplaceAll(html.EscapeString(tweet.Text), "\n", "<br>")}
	metadata.Source.Kind, metadata.Source.AccountID = "twitter", tweet.AccountID
	if metadata.Source.AccountID == "" {
		metadata.Source.AccountID = accountID
	}
	metadata.Source.ItemID, metadata.Source.URL = tweet.ID, tweet.SourceURL
	if metadata.Source.URL == "" {
		metadata.Source.URL = "https://x.com/i/status/" + tweet.ID
	}
	return metadata
}

func openPrivateAppend(path string) (*os.File, error) {
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return nil, errors.New("sync report must be a regular mode-0600 file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
}

func appendSyncReport(writer *bufio.Writer, record reportRecord) error {
	if writer == nil {
		return nil
	}
	return appendReport(writer, record)
}

func loadSyncState(path string) (syncState, error) {
	state := syncState{Version: 1, Positions: make(map[string][]syncPosition)}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return state, err
	}
	if state.Version != 1 || state.Positions == nil {
		return state, errors.New("invalid sync state")
	}
	return state, nil
}

func saveSyncState(path string, state syncState) error { return writePrivateJSON(path, state) }

func writePrivateJSON(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(append(raw, '\n')); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
