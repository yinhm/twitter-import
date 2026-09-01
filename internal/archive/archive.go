package archive

import (
	"archive/zip"
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"
)

const twitterTime = "Mon Jan 02 15:04:05 -0700 2006"

type Tweet struct {
	ID        string
	AccountID string
	Text      string
	CreatedAt time.Time
	ReplyTo   string
	RetweetOf string
	QuoteOf   string
	Media     []*zip.File
	SourceURL string
}

type rawTweet struct {
	ID        string `json:"id_str"`
	Text      string `json:"full_text"`
	CreatedAt string `json:"created_at"`
	ReplyTo   string `json:"in_reply_to_status_id_str"`
	RetweetOf string `json:"retweeted_status_id_str"`
	QuoteOf   string `json:"quoted_status_id_str"`
}

type tweetEnvelope struct {
	Tweet rawTweet `json:"tweet"`
}

type Reader struct {
	zip       *zip.ReadCloser
	parts     []*zip.File
	media     map[string][]*zip.File
	accountID string
	scopeID   string
	items     *zip.File
	files     map[string]*zip.File
}

func Open(path string) (*Reader, error) {
	zr, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}
	reader := &Reader{zip: zr, media: make(map[string][]*zip.File), files: make(map[string]*zip.File)}
	for _, file := range zr.File {
		name := strings.ReplaceAll(file.Name, "\\", "/")
		reader.files[name] = file
		base := name[strings.LastIndex(name, "/")+1:]
		switch {
		case name == "source.json":
			if err := reader.readBundleSource(file); err != nil {
				zr.Close()
				return nil, err
			}
		case name == "items.jsonl":
			reader.items = file
		case strings.HasSuffix(name, "/account.js") || name == "account.js" || strings.HasSuffix(name, "/account-part0.js"):
			reader.accountID, _ = readAccountID(file)
		case strings.Contains(name, "/tweets") && strings.HasSuffix(name, ".js"):
			reader.parts = append(reader.parts, file)
		case strings.Contains(name, "tweets_media/"):
			if cut := strings.IndexByte(base, '-'); cut > 0 {
				reader.media[base[:cut]] = append(reader.media[base[:cut]], file)
			}
		}
	}
	if reader.items != nil {
		if reader.scopeID == "" {
			zr.Close()
			return nil, errors.New("twitter-import bundle has no source scope")
		}
		return reader, nil
	}
	sort.Slice(reader.parts, func(i, j int) bool { return reader.parts[i].Name < reader.parts[j].Name })
	if len(reader.parts) == 0 {
		zr.Close()
		return nil, errors.New("archive contains no tweet parts")
	}
	return reader, nil
}

func readAccountID(file *zip.File) (string, error) {
	decoder, source, err := jsonArray(file)
	if err != nil {
		return "", err
	}
	defer source.Close()
	if !decoder.More() {
		return "", errors.New("empty account array")
	}
	var envelope struct {
		Account struct {
			ID string `json:"accountId"`
		} `json:"account"`
	}
	if err := decoder.Decode(&envelope); err != nil {
		return "", err
	}
	return envelope.Account.ID, nil
}

func (r *Reader) AccountID() string { return r.accountID }

func (r *Reader) ScopeID() string {
	if r.scopeID != "" {
		return r.scopeID
	}
	return r.accountID
}

func (r *Reader) Close() error { return r.zip.Close() }

func jsonArray(file *zip.File) (*json.Decoder, io.Closer, error) {
	source, err := file.Open()
	if err != nil {
		return nil, nil, err
	}
	buffered := bufio.NewReader(source)
	for {
		b, err := buffered.ReadByte()
		if err != nil {
			source.Close()
			return nil, nil, fmt.Errorf("find JSON array in %s: %w", file.Name, err)
		}
		if b == '[' {
			break
		}
	}
	decoder := json.NewDecoder(io.MultiReader(strings.NewReader("["), buffered))
	if token, err := decoder.Token(); err != nil || token != json.Delim('[') {
		source.Close()
		return nil, nil, fmt.Errorf("invalid tweet array in %s", file.Name)
	}
	return decoder, source, nil
}

func (r *Reader) Iterate(fn func(Tweet) error) (int, error) {
	if r.items != nil {
		return r.iterateBundle(fn)
	}
	invalid := 0
	for _, part := range r.parts {
		decoder, source, err := jsonArray(part)
		if err != nil {
			return invalid, err
		}
		for decoder.More() {
			var envelope tweetEnvelope
			if err := decoder.Decode(&envelope); err != nil {
				source.Close()
				return invalid, fmt.Errorf("decode %s: %w", part.Name, err)
			}
			created, err := time.Parse(twitterTime, envelope.Tweet.CreatedAt)
			if err != nil || envelope.Tweet.ID == "" {
				invalid++
				continue
			}
			if err := fn(Tweet{ID: envelope.Tweet.ID, Text: envelope.Tweet.Text, CreatedAt: created.UTC(), ReplyTo: envelope.Tweet.ReplyTo, RetweetOf: envelope.Tweet.RetweetOf, QuoteOf: envelope.Tweet.QuoteOf, Media: r.media[envelope.Tweet.ID]}); err != nil {
				source.Close()
				return invalid, err
			}
		}
		if err := source.Close(); err != nil {
			return invalid, err
		}
	}
	return invalid, nil
}

type bundleSource struct {
	Version     int    `json:"version"`
	Format      string `json:"format"`
	Collector   string `json:"collector"`
	CollectedAt string `json:"collected_at"`
	AccountID   string `json:"account_id"`
	Collection  struct {
		Kind string `json:"kind"`
		ID   string `json:"id"`
	} `json:"collection"`
}

func (r *Reader) readBundleSource(file *zip.File) error {
	source, err := file.Open()
	if err != nil {
		return err
	}
	defer source.Close()
	var metadata bundleSource
	decoder := json.NewDecoder(io.LimitReader(source, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&metadata); err != nil {
		return fmt.Errorf("decode source.json: %w", err)
	}
	collectedAt, timeErr := time.Parse(time.RFC3339Nano, metadata.CollectedAt)
	if metadata.Version != 1 || metadata.Format != "twitter-import/v1" || metadata.Collector == "" || timeErr != nil || collectedAt.IsZero() ||
		(metadata.Collection.Kind != "user" && metadata.Collection.Kind != "list") || !decimal(metadata.Collection.ID) ||
		(metadata.Collection.Kind == "user" && metadata.AccountID != metadata.Collection.ID) {
		return errors.New("invalid twitter-import/v1 source metadata")
	}
	r.accountID = metadata.AccountID
	r.scopeID = metadata.Collection.Kind + ":" + metadata.Collection.ID
	return nil
}

type bundleItem struct {
	ID        string   `json:"id"`
	AccountID string   `json:"account_id"`
	CreatedAt string   `json:"created_at"`
	Text      string   `json:"text"`
	ReplyTo   string   `json:"reply_to"`
	RetweetOf string   `json:"retweet_of"`
	QuoteOf   string   `json:"quote_of"`
	URL       string   `json:"url"`
	Media     []string `json:"media"`
}

func (r *Reader) iterateBundle(fn func(Tweet) error) (int, error) {
	source, err := r.items.Open()
	if err != nil {
		return 0, err
	}
	defer source.Close()
	invalid := 0
	scanner := bufio.NewScanner(source)
	scanner.Buffer(make([]byte, 64<<10), 2<<20)
	for scanner.Scan() {
		var item bundleItem
		decoder := json.NewDecoder(strings.NewReader(scanner.Text()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&item); err != nil {
			invalid++
			continue
		}
		created, err := time.Parse(time.RFC3339Nano, item.CreatedAt)
		if err != nil || !decimal(item.ID) || !decimal(item.AccountID) || item.URL == "" {
			invalid++
			continue
		}
		media := make([]*zip.File, 0, len(item.Media))
		valid := true
		for _, name := range item.Media {
			clean := strings.ReplaceAll(name, "\\", "/")
			if path.Clean(clean) != clean || !strings.HasPrefix(clean, "media/") || r.files[clean] == nil {
				valid = false
				break
			}
			media = append(media, r.files[clean])
		}
		if !valid {
			invalid++
			continue
		}
		if err := fn(Tweet{
			ID: item.ID, AccountID: item.AccountID, Text: item.Text, CreatedAt: created.UTC(),
			ReplyTo: item.ReplyTo, RetweetOf: item.RetweetOf, QuoteOf: item.QuoteOf,
			Media: media, SourceURL: item.URL,
		}); err != nil {
			return invalid, err
		}
	}
	return invalid, scanner.Err()
}

func decimal(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}
