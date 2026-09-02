package getxapi

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	DefaultEndpoint = "https://api.getxapi.com"
	MaxMediaFile    = 20 << 20
	MaxMediaTotal   = 100 << 20
	MaxMediaCount   = 10
	twitterTime     = "Mon Jan 02 15:04:05 -0700 2006"
)

type Account struct {
	FeedID, FeedUUID, Username, UserID, BoundaryTweetID string
	BoundaryAt                                          time.Time
}

type Media struct {
	Name string
	Data []byte
}

type RemoteMedia struct {
	Type     string `json:"type"`
	URL      string `json:"url"`
	VideoURL string `json:"video_url"`
}

type Tweet struct {
	ID, AccountID, CreatedAt, Text, ReplyTo, RetweetOf, QuoteOf, URL string
	Media                                                            []RemoteMedia
}

type Page struct {
	Tweets     []Tweet
	NextCursor string
	HasMore    bool
}

type Client struct {
	Endpoint string
	Key      string
	HTTP     *http.Client
}

type HTTPError struct {
	Operation  string
	StatusCode int
}

func (e *HTTPError) Error() string { return fmt.Sprintf("%s: HTTP %d", e.Operation, e.StatusCode) }

func IsAccountUnavailable(err error) bool {
	var httpErr *HTTPError
	return errors.As(err, &httpErr) && (httpErr.StatusCode == http.StatusUnauthorized || httpErr.StatusCode == http.StatusForbidden || httpErr.StatusCode == http.StatusNotFound)
}

type apiTweet struct {
	ID          string        `json:"id"`
	URL         string        `json:"url"`
	Text        string        `json:"text"`
	CreatedAt   string        `json:"createdAt"`
	IsReply     bool          `json:"isReply"`
	InReplyToID string        `json:"inReplyToId"`
	Media       []RemoteMedia `json:"media"`
	Author      struct {
		ID string `json:"id"`
	} `json:"author"`
	Entities struct {
		URLs []struct {
			URL      string `json:"url"`
			Expanded string `json:"expanded_url"`
		} `json:"urls"`
	} `json:"entities"`
	QuotedTweet *struct {
		ID string `json:"id"`
	} `json:"quoted_tweet"`
	RetweetedTweet *struct {
		ID string `json:"id"`
	} `json:"retweeted_tweet"`
}

func ReadAccounts(path string) ([]Account, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.Comma = '\t'
	rows, err := reader.ReadAll()
	if err != nil || len(rows) < 2 {
		return nil, errors.New("accounts TSV requires a header and at least one account")
	}
	columns := make(map[string]int)
	for index, name := range rows[0] {
		columns[strings.TrimSpace(name)] = index
	}
	required := []string{"feed_id", "feed_uuid", "twitter_username", "twitter_user_id"}
	for _, name := range required {
		if _, ok := columns[name]; !ok {
			return nil, fmt.Errorf("accounts TSV is missing %s", name)
		}
	}
	var accounts []Account
	seen := make(map[string]bool)
	for line, row := range rows[1:] {
		value := func(name string) string {
			index, ok := columns[name]
			if !ok {
				return ""
			}
			if index >= len(row) {
				return ""
			}
			return strings.TrimSpace(row[index])
		}
		account := Account{FeedID: value("feed_id"), FeedUUID: value("feed_uuid"), Username: strings.TrimPrefix(value("twitter_username"), "@"), UserID: value("twitter_user_id"), BoundaryTweetID: value("boundary_tweet_id")}
		if !uuid(account.FeedUUID) || account.Username == "" || !decimal(account.UserID) {
			return nil, fmt.Errorf("invalid account at TSV line %d", line+2)
		}
		if raw := value("boundary_at"); raw != "" {
			account.BoundaryAt, err = time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				return nil, fmt.Errorf("invalid boundary_at at TSV line %d: %w", line+2, err)
			}
		}
		if account.BoundaryTweetID != "" && !decimal(account.BoundaryTweetID) {
			return nil, fmt.Errorf("invalid boundary_tweet_id at TSV line %d", line+2)
		}
		key := account.FeedUUID + "\x00" + account.UserID
		if !seen[key] {
			seen[key] = true
			accounts = append(accounts, account)
		}
	}
	return accounts, nil
}

func (c *Client) UserTweets(ctx context.Context, userID, cursor string) (Page, error) {
	if !decimal(userID) {
		return Page{}, errors.New("Twitter user ID must be decimal")
	}
	endpoint := strings.TrimRight(c.Endpoint, "/") + "/twitter/user/tweets?userId=" + url.QueryEscape(userID)
	if cursor != "" {
		endpoint += "&cursor=" + url.QueryEscape(cursor)
	}
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	request.Header.Set("Authorization", "Bearer "+c.Key)
	response, err := c.HTTP.Do(request)
	if err != nil {
		return Page{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return Page{}, &HTTPError{Operation: "GetXAPI user tweets", StatusCode: response.StatusCode}
	}
	var raw struct {
		UserID     string     `json:"userId"`
		Tweets     []apiTweet `json:"tweets"`
		HasMore    bool       `json:"has_more"`
		NextCursor string     `json:"next_cursor"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 16<<20)).Decode(&raw); err != nil {
		return Page{}, err
	}
	if raw.UserID != "" && raw.UserID != userID {
		return Page{}, fmt.Errorf("GetXAPI returned Twitter user %s, expected %s", raw.UserID, userID)
	}
	page := Page{NextCursor: raw.NextCursor, HasMore: raw.HasMore}
	seen := make(map[string]bool)
	for _, item := range raw.Tweets {
		created, err := parseTime(item.CreatedAt)
		if err != nil || !decimal(item.ID) || item.Author.ID != userID || item.URL == "" {
			return Page{}, fmt.Errorf("GetXAPI returned invalid tweet for user %s", userID)
		}
		if seen[item.ID] {
			continue
		}
		seen[item.ID] = true
		for _, entity := range item.Entities.URLs {
			if entity.URL != "" && entity.Expanded != "" {
				item.Text = strings.ReplaceAll(item.Text, entity.URL, entity.Expanded)
			}
		}
		tweet := Tweet{ID: item.ID, AccountID: userID, CreatedAt: created.UTC().Format(time.RFC3339Nano), Text: item.Text, URL: item.URL, Media: item.Media}
		if item.IsReply {
			tweet.ReplyTo = item.InReplyToID
		}
		if item.QuotedTweet != nil {
			tweet.QuoteOf = item.QuotedTweet.ID
		}
		if item.RetweetedTweet != nil {
			tweet.RetweetOf = item.RetweetedTweet.ID
		}
		page.Tweets = append(page.Tweets, tweet)
	}
	if page.HasMore && page.NextCursor == "" {
		return Page{}, errors.New("GetXAPI response has_more without next_cursor")
	}
	return page, nil
}

func DownloadMedia(ctx context.Context, httpClient *http.Client, tweet Tweet) ([]Media, error) {
	if len(tweet.Media) > MaxMediaCount {
		return nil, errors.New("tweet exceeds media count limit")
	}
	result := make([]Media, 0, len(tweet.Media))
	total := 0
	for index, remote := range tweet.Media {
		raw := remote.URL
		if remote.VideoURL != "" {
			raw = remote.VideoURL
		}
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Scheme != "https" || !slices.Contains([]string{"pbs.twimg.com", "video.twimg.com"}, strings.ToLower(parsed.Hostname())) {
			return nil, errors.New("GetXAPI returned unsupported media URL")
		}
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
		response, err := httpClient.Do(request)
		if err != nil {
			return nil, err
		}
		if response.StatusCode != http.StatusOK {
			response.Body.Close()
			return nil, fmt.Errorf("download Twitter media: HTTP %d", response.StatusCode)
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, MaxMediaFile+1))
		response.Body.Close()
		if err != nil || len(data) > MaxMediaFile {
			return nil, errors.New("Twitter media exceeds file size limit")
		}
		total += len(data)
		if total > MaxMediaTotal {
			return nil, errors.New("tweet media exceeds total size limit")
		}
		extension := strings.ToLower(filepath.Ext(parsed.Path))
		if extension == "" {
			if extensions, _ := mime.ExtensionsByType(response.Header.Get("Content-Type")); len(extensions) > 0 {
				extension = extensions[0]
			}
		}
		if len(extension) > 6 || strings.ContainsAny(extension, "/\\") {
			extension = ""
		}
		result = append(result, Media{Name: fmt.Sprintf("%s-%d%s", tweet.ID, index, extension), Data: data})
	}
	return result, nil
}

func WriteBundle(path string, account Account, tweets []Tweet, media map[string][]Media) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
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
	writer := zip.NewWriter(temporary)
	source := map[string]any{"version": 1, "format": "twitter-import/v1", "collector": "getxapi", "collected_at": time.Now().UTC().Format(time.RFC3339Nano), "account_id": account.UserID, "collection": map[string]string{"kind": "user", "id": account.UserID}}
	if err := writeJSONFile(writer, "source.json", source); err != nil {
		writer.Close()
		temporary.Close()
		return err
	}
	mediaNames := make(map[string][]string)
	for _, tweet := range tweets {
		names := make([]string, 0, len(media[tweet.ID]))
		for _, object := range media[tweet.ID] {
			fileName := "media/" + object.Name
			part, createErr := writer.Create(fileName)
			if createErr != nil {
				writer.Close()
				temporary.Close()
				return createErr
			}
			if _, err := io.Copy(part, bytes.NewReader(object.Data)); err != nil {
				writer.Close()
				temporary.Close()
				return err
			}
			names = append(names, fileName)
		}
		mediaNames[tweet.ID] = names
	}
	items, err := writer.Create("items.jsonl")
	if err != nil {
		writer.Close()
		temporary.Close()
		return err
	}
	buffer := bufio.NewWriter(items)
	for _, tweet := range tweets {
		row := map[string]any{"id": tweet.ID, "account_id": tweet.AccountID, "created_at": tweet.CreatedAt, "text": tweet.Text, "reply_to": tweet.ReplyTo, "retweet_of": tweet.RetweetOf, "quote_of": tweet.QuoteOf, "url": tweet.URL, "media": mediaNames[tweet.ID]}
		raw, _ := json.Marshal(row)
		if _, err := buffer.Write(append(raw, '\n')); err != nil {
			writer.Close()
			temporary.Close()
			return err
		}
	}
	if err := buffer.Flush(); err != nil {
		writer.Close()
		temporary.Close()
		return err
	}
	if err := writer.Close(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func writeJSONFile(writer *zip.Writer, name string, value any) error {
	part, err := writer.Create(name)
	if err != nil {
		return err
	}
	return json.NewEncoder(part).Encode(value)
}

func decimal(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func parseTime(value string) (time.Time, error) {
	if parsed, err := time.Parse(twitterTime, value); err == nil {
		return parsed, nil
	}
	return time.Parse(time.RFC3339Nano, value)
}

func uuid(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for index, character := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f') || (character >= 'A' && character <= 'F')) {
			return false
		}
	}
	return true
}
