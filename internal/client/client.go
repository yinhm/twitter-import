package client

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	maxFiles      = 10
	maxFileBytes  = 20 << 20
	maxTotalBytes = 100 << 20
)

type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }
func IsPermanent(err error) bool        { var target *PermanentError; return errors.As(err, &target) }

func permanent(message string) error { return &PermanentError{Err: errors.New(message)} }

// FatalError means the Feed capability or its target is no longer usable.
// Unlike a content-level PermanentError, it must abort the whole run without
// checkpointing the current item: rotating the key and retrying must remain
// sufficient to resume the archive.
type FatalError struct{ Err error }

func (e *FatalError) Error() string { return e.Err.Error() }
func (e *FatalError) Unwrap() error { return e.Err }
func IsFatal(err error) bool        { var target *FatalError; return errors.As(err, &target) }

type Client struct {
	Endpoint string
	Key      string
	Target   string
	HTTP     *http.Client
}

type Entry struct {
	ID        string `json:"id"`
	SourceURL string `json:"source_url"`
}

type Feed struct {
	UUID string `json:"uuid"`
}

func (c *Client) GetFeed(ctx context.Context) (Feed, error) {
	request, _ := http.NewRequest(http.MethodGet, strings.TrimRight(c.Endpoint, "/")+"/api/v1/feed", nil)
	response, err := c.request(ctx, request)
	if err != nil {
		return Feed{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return Feed{}, fmt.Errorf("get Feed: HTTP %d", response.StatusCode)
	}
	var envelope struct {
		Data Feed `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&envelope); err != nil {
		return Feed{}, err
	}
	if envelope.Data.UUID == "" {
		return Feed{}, errors.New("Feed response has no UUID")
	}
	return envelope.Data, nil
}

type listResponse struct {
	Data       []Entry `json:"data"`
	Pagination struct {
		NextCursor string `json:"next_cursor"`
	} `json:"pagination"`
}

type ImportResult struct {
	Created bool `json:"created"`
	Data    struct {
		ID string `json:"id"`
	} `json:"data"`
}

type Upload struct {
	Name string
	Size int64
	Open func() (io.ReadCloser, error)
}

type ImportMetadata struct {
	Source struct {
		Kind      string `json:"kind"`
		AccountID string `json:"account_id"`
		ItemID    string `json:"item_id"`
		URL       string `json:"url"`
	} `json:"source"`
	PublishedAt string `json:"published_at"`
	Title       string `json:"title"`
	BodyHTML    string `json:"body_html"`
}

func (c *Client) request(ctx context.Context, request *http.Request) (*http.Response, error) {
	request = request.WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+c.Key)
	return c.HTTP.Do(request)
}

func (c *Client) ListEntries(ctx context.Context, fn func(Entry) error) error {
	cursor := ""
	for {
		endpoint := strings.TrimRight(c.Endpoint, "/") + "/api/v1/feed/entries?limit=100"
		if cursor != "" {
			endpoint += "&cursor=" + url.QueryEscape(cursor)
		}
		request, _ := http.NewRequest(http.MethodGet, endpoint, nil)
		response, err := c.request(ctx, request)
		if err != nil {
			return err
		}
		if response.StatusCode != http.StatusOK {
			response.Body.Close()
			return fmt.Errorf("list entries: HTTP %d", response.StatusCode)
		}
		var result listResponse
		err = json.NewDecoder(io.LimitReader(response.Body, 8<<20)).Decode(&result)
		response.Body.Close()
		if err != nil {
			return err
		}
		for _, entry := range result.Data {
			if err := fn(entry); err != nil {
				return err
			}
		}
		if result.Pagination.NextCursor == "" {
			return nil
		}
		cursor = result.Pagination.NextCursor
	}
}

func (c *Client) Import(ctx context.Context, metadata ImportMetadata, files []*zip.File) (ImportResult, error) {
	uploads := make([]Upload, 0, len(files))
	for _, file := range files {
		if file == nil {
			uploads = append(uploads, Upload{})
			continue
		}
		uploads = append(uploads, Upload{Name: file.Name, Size: int64(file.UncompressedSize64), Open: file.Open})
	}
	return c.ImportUploads(ctx, metadata, uploads)
}

func (c *Client) ImportUploads(ctx context.Context, metadata ImportMetadata, files []Upload) (ImportResult, error) {
	var last error
	for attempt := 0; attempt < 6; attempt++ {
		result, retryAfter, err := c.importOnce(ctx, metadata, files)
		if err == nil {
			return result, nil
		}
		last = err
		if retryAfter < 0 {
			return ImportResult{}, err
		}
		if retryAfter == 0 {
			retryAfter = time.Second << attempt
			if retryAfter > 30*time.Second {
				retryAfter = 30 * time.Second
			}
		}
		// Positive jitter avoids synchronized retries while never violating a
		// server-provided minimum Retry-After.
		retryAfter += time.Duration(rand.Int64N(max(1, int64(retryAfter/4))))
		timer := time.NewTimer(retryAfter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ImportResult{}, ctx.Err()
		case <-timer.C:
		}
	}
	return ImportResult{}, last
}

func (c *Client) importOnce(ctx context.Context, metadata ImportMetadata, files []Upload) (ImportResult, time.Duration, error) {
	if len(files) > maxFiles {
		return ImportResult{}, -1, permanent("archive item exceeds file count limit")
	}
	var declared uint64
	for _, file := range files {
		if file.Open == nil || file.Size < 0 || file.Size > maxFileBytes {
			return ImportResult{}, -1, permanent("archive media exceeds file size limit")
		}
		declared += uint64(file.Size)
		if declared > maxTotalBytes {
			return ImportResult{}, -1, permanent("archive item exceeds total media limit")
		}
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	raw, err := json.Marshal(metadata)
	if err != nil {
		return ImportResult{}, -1, err
	}
	if err := writer.WriteField("metadata", string(raw)); err != nil {
		return ImportResult{}, -1, err
	}
	for _, file := range files {
		source, err := file.Open()
		if err != nil {
			return ImportResult{}, -1, err
		}
		part, err := writer.CreateFormFile("file", filepath.Base(file.Name))
		if err == nil {
			var copied int64
			copied, err = io.Copy(part, io.LimitReader(source, maxFileBytes+1))
			if err == nil && copied > maxFileBytes {
				err = permanent("archive media exceeds file size limit")
			}
		}
		closeErr := source.Close()
		if err != nil {
			return ImportResult{}, -1, err
		}
		if closeErr != nil {
			return ImportResult{}, -1, closeErr
		}
	}
	if err := writer.Close(); err != nil {
		return ImportResult{}, -1, err
	}
	request, _ := http.NewRequest(http.MethodPost, strings.TrimRight(c.Endpoint, "/")+"/api/v1/feed/imports", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	if c.Target != "" {
		request.Header.Set("X-FF-Import-Target", c.Target)
	}
	response, err := c.request(ctx, request)
	if err != nil {
		return ImportResult{}, 0, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusOK || response.StatusCode == http.StatusCreated {
		var result ImportResult
		if err := json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&result); err != nil {
			return ImportResult{}, -1, err
		}
		return result, -1, nil
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden || response.StatusCode == http.StatusNotFound {
		return ImportResult{}, -1, &FatalError{Err: fmt.Errorf("import item %s: Feed capability unavailable (HTTP %d)", metadata.Source.ItemID, response.StatusCode)}
	}
	if response.StatusCode != http.StatusTooManyRequests && response.StatusCode < http.StatusInternalServerError {
		return ImportResult{}, -1, &PermanentError{Err: fmt.Errorf("import item %s: HTTP %d", metadata.Source.ItemID, response.StatusCode)}
	}
	retry := time.Duration(0)
	if raw := response.Header.Get("Retry-After"); raw != "" {
		if seconds, parseErr := strconv.Atoi(raw); parseErr == nil && seconds >= 0 {
			retry = time.Duration(seconds) * time.Second
		}
	}
	return ImportResult{}, retry, errors.New("temporary import failure")
}
