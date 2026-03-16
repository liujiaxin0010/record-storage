package storage

import (
	"bytes"
	"encoding/json"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"
)

type FilerHTTPStorage struct {
	filerURL string
	client   *http.Client
}

func NewFilerHTTPStorage(filerURL string) *FilerHTTPStorage {
	return &FilerHTTPStorage{
		filerURL: strings.TrimSuffix(filerURL, "/"),
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (s *FilerHTTPStorage) Put(ctx context.Context, key string, data []byte) error {
	url := fmt.Sprintf("%s/%s", s.filerURL, strings.TrimPrefix(key, "/"))
	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("put failed: %d %s", resp.StatusCode, string(body))
	}
	return nil
}

func (s *FilerHTTPStorage) Get(ctx context.Context, key string) ([]byte, error) {
	url := fmt.Sprintf("%s/%s", s.filerURL, strings.TrimPrefix(key, "/"))
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get failed: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func (s *FilerHTTPStorage) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	url := fmt.Sprintf("%s/%s", s.filerURL, strings.TrimPrefix(key, "/"))
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("get range failed: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func (s *FilerHTTPStorage) Delete(ctx context.Context, key string) error {
	url := fmt.Sprintf("%s/%s", s.filerURL, strings.TrimPrefix(key, "/"))
	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("delete failed: %d", resp.StatusCode)
	}
	return nil
}

func (s *FilerHTTPStorage) List(ctx context.Context, prefix string) ([]string, error) {
	dir := strings.TrimSuffix(prefix, "/")
	if dir == "" {
		dir = "/"
	}
	url := fmt.Sprintf("%s/%s?limit=1000", s.filerURL, strings.TrimPrefix(dir, "/"))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list failed: %d %s", resp.StatusCode, string(body))
	}

	var payload struct {
		Entries []struct {
			Name string `json:"name"`
		} `json:"Entries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	items := make([]string, 0, len(payload.Entries))
	for _, entry := range payload.Entries {
		items = append(items, path.Join(strings.TrimPrefix(dir, "/"), entry.Name))
	}
	return items, nil
}

func (s *FilerHTTPStorage) Close() error {
	return nil
}
