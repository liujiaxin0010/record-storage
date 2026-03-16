package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type HTTPVolumeClient struct {
	client *http.Client
}

func NewHTTPVolumeClient() *HTTPVolumeClient {
	return &HTTPVolumeClient{
		client: &http.Client{Timeout: 2 * time.Second},
	}
}

func (c *HTTPVolumeClient) Put(ctx context.Context, volumeURL, fileID string, data []byte, authToken string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, buildVolumeObjectURL(volumeURL, fileID), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if authToken != "" {
		req.Header.Set("Authorization", "Bearer "+authToken)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("volume put failed: %d %s", resp.StatusCode, string(body))
	}
	return nil
}

func (c *HTTPVolumeClient) Get(ctx context.Context, volumeURL, fileID string) ([]byte, error) {
	return c.doGet(ctx, volumeURL, fileID, "")
}

func (c *HTTPVolumeClient) GetRange(ctx context.Context, volumeURL, fileID string, offset, length int64) ([]byte, error) {
	return c.doGet(ctx, volumeURL, fileID, fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
}

func (c *HTTPVolumeClient) doGet(ctx context.Context, volumeURL, fileID, rangeHeader string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, buildVolumeObjectURL(volumeURL, fileID), nil)
	if err != nil {
		return nil, err
	}
	if rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("volume get failed: %d %s", resp.StatusCode, string(body))
	}
	return io.ReadAll(resp.Body)
}

func buildVolumeObjectURL(volumeURL, fileID string) string {
	return strings.TrimSuffix(volumeURL, "/") + "/" + strings.TrimPrefix(fileID, "/")
}
