package storage

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound                 = errors.New("object not found")
	ErrInvalidFID               = errors.New("invalid fid")
	ErrEntryPendingCompensation = errors.New("entry pending compensation")
	ErrStorageDegraded          = errors.New("storage degraded to filer_http")
	ErrVolumeRouteNotFound      = errors.New("volume route not found")
)

type StorageBackend interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
	Close() error
}

type AssignParams struct {
	Count       uint64
	Collection  string
	Replication string
	TTL         string
	DataCenter  string
	Rack        string
	DataNode    string
	DiskType    string
}

type AssignResult struct {
	FID       string
	VolumeID  string
	VolumeURL string
	AuthToken string
}

type VolumeLocation struct {
	URL       string
	PublicURL string
	AuthToken string
}

type EntryMeta struct {
	Key        string
	FileID     string
	VolumeID   string
	Size       int64
	MimeType   string
	CreatedAt  int64
	ModifiedAt int64
	ETag       string
}

type MasterClient interface {
	Assign(ctx context.Context, p AssignParams) (AssignResult, error)
	LookupVolume(ctx context.Context, volumeOrFileIDs []string) (map[string][]VolumeLocation, error)
	Ping(ctx context.Context) error
	Close() error
}

type FilerClient interface {
	CreateEntry(ctx context.Context, meta EntryMeta) error
	LookupEntry(ctx context.Context, key string) (EntryMeta, error)
	DeleteEntry(ctx context.Context, key string, recursive bool) error
	ListEntries(ctx context.Context, prefix string, limit int, startFrom string) ([]EntryMeta, bool, error)
	Ping(ctx context.Context) error
	Close() error
}

type VolumeClient interface {
	Put(ctx context.Context, volumeURL, fileID string, data []byte, authToken string) error
	Get(ctx context.Context, volumeURL, fileID string) ([]byte, error)
	GetRange(ctx context.Context, volumeURL, fileID string, offset, length int64) ([]byte, error)
}

type cacheEntry[T any] struct {
	Value     T
	ExpiresAt time.Time
}
