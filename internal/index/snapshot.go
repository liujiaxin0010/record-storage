package index

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// StorageWriter is a minimal interface for snapshot backup/restore to remote storage.
type StorageWriter interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
}

// SnapshotManager periodically creates BoltDB snapshots and uploads them to storage.
type SnapshotManager struct {
	db       *bolt.DB
	storage  StorageWriter
	interval time.Duration
	key      string

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewSnapshotManager(db *bolt.DB, storage StorageWriter, intervalSeconds int) *SnapshotManager {
	ctx, cancel := context.WithCancel(context.Background())
	interval := time.Duration(intervalSeconds) * time.Second
	if interval < 60*time.Second {
		interval = 300 * time.Second
	}
	return &SnapshotManager{
		db:       db,
		storage:  storage,
		interval: interval,
		key:      "backups/index_snapshot.db",
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (s *SnapshotManager) Start() {
	s.wg.Add(1)
	go s.run()
	log.Printf("index snapshot manager started: interval=%v, key=%s", s.interval, s.key)
}

func (s *SnapshotManager) Stop() {
	s.cancel()
	s.wg.Wait()
}

func (s *SnapshotManager) run() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.CreateSnapshot(); err != nil {
				log.Printf("index snapshot failed: %v", err)
			}
		}
	}
}

// CreateSnapshot creates a consistent BoltDB snapshot and uploads it to storage.
func (s *SnapshotManager) CreateSnapshot() error {
	var buf bytes.Buffer

	err := s.db.View(func(tx *bolt.Tx) error {
		_, err := tx.WriteTo(&buf)
		return err
	})
	if err != nil {
		return fmt.Errorf("snapshot write: %w", err)
	}

	ctx, cancel := context.WithTimeout(s.ctx, 30*time.Second)
	defer cancel()

	if err := s.storage.Put(ctx, s.key, buf.Bytes()); err != nil {
		return fmt.Errorf("snapshot upload: %w", err)
	}

	log.Printf("index snapshot uploaded: %d bytes", buf.Len())
	return nil
}

// DB returns the underlying bolt.DB for external use (e.g., snapshot manager attachment).
func (b *BoltIndex) DB() *bolt.DB {
	return b.db
}

// RestoreFromSnapshot downloads a snapshot from storage and writes it to dbPath.
// The caller must ensure the BoltDB is NOT open before calling this function.
// After restore, the caller should open the database normally with NewBoltIndex.
func RestoreFromSnapshot(storage StorageWriter, dbPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	data, err := storage.Get(ctx, "backups/index_snapshot.db")
	if err != nil {
		return fmt.Errorf("download snapshot: %w", err)
	}

	if len(data) == 0 {
		return fmt.Errorf("snapshot is empty")
	}

	// Write snapshot data directly to the db path
	if err := os.WriteFile(dbPath, data, 0600); err != nil {
		return fmt.Errorf("write restored db: %w", err)
	}

	// Validate the restored database by opening and closing it
	db, err := bolt.Open(dbPath, 0600, &bolt.Options{Timeout: 5 * time.Second, ReadOnly: true})
	if err != nil {
		return fmt.Errorf("validate restored db: %w", err)
	}
	db.Close()

	log.Printf("index restored from snapshot: %d bytes", len(data))
	return nil
}
