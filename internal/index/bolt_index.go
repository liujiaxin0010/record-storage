package index

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	keyVersionV1 byte = 0x01
	keyVersionV2 byte = 0x02
)

var (
	gopBucket    = []byte("gop_index")
	gopBucketV2  = []byte("gop_v2")
	eventBucket  = []byte("events")
)

type BoltIndex struct {
	db      *bolt.DB
	useV2   bool
}

func NewBoltIndex(path string) (*BoltIndex, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(gopBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(gopBucketV2); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(eventBucket); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &BoltIndex{db: db, useV2: true}, nil
}

func (b *BoltIndex) WriteGOP(ctx context.Context, cameraID string, meta GOPMeta) error {
	value, err := encodeGOPMeta(meta)
	if err != nil {
		return err
	}

	return b.db.Update(func(tx *bolt.Tx) error {
		if b.useV2 {
			dateBucket, err := getOrCreateDateBucket(tx, gopBucketV2, cameraID, meta.StartTime)
			if err != nil {
				return err
			}
			key := makeBinaryKey(cameraID, meta.StartTime)
			if err := dateBucket.Put(key, value); err != nil {
				return err
			}
		}
		key := makeGOPKey(cameraID, meta.StartTime)
		bucket := tx.Bucket(gopBucket)
		return bucket.Put(key, value)
	})
}

func (b *BoltIndex) BatchWriteGOP(ctx context.Context, items []struct {
	CameraID string
	Meta     GOPMeta
}) error {
	if len(items) == 0 {
		return nil
	}
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(gopBucket)
		for _, item := range items {
			value, err := encodeGOPMeta(item.Meta)
			if err != nil {
				return err
			}
			if b.useV2 {
				dateBucket, err := getOrCreateDateBucket(tx, gopBucketV2, item.CameraID, item.Meta.StartTime)
				if err != nil {
					return err
				}
				key := makeBinaryKey(item.CameraID, item.Meta.StartTime)
				if err := dateBucket.Put(key, value); err != nil {
					return err
				}
			}
			key := makeGOPKey(item.CameraID, item.Meta.StartTime)
			if err := bucket.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BoltIndex) QueryRange(ctx context.Context, cameraID string, start, end time.Time) ([]GOPMeta, error) {
	var results []GOPMeta
	err := b.db.View(func(tx *bolt.Tx) error {
		if b.useV2 {
			return b.queryV2(tx, cameraID, start, end, func(meta GOPMeta) error {
				results = append(results, meta)
				return nil
			})
		}
		return b.queryV1(tx, cameraID, start, end, func(meta GOPMeta) error {
			results = append(results, meta)
			return nil
		})
	})
	return results, err
}

func (b *BoltIndex) WriteEvent(ctx context.Context, cameraID string, event RecordingEvent) error {
	key := makeEventKey(cameraID, event.Timestamp)
	value, err := json.Marshal(event)
	if err != nil {
		return err
	}

	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(eventBucket)
		return bucket.Put(key, value)
	})
}

func (b *BoltIndex) GetRecordingRanges(ctx context.Context, cameraID string, date time.Time) ([]TimeRange, error) {
	start := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())
	end := start.Add(24 * time.Hour)

	gops, err := b.QueryRange(ctx, cameraID, start, end)
	if err != nil {
		return nil, err
	}

	if len(gops) == 0 {
		return nil, nil
	}

	var ranges []TimeRange
	currentRange := TimeRange{Start: gops[0].StartTime, End: gops[0].EndTime}

	for i := 1; i < len(gops); i++ {
		gap := gops[i].StartTime.Sub(currentRange.End)
		if gap < 5*time.Second {
			currentRange.End = gops[i].EndTime
		} else {
			ranges = append(ranges, currentRange)
			currentRange = TimeRange{Start: gops[i].StartTime, End: gops[i].EndTime}
		}
	}
	ranges = append(ranges, currentRange)

	return ranges, nil
}

// DeleteRange removes all GOPs for a camera within the specified time range.
func (b *BoltIndex) DeleteRange(ctx context.Context, cameraID string, start, end time.Time) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		// Delete from V2 buckets
		if b.useV2 {
			root := tx.Bucket(gopBucketV2)
			cameraBucket := root.Bucket([]byte(cameraID))
			if cameraBucket != nil {
				startDate := start.Truncate(24 * time.Hour)
				endDate := end.Truncate(24 * time.Hour).Add(24 * time.Hour)

				for d := startDate; !d.After(endDate); d = d.Add(24 * time.Hour) {
					dateKey := []byte(d.Format("2006-01-02"))
					dateBucket := cameraBucket.Bucket(dateKey)
					if dateBucket == nil {
						continue
					}

					startKey := makeBinaryKey(cameraID, start)
					endKey := makeBinaryKey(cameraID, end)

					c := dateBucket.Cursor()
					var toDelete [][]byte
					for k, _ := c.Seek(startKey); k != nil && string(k) <= string(endKey); k, _ = c.Next() {
						keyCopy := make([]byte, len(k))
						copy(keyCopy, k)
						toDelete = append(toDelete, keyCopy)
					}
					for _, k := range toDelete {
						if err := dateBucket.Delete(k); err != nil {
							return err
						}
					}

					// If date bucket is now empty, remove it
					if dateBucket.Stats().KeyN == 0 {
						_ = cameraBucket.DeleteBucket(dateKey)
					}
				}
			}
		}

		// Delete from V1 bucket
		bucket := tx.Bucket(gopBucket)
		startKey := makeGOPKey(cameraID, start)
		endKey := makeGOPKey(cameraID, end)
		prefix := []byte(cameraID + ":")
		c := bucket.Cursor()
		var toDelete [][]byte
		for k, _ := c.Seek(startKey); k != nil && string(k) <= string(endKey); k, _ = c.Next() {
			if len(k) < len(prefix) || string(k[:len(prefix)]) != string(prefix) {
				continue
			}
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			toDelete = append(toDelete, keyCopy)
		}
		for _, k := range toDelete {
			if err := bucket.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *BoltIndex) Close() error {
	return b.db.Close()
}

func (b *BoltIndex) queryV1(tx *bolt.Tx, cameraID string, start, end time.Time, fn func(GOPMeta) error) error {
	bucket := tx.Bucket(gopBucket)
	c := bucket.Cursor()
	prefix := []byte(cameraID + ":")
	startKey := makeGOPKey(cameraID, start)
	endKey := makeGOPKey(cameraID, end)

	for k, v := c.Seek(startKey); k != nil && string(k) <= string(endKey); k, v = c.Next() {
		if len(k) < len(prefix) || string(k[:len(prefix)]) != string(prefix) {
			continue
		}
		meta, err := decodeGOPMeta(v)
		if err != nil {
			continue
		}
		if err := fn(meta); err != nil {
			return err
		}
	}
	return nil
}

func (b *BoltIndex) queryV2(tx *bolt.Tx, cameraID string, start, end time.Time, fn func(GOPMeta) error) error {
	root := tx.Bucket(gopBucketV2)
	cameraBucket := root.Bucket([]byte(cameraID))
	if cameraBucket == nil {
		return nil
	}

	startDate := start.Truncate(24 * time.Hour)
	endDate := end.Truncate(24 * time.Hour).Add(24 * time.Hour)

	for d := startDate; !d.After(endDate); d = d.Add(24 * time.Hour) {
		dateKey := []byte(d.Format("2006-01-02"))
		dateBucket := cameraBucket.Bucket(dateKey)
		if dateBucket == nil {
			continue
		}

		c := dateBucket.Cursor()
		startKey := makeBinaryKey(cameraID, start)
		endKey := makeBinaryKey(cameraID, end)

		for k, v := c.Seek(startKey); k != nil && string(k) <= string(endKey); k, v = c.Next() {
			meta, err := decodeGOPMeta(v)
			if err != nil {
				continue
			}
			if meta.StartTime.Before(start) || meta.StartTime.After(end) {
				continue
			}
			if err := fn(meta); err != nil {
				return err
			}
		}
	}
	return nil
}

func makeGOPKey(cameraID string, t time.Time) []byte {
	return []byte(fmt.Sprintf("%s:%d", cameraID, t.UnixNano()))
}

func MakeBinaryKey(cameraID string, t time.Time) []byte {
	key := make([]byte, 17)
	key[0] = keyVersionV2
	binary.BigEndian.PutUint64(key[1:9], hashCameraID(cameraID))
	binary.BigEndian.PutUint64(key[9:17], uint64(t.UnixNano()))
	return key
}

func makeBinaryKey(cameraID string, t time.Time) []byte {
	return MakeBinaryKey(cameraID, t)
}

func hashCameraID(cameraID string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(cameraID))
	return h.Sum64()
}

func getOrCreateDateBucket(tx *bolt.Tx, rootBucket []byte, cameraID string, t time.Time) (*bolt.Bucket, error) {
	root := tx.Bucket(rootBucket)
	cameraBucket, err := root.CreateBucketIfNotExists([]byte(cameraID))
	if err != nil {
		return nil, err
	}
	dateKey := []byte(t.Format("2006-01-02"))
	return cameraBucket.CreateBucketIfNotExists(dateKey)
}

func makeEventKey(cameraID string, t time.Time) []byte {
	return []byte(fmt.Sprintf("%s:%d", cameraID, t.UnixNano()))
}

func encodeGOPMeta(meta GOPMeta) ([]byte, error) {
	return json.Marshal(meta)
}

func DecodeGOPMeta(data []byte) (GOPMeta, error) {
	var meta GOPMeta
	err := json.Unmarshal(data, &meta)
	return meta, err
}

func decodeGOPMeta(data []byte) (GOPMeta, error) {
	return DecodeGOPMeta(data)
}
