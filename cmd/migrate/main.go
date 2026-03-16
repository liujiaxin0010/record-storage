package main

import (
	"flag"
	"log"
	"time"

	"recording-server/internal/index"

	bolt "go.etcd.io/bbolt"
)

func main() {
	dbPath := flag.String("db", "data/index.db", "path to bolt database")
	dryRun := flag.Bool("dry-run", false, "show migration plan without executing")
	flag.Parse()

	db, err := bolt.Open(*dbPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	var count int
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("gop_index"))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(k, v []byte) error {
			count++
			return nil
		})
	})
	if err != nil {
		log.Fatalf("count entries: %v", err)
	}

	log.Printf("Found %d entries to migrate", count)

	if *dryRun {
		log.Printf("Dry run mode - no changes made")
		return
	}

	migrated := 0
	err = db.Update(func(tx *bolt.Tx) error {
		v1Bucket := tx.Bucket([]byte("gop_index"))
		if v1Bucket == nil {
			return nil
		}

		v2Root := tx.Bucket([]byte("gop_v2"))
		if v2Root == nil {
			var err error
			v2Root, err = tx.CreateBucket([]byte("gop_v2"))
			if err != nil {
				return err
			}
		}

		return v1Bucket.ForEach(func(k, v []byte) error {
			meta, err := index.DecodeGOPMeta(v)
			if err != nil {
				log.Printf("skip invalid entry: %v", err)
				return nil
			}

			cameraBucket, err := v2Root.CreateBucketIfNotExists([]byte(meta.CameraID))
			if err != nil {
				return err
			}

			dateKey := []byte(meta.StartTime.Format("2006-01-02"))
			dateBucket, err := cameraBucket.CreateBucketIfNotExists(dateKey)
			if err != nil {
				return err
			}

			binaryKey := index.MakeBinaryKey(meta.CameraID, meta.StartTime)
			if err := dateBucket.Put(binaryKey, v); err != nil {
				return err
			}

			migrated++
			if migrated%1000 == 0 {
				log.Printf("Migrated %d/%d entries", migrated, count)
			}
			return nil
		})
	})

	if err != nil {
		log.Fatalf("migration failed: %v", err)
	}

	log.Printf("Migration complete: %d entries migrated", migrated)
}
