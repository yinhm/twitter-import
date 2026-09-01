package state

import (
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	doneBucket   = []byte("done")
	legacyBucket = []byte("legacy")
	metaBucket   = []byte("meta")
	scopeKey     = []byte("scope")
)

type DB struct{ db *bolt.DB }

func Open(path string) (*DB, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(doneBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(legacyBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(metaBucket)
		return err
	}); err != nil {
		db.Close()
		return nil, err
	}
	return &DB{db: db}, nil
}

func (d *DB) Close() error { return d.db.Close() }

func (d *DB) Bind(scope string) error {
	return d.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(metaBucket)
		existing := bucket.Get(scopeKey)
		if existing != nil && string(existing) != scope {
			return fmt.Errorf("checkpoint belongs to a different endpoint, Feed, or account")
		}
		if existing == nil {
			return bucket.Put(scopeKey, []byte(scope))
		}
		return nil
	})
}
func (d *DB) HasDone(id string) bool {
	found := false
	_ = d.db.View(func(tx *bolt.Tx) error { found = tx.Bucket(doneBucket).Get([]byte(id)) != nil; return nil })
	return found
}
func (d *DB) MarkDone(id, result string) error {
	return d.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(doneBucket).Put([]byte(id), []byte(result)) })
}
func (d *DB) HasLegacy(id string) bool {
	found := false
	_ = d.db.View(func(tx *bolt.Tx) error { found = tx.Bucket(legacyBucket).Get([]byte(id)) != nil; return nil })
	return found
}
func (d *DB) MarkLegacy(id string) error {
	return d.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(legacyBucket).Put([]byte(id), []byte{1}) })
}
func (d *DB) ClearLegacy() error {
	return d.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(legacyBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucket(legacyBucket)
		return err
	})
}
