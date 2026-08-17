// Package store persists open window buckets in BadgerDB so that in-progress
// aggregation survives a process restart (Phase 4). See ADR-003-badgerdb and
// docs/data-model.md for the key schema.
package store

import (
	"encoding/json"
	"fmt"
	"strings"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/flipslidersand/stream-rail/internal/window"
)

// Badger owns the underlying key-value database.
type Badger struct {
	db *badger.DB
}

// Open opens (or creates) a BadgerDB at dir. Badger's own logging is silenced
// to keep StreamRail's console output clean.
func Open(dir string) (*Badger, error) {
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger at %s: %w", dir, err)
	}
	return &Badger{db: db}, nil
}

// Close flushes and closes the database.
func (b *Badger) Close() error { return b.db.Close() }

// Buckets returns a window.Store namespaced by prefix (typically the window
// size, e.g. "5m0s") so buckets from managers of different sizes never collide.
func (b *Badger) Buckets(prefix string) window.Store {
	return &bucketStore{db: b.db, prefix: prefix}
}

// bucketStore implements window.Store for one namespace.
type bucketStore struct {
	db     *badger.DB
	prefix string
}

// keyPrefix is the Badger key namespace for this store's buckets:
//   window/{prefix}/
func (s *bucketStore) keyPrefix() string {
	return fmt.Sprintf("window/%s/", s.prefix)
}

// key builds the full Badger key for a window bucket:
//   window/{prefix}/{group_key}/{window_start_unix}
func (s *bucketStore) key(k window.Key) []byte {
	return []byte(fmt.Sprintf("%s%s/%d", s.keyPrefix(), k.GroupKey, k.WindowStart.Unix()))
}

func (s *bucketStore) Save(b *window.Bucket) error {
	data, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("marshal bucket: %w", err)
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.key(b.Key), data)
	})
}

func (s *bucketStore) Delete(k window.Key) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(s.key(k))
	})
}

func (s *bucketStore) LoadAll() ([]*window.Bucket, error) {
	var buckets []*window.Bucket
	prefix := []byte(s.keyPrefix())
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var b window.Bucket
			if err := it.Item().Value(func(val []byte) error {
				return json.Unmarshal(val, &b)
			}); err != nil {
				return fmt.Errorf("unmarshal %s: %w", strings.TrimSpace(string(it.Item().Key())), err)
			}
			bucket := b
			buckets = append(buckets, &bucket)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return buckets, nil
}
