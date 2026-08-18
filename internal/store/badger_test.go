package store_test

import (
	"testing"
	"time"

	"github.com/flipslidersand/stream-rail/internal/model"
	"github.com/flipslidersand/stream-rail/internal/store"
	"github.com/flipslidersand/stream-rail/internal/window"
)

func openTemp(t *testing.T) *store.Badger {
	t.Helper()
	db, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func bucket(group string, start time.Time, count int64) *window.Bucket {
	return &window.Bucket{
		Key:    window.Key{GroupKey: group, WindowStart: start},
		Events: []model.Event{{Service: group, Level: "ERROR", Timestamp: start.Unix()}},
		Count:  count,
	}
}

func TestBadger_SaveLoadDelete(t *testing.T) {
	db := openTemp(t)
	s := db.Buckets("5m0s")
	start := time.Date(2024, 6, 10, 10, 0, 0, 0, time.UTC)

	if err := s.Save(bucket("api", start, 3)); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := s.LoadAll()
	if err != nil {
		t.Fatalf("loadall: %v", err)
	}
	if len(got) != 1 || got[0].Count != 3 || got[0].Key.GroupKey != "api" {
		t.Fatalf("loaded = %+v, want 1 bucket count=3 group=api", got)
	}
	if !got[0].Key.WindowStart.Equal(start) {
		t.Fatalf("window start = %s, want %s", got[0].Key.WindowStart, start)
	}

	if err := s.Delete(got[0].Key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	after, _ := s.LoadAll()
	if len(after) != 0 {
		t.Fatalf("after delete = %d buckets, want 0", len(after))
	}
}

func TestBadger_CheckpointRoundTrip(t *testing.T) {
	db := openTemp(t)
	s := db.Buckets("5m0s")

	// No checkpoint yet.
	if _, found, err := s.LoadCheckpoint(); err != nil || found {
		t.Fatalf("LoadCheckpoint (empty) = found=%v err=%v, want false,nil", found, err)
	}

	if err := s.SaveCheckpoint(1_717_000_000); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	got, found, err := s.LoadCheckpoint()
	if err != nil || !found || got != 1_717_000_000 {
		t.Fatalf("LoadCheckpoint = %d,%v,%v want 1717000000,true,nil", got, found, err)
	}

	// Overwrite.
	if err := s.SaveCheckpoint(1_717_000_500); err != nil {
		t.Fatalf("SaveCheckpoint overwrite: %v", err)
	}
	got, _, _ = s.LoadCheckpoint()
	if got != 1_717_000_500 {
		t.Fatalf("after overwrite = %d, want 1717000500", got)
	}
}

func TestBadger_CheckpointNamespacedByPrefix(t *testing.T) {
	db := openTemp(t)
	if err := db.Buckets("5m0s/service").SaveCheckpoint(100); err != nil {
		t.Fatal(err)
	}
	if err := db.Buckets("5m0s/host").SaveCheckpoint(200); err != nil {
		t.Fatal(err)
	}
	svc, _, _ := db.Buckets("5m0s/service").LoadCheckpoint()
	host, _, _ := db.Buckets("5m0s/host").LoadCheckpoint()
	if svc != 100 || host != 200 {
		t.Fatalf("checkpoints collided: service=%d host=%d", svc, host)
	}
}

func TestBadger_NamespacesByPrefix(t *testing.T) {
	db := openTemp(t)
	start := time.Date(2024, 6, 10, 10, 0, 0, 0, time.UTC)

	// Same group + start, different window sizes must not collide.
	if err := db.Buckets("5m0s").Save(bucket("api", start, 5)); err != nil {
		t.Fatal(err)
	}
	if err := db.Buckets("10s").Save(bucket("api", start, 9)); err != nil {
		t.Fatal(err)
	}

	five, _ := db.Buckets("5m0s").LoadAll()
	ten, _ := db.Buckets("10s").LoadAll()
	if len(five) != 1 || five[0].Count != 5 {
		t.Fatalf("5m namespace = %+v, want count=5", five)
	}
	if len(ten) != 1 || ten[0].Count != 9 {
		t.Fatalf("10s namespace = %+v, want count=9", ten)
	}
}
