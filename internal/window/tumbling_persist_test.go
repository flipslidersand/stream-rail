package window

import (
	"context"
	"testing"
	"time"

	"github.com/flipslidersand/stream-rail/internal/model"
)

// fakeStore is an in-memory window.Store for testing persistence wiring.
type fakeStore struct {
	saved      map[Key]*Bucket
	deleted    []Key
	checkpoint int64
	hasCP      bool
}

func newFakeStore() *fakeStore { return &fakeStore{saved: map[Key]*Bucket{}} }

func (f *fakeStore) Save(b *Bucket) error {
	// Copy so later in-place mutations of the live bucket don't retro-edit the
	// "persisted" snapshot (mirrors real JSON serialization).
	cp := *b
	cp.Events = append([]model.Event(nil), b.Events...)
	f.saved[b.Key] = &cp
	return nil
}
func (f *fakeStore) Delete(k Key) error {
	delete(f.saved, k)
	f.deleted = append(f.deleted, k)
	return nil
}
func (f *fakeStore) LoadAll() ([]*Bucket, error) {
	out := make([]*Bucket, 0, len(f.saved))
	for _, b := range f.saved {
		out = append(out, b)
	}
	return out, nil
}
func (f *fakeStore) SaveCheckpoint(maxTS int64) error {
	f.checkpoint = maxTS
	f.hasCP = true
	return nil
}
func (f *fakeStore) LoadCheckpoint() (int64, bool, error) {
	return f.checkpoint, f.hasCP, nil
}

func TestManager_PersistsOnAddAndDeletesOnClose(t *testing.T) {
	fs := newFakeStore()
	out := make(chan Batch, 1)
	m := NewManager(5*time.Minute, nil, out, nil, nil)
	m.SetStore(fs)

	ev := model.Event{Service: "api", Level: "ERROR", Timestamp: base.Unix()}
	m.add(ev)

	if len(fs.saved) != 1 {
		t.Fatalf("expected 1 persisted bucket, got %d", len(fs.saved))
	}

	// Close the window: bucket must be flushed and its persisted state removed.
	m.closeExpired(context.Background(), base.Add(5*time.Minute))
	<-out
	if len(fs.saved) != 0 {
		t.Fatalf("persisted bucket not deleted on close: %d remain", len(fs.saved))
	}
	if len(fs.deleted) != 1 {
		t.Fatalf("expected 1 delete, got %d", len(fs.deleted))
	}
}

func TestManager_RestoreLoadsOpenWindows(t *testing.T) {
	fs := newFakeStore()
	key := Key{GroupKey: "api", WindowStart: base}
	fs.saved[key] = &Bucket{Key: key, Count: 15}

	m := NewManager(5*time.Minute, nil, nil, nil, nil)
	m.SetStore(fs)

	n, err := m.Restore()
	if err != nil || n != 1 {
		t.Fatalf("Restore = %d,%v want 1,nil", n, err)
	}
	if m.windows[key] == nil || m.windows[key].Count != 15 {
		t.Fatalf("restored window missing or wrong count: %+v", m.windows[key])
	}
}

// #18: the watermark (maxTS) is persisted on each advance and restored on
// startup so event-time progress continues across a restart.
func TestManager_SavesAndRestoresWatermark(t *testing.T) {
	fs := newFakeStore()
	m := NewManager(10*time.Second, nil, nil, nil, nil)
	m.SetStore(fs)

	m.add(evAt("api", base.Add(30*time.Second).Unix()))
	if !fs.hasCP || fs.checkpoint != base.Add(30*time.Second).Unix() {
		t.Fatalf("checkpoint = %d (has=%v), want %d", fs.checkpoint, fs.hasCP, base.Add(30*time.Second).Unix())
	}

	// A fresh manager restoring from the same store recovers the watermark.
	m2 := NewManager(10*time.Second, nil, nil, nil, nil)
	m2.SetStore(fs)
	if _, err := m2.Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if m2.maxTS != base.Add(30*time.Second).Unix() {
		t.Fatalf("restored maxTS = %d, want %d", m2.maxTS, base.Add(30*time.Second).Unix())
	}
}

// #18: a window flushed before a crash is persisted as Closed and must be
// restored into the retained set — not re-opened and re-emitted.
func TestManager_RestoreClosedBucketNotReemitted(t *testing.T) {
	fs := newFakeStore()
	out := make(chan Batch, 4)
	m := NewManager(10*time.Second, nil, out, nil, nil)
	m.SetLateness(5 * time.Second)
	m.SetStore(fs)

	// One event, then close window A via the processing-time fallback (no newer
	// event, so no second window opens). A is persisted with Closed=true.
	m.add(evAt("api", base.Unix()))
	m.closeExpired(context.Background(), base.Add(16*time.Second))
	<-out // the close batch

	// Simulate a restart: a fresh manager restores from the same store.
	out2 := make(chan Batch, 4)
	m2 := NewManager(10*time.Second, nil, out2, nil, nil)
	m2.SetLateness(5 * time.Second)
	m2.SetStore(fs)
	open, err := m2.Restore()
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if open != 0 {
		t.Fatalf("restored open windows = %d, want 0 (bucket was closed)", open)
	}
	if m2.closed[Key{GroupKey: "api", WindowStart: base}] == nil {
		t.Fatal("closed bucket should be restored into the retained set")
	}

	// Closing again must not re-emit the already-closed window.
	m2.closeExpired(context.Background(), base.Add(16*time.Second))
	select {
	case b := <-out2:
		t.Fatalf("restored closed window was re-emitted: %+v", b)
	default:
	}
}
