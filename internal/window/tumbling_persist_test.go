package window

import (
	"context"
	"testing"
	"time"

	"github.com/flipslidersand/stream-rail/internal/model"
)

// fakeStore is an in-memory window.Store for testing persistence wiring.
type fakeStore struct {
	saved   map[Key]*Bucket
	deleted []Key
}

func newFakeStore() *fakeStore { return &fakeStore{saved: map[Key]*Bucket{}} }

func (f *fakeStore) Save(b *Bucket) error { f.saved[b.Key] = b; return nil }
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
