package kolokv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"iter"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"
)

func TestPutGetDelete(t *testing.T) {
	db, _, _ := newDB(t)
	wantMissing(t, db, "a")
	put(t, db, "a", "1")
	put(t, db, "b", "")
	wantValue(t, db, "a", "1")
	wantValue(t, db, "b", "")
	put(t, db, "a", "2")
	wantValue(t, db, "a", "2")
	del(t, db, "a")
	wantMissing(t, db, "a")
	wantValue(t, db, "b", "")
	if db.Len() != 4 {
		t.Fatalf("Len = %d, want 4", db.Len())
	}
}

func TestArgumentsAreCopied(t *testing.T) {
	db, _, _ := newDB(t)
	k, v := []byte("k"), []byte("v")
	if err := db.Put(context.Background(), k, v); err != nil {
		t.Fatal(err)
	}
	k[0], v[0] = 'x', 'x'
	got, _ := db.Get([]byte("k"))
	got[0] = 'y'
	wantValue(t, db, "k", "v")
}

func TestDeleteMissingWritesNothing(t *testing.T) {
	db, _, _ := newDB(t)
	put(t, db, "a", "1")
	del(t, db, "b")
	if db.Len() != 1 {
		t.Fatalf("Len = %d, want 1", db.Len())
	}
}

func TestEmptyKey(t *testing.T) {
	db, _, _ := newDB(t)
	put(t, db, "", "empty")
	put(t, db, "a", "1")
	wantValue(t, db, "", "empty")
	if got := keys(t, db.Range(nil, nil)); !slices.Equal(got, []string{"", "a"}) {
		t.Fatalf("keys = %q", got)
	}
	if got := keys(t, db.Range(nil, []byte(""))); len(got) != 0 {
		t.Fatalf("Range(nil, \"\") = %q, want none", got)
	}
	del(t, db, "")
	wantMissing(t, db, "")
}

func TestOverwriteDeleteReopen(t *testing.T) {
	db, s, id := newDB(t)
	put(t, db, "k", "v1")
	put(t, db, "other", "x")
	put(t, db, "k", "v2")
	put(t, db, "k", "v3")
	del(t, db, "k")
	for _, i := range []uint64{0, 2, 3} {
		if _, err := db.log.Get(i); !errors.Is(err, kololog.ErrDeleted) {
			t.Fatalf("entry %d: %v, want ErrDeleted", i, err)
		}
	}
	db = reopen(t, db, s, id)
	wantMissing(t, db, "k")
	wantValue(t, db, "other", "x")
	put(t, db, "k", "v4")
	db = reopen(t, db, s, id)
	wantValue(t, db, "k", "v4")
}

func TestOpenFinishesInterruptedDelete(t *testing.T) {
	db, s, id := newDB(t)
	put(t, db, "k", "v1")
	put(t, db, "k", "v2")
	put(t, db, "j", "x")
	// A crash right after the delete op: the puts of k are still readable.
	if _, err := db.log.Append(encodeOp(op{del: true, key: []byte("k"), list: []uint64{0, 1}})); err != nil {
		t.Fatal(err)
	}
	db = reopen(t, db, s, id)
	wantMissing(t, db, "k")
	wantValue(t, db, "j", "x")
	for _, i := range []uint64{0, 1} {
		if _, err := db.log.Get(i); !errors.Is(err, kololog.ErrDeleted) {
			t.Fatalf("entry %d: %v, want ErrDeleted", i, err)
		}
	}
	if _, err := db.log.Get(2); err != nil {
		t.Fatalf("entry 2: %v", err)
	}
}

func TestOrder(t *testing.T) {
	db, _, _ := newDB(t)
	in := []string{"b", "a", "\xff", "ab", "\x00", "a\x00", "aa", "B", "\xff\xff", "a\xff"}
	for _, k := range in {
		put(t, db, k, "v"+k)
	}
	want := slices.Clone(in)
	slices.SortFunc(want, func(a, b string) int { return bytes.Compare([]byte(a), []byte(b)) })
	if got := keys(t, db.Range(nil, nil)); !slices.Equal(got, want) {
		t.Fatalf("keys = %q, want %q", got, want)
	}
	it := db.Range(nil, nil)
	for it.Next() {
		if string(it.Value()) != "v"+string(it.Key()) {
			t.Fatalf("value of %q = %q", it.Key(), it.Value())
		}
	}
	it.Close()
}

func TestRange(t *testing.T) {
	db, _, _ := newDB(t)
	for _, k := range []string{"a", "b", "c", "d"} {
		put(t, db, k, k)
	}
	b := func(s string) []byte { return []byte(s) }
	tests := []struct {
		start, end []byte
		want       []string
	}{
		{nil, nil, []string{"a", "b", "c", "d"}},
		{b(""), nil, []string{"a", "b", "c", "d"}},
		{b("b"), b("d"), []string{"b", "c"}},
		{b("b"), nil, []string{"b", "c", "d"}},
		{nil, b("c"), []string{"a", "b"}},
		{b("a\x00"), b("c\x00"), []string{"b", "c"}},
		{b("b"), b("b\x00"), []string{"b"}},
		{b("b"), b("b"), nil},
		{b("c"), b("b"), nil},
		{b("e"), nil, nil},
		{nil, b("a"), nil},
		{nil, b(""), nil},
	}
	for _, tc := range tests {
		if got := keys(t, db.Range(tc.start, tc.end)); !slices.Equal(got, tc.want) {
			t.Errorf("Range(%q, %q) = %q, want %q", tc.start, tc.end, got, tc.want)
		}
	}
}

func TestPrefix(t *testing.T) {
	db, _, _ := newDB(t)
	for _, k := range []string{"a", "ab", "abc", "ab\xff", "ab\xff\xff", "ac", "b", "\xff", "\xff\x00", "\xff\xff"} {
		put(t, db, k, k)
	}
	tests := []struct {
		prefix string
		want   []string
	}{
		{"", []string{"a", "ab", "abc", "ab\xff", "ab\xff\xff", "ac", "b", "\xff", "\xff\x00", "\xff\xff"}},
		{"ab", []string{"ab", "abc", "ab\xff", "ab\xff\xff"}},
		{"ab\xff", []string{"ab\xff", "ab\xff\xff"}},
		{"\xff", []string{"\xff", "\xff\x00", "\xff\xff"}},
		{"\xff\xff", []string{"\xff\xff"}},
		{"abd", nil},
		{"z", nil},
	}
	for _, tc := range tests {
		if got := keys(t, db.Prefix([]byte(tc.prefix))); !slices.Equal(got, tc.want) {
			t.Errorf("Prefix(%q) = %q, want %q", tc.prefix, got, tc.want)
		}
	}
}

func TestUpper(t *testing.T) {
	tests := []struct{ in, want []byte }{
		{nil, nil},
		{[]byte{}, nil},
		{[]byte{0xff, 0xff}, nil},
		{[]byte("a"), []byte("b")},
		{[]byte("a\xff"), []byte("b")},
		{[]byte("a\xfe\xff"), []byte("a\xff")},
	}
	for _, tc := range tests {
		in := slices.Clone(tc.in)
		if got := upper(in); !bytes.Equal(got, tc.want) || (got == nil) != (tc.want == nil) {
			t.Errorf("upper(%q) = %q, want %q", tc.in, got, tc.want)
		}
		if !bytes.Equal(in, tc.in) {
			t.Errorf("upper changed its input %q", tc.in)
		}
	}
}

func TestIteratorCloseRules(t *testing.T) {
	db, _, _ := newDB(t)
	put(t, db, "a", "1")
	put(t, db, "b", "2")

	it := db.Range(nil, nil)
	if it.Key() != nil || it.Value() != nil {
		t.Fatal("Key or Value before Next")
	}
	if !it.Next() || string(it.Key()) != "a" {
		t.Fatal("first Next")
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if it.Next() || it.Key() != nil || it.Value() != nil {
		t.Fatal("Next after Close")
	}
	if err := it.Close(); err != nil || it.Err() != nil {
		t.Fatal("second Close")
	}

	it = db.Range(nil, nil)
	for it.Next() {
	}
	if it.Next() || it.Err() != nil || it.Close() != nil {
		t.Fatal("exhausted iterator")
	}

	it = db.Range(nil, nil)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if it.Next() || !errors.Is(it.Err(), ErrClosed) || !errors.Is(it.Close(), ErrClosed) {
		t.Fatalf("iterator after DB.Close: %v", it.Err())
	}
	if !errors.Is(it.Close(), ErrClosed) {
		t.Fatal("Close changed Err")
	}
	it = db.Range(nil, nil)
	if it.Next() || !errors.Is(it.Err(), ErrClosed) {
		t.Fatalf("Range on a closed DB: %v", it.Err())
	}
	if it := db.Prefix(nil); it.Next() || !errors.Is(it.Err(), ErrClosed) {
		t.Fatalf("Prefix on a closed DB: %v", it.Err())
	}
}

func TestIteratorIsPointInTime(t *testing.T) {
	db, _, _ := newDB(t)
	put(t, db, "a", "1")
	put(t, db, "b", "2")
	put(t, db, "c", "3")
	it := db.Range(nil, nil)
	put(t, db, "a", "changed")
	put(t, db, "bb", "new")
	del(t, db, "c")
	if got := pairs(t, it); !slices.Equal(got, []string{"a=1", "b=2"}) {
		t.Fatalf("got %q", got)
	}
}

func TestSnapshot(t *testing.T) {
	db, _, _ := newDB(t)
	put(t, db, "a", "1")
	put(t, db, "b", "2")
	put(t, db, "c", "3")
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	put(t, db, "a", "changed")
	put(t, db, "d", "new")
	del(t, db, "b")
	if snap.Len() != 3 || db.Len() != 6 {
		t.Fatalf("Len: snapshot %d, db %d", snap.Len(), db.Len())
	}
	wantValue(t, snap, "a", "1")
	wantMissing(t, snap, "d")
	// Delete wins: the value of b is destroyed, older views included.
	wantMissing(t, snap, "b")
	wantValue(t, snap, "c", "3")
	if got := pairs(t, snap.Range(nil, nil)); !slices.Equal(got, []string{"a=1", "c=3"}) {
		t.Fatalf("snapshot pairs = %q", got)
	}
	if got := keys(t, snap.Prefix([]byte("c"))); !slices.Equal(got, []string{"c"}) {
		t.Fatalf("snapshot prefix = %q", got)
	}
	if got := pairs(t, db.Range(nil, nil)); !slices.Equal(got, []string{"a=changed", "c=3", "d=new"}) {
		t.Fatalf("db pairs = %q", got)
	}
	db.Close()
	if _, err := snap.Get([]byte("a")); !errors.Is(err, ErrClosed) {
		t.Fatalf("snapshot Get after Close: %v", err)
	}
	if _, err := snap.Get([]byte("zz")); !errors.Is(err, ErrClosed) {
		t.Fatalf("snapshot Get of a missing key after Close: %v", err)
	}
	if it := snap.Range(nil, nil); it.Next() || !errors.Is(it.Err(), ErrClosed) {
		t.Fatalf("snapshot Range after Close: %v", it.Err())
	}
	if _, err := db.Snapshot(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Snapshot after Close: %v", err)
	}
}

func TestReopenFromDisk(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	id := newIdentity(t)
	b, err := kolostore.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	s := kolostore.New(b)
	l, err := s.Create(id)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(l)
	if err != nil {
		t.Fatal(err)
	}
	logID := l.ID()
	for i := range 20 {
		put(t, db, fmt.Sprintf("k%02d", i), fmt.Sprint(i))
	}
	put(t, db, "k03", "again")
	del(t, db, "k05")
	want := pairs(t, db.Range(nil, nil))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	b, err = kolostore.OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	s = kolostore.New(b)
	defer s.Close()
	l, err = s.Open(id, logID)
	if err != nil {
		t.Fatal(err)
	}
	db, err = Open(l)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if got := pairs(t, db.Range(nil, nil)); !slices.Equal(got, want) {
		t.Fatalf("after reopen %q, want %q", got, want)
	}
	wantMissing(t, db, "k05")
	wantValue(t, db, "k03", "again")
}

// flipBackend changes one byte of the longest stored value, the entry of the largest put.
func flipLongest(t *testing.T, b kolostore.Backend) {
	t.Helper()
	var key, value []byte
	err := b.Scan(nil, nil, false, 0, func(k, v []byte) bool {
		if len(v) > len(value) {
			key, value = k, v
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	value[len(value)/2] ^= 1
	if err := b.Put(key, value); err != nil {
		t.Fatal(err)
	}
}

func TestTamperedLogRefusesToOpen(t *testing.T) {
	b := kolostore.NewMemory()
	s := kolostore.New(b)
	defer s.Close()
	id := newIdentity(t)
	l, err := s.Create(id)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(l)
	if err != nil {
		t.Fatal(err)
	}
	put(t, db, "a", "1")
	put(t, db, "big", string(make([]byte, 4096)))
	logID := l.ID()
	db.Close()
	flipLongest(t, b)
	if l, err := s.Open(id, logID); err == nil {
		l.Close()
		t.Fatal("tampered log opened")
	}
}

func TestForeignLogRefused(t *testing.T) {
	s := newStore(t)
	id := newIdentity(t)

	l, err := s.Create(id)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, err := l.Append([]byte("not an op")); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(l); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open of a log of other data: %v, want ErrCorrupt", err)
	}

	u, err := s.Create(id, kololog.WithValueEncoding(kololog.UTF8))
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	if _, err := Open(u); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open of a UTF8 log: %v, want ErrCorrupt", err)
	}
}

func TestOpenNil(t *testing.T) {
	if _, err := Open(nil); err == nil {
		t.Fatal("Open(nil)")
	}
	if _, err := OpenReader(nil); err == nil {
		t.Fatal("OpenReader(nil)")
	}
}

func TestMaxSizes(t *testing.T) {
	db, s, id := newDB(t)
	bigKey := bytes.Repeat([]byte("k"), MaxKeySize)
	bigValue := bytes.Repeat([]byte("v"), MaxValueSize)
	if err := db.Put(context.Background(), bigKey, bigValue); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(context.Background(), append(bigKey, 'k'), nil); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("key over MaxKeySize: %v", err)
	}
	if err := db.Put(context.Background(), []byte("k"), append(bigValue, 'v')); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("value over MaxValueSize: %v", err)
	}
	if db.Len() != 1 {
		t.Fatalf("Len = %d, want 1", db.Len())
	}
	if err := db.Delete(context.Background(), append(bigKey, 'k')); err != nil {
		t.Fatalf("Delete of a key over MaxKeySize: %v", err)
	}
	db = reopen(t, db, s, id)
	got, err := db.Get(bigKey)
	if err != nil || !bytes.Equal(got, bigValue) {
		t.Fatalf("largest pair after reopen: %v", err)
	}
}

func TestClosed(t *testing.T) {
	db, _, _ := newDB(t)
	put(t, db, "a", "1")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := db.Put(context.Background(), []byte("a"), nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Put: %v", err)
	}
	if err := db.Delete(context.Background(), []byte("a")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := db.Get([]byte("a")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get: %v", err)
	}
	if err := db.Refresh(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Refresh: %v", err)
	}
	if _, err := db.log.Get(0); !errors.Is(err, kolostore.ErrClosed) {
		t.Fatalf("log after Close: %v", err)
	}
}

func TestWriterRefreshDoesNothing(t *testing.T) {
	db, _, _ := newDB(t)
	put(t, db, "a", "1")
	if err := db.Refresh(); err != nil {
		t.Fatal(err)
	}
	wantValue(t, db, "a", "1")
}

func TestConcurrentUse(t *testing.T) {
	db, _, _ := newDB(t)
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 25 {
				k := []byte(fmt.Sprintf("w%d-%02d", w, i))
				if err := db.Put(context.Background(), k, k); err != nil {
					t.Error(err)
					return
				}
				if i%3 == 0 {
					if err := db.Delete(context.Background(), k); err != nil {
						t.Error(err)
						return
					}
				}
				if _, err := db.Get(k); err != nil && !errors.Is(err, ErrNotFound) {
					t.Error(err)
					return
				}
				it := db.Prefix([]byte(fmt.Sprintf("w%d", w)))
				for it.Next() {
				}
				if err := it.Close(); err != nil {
					t.Error(err)
					return
				}
				if _, err := db.Snapshot(); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := len(keys(t, db.Range(nil, nil))); got != 4*16 {
		t.Fatalf("%d keys, want %d", got, 4*16)
	}
}

func TestEmptyValueIsNotNil(t *testing.T) {
	db, s, id := newDB(t)
	put(t, db, "k", "")
	db = reopen(t, db, s, id)
	if v, err := db.Get([]byte("k")); err != nil || v == nil {
		t.Fatalf("empty value: %#v %v", v, err)
	}
}

func TestDeleteOutsideTheDBIsCorrupt(t *testing.T) {
	db, s, id := newDB(t)
	put(t, db, "k", "v")
	put(t, db, "j", "x")
	if err := db.log.Delete(0); err != nil {
		t.Fatal(err)
	}
	logID := db.log.ID()
	db.Close()
	l, err := s.Open(id, logID)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, err := Open(l); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open with a destroyed entry and no delete op: %v, want ErrCorrupt", err)
	}
}

func TestPutRollsOverOldValues(t *testing.T) {
	defer func(n int) { maxPuts = n }(maxPuts)
	maxPuts = 3
	db, s, id := newDB(t)
	r := newReplica(t, db)
	for _, v := range []string{"v1", "v2", "v3", "v4"} {
		put(t, db, "k", v)
	}
	// Entries: puts 0 1 2, the delete op 3 that lists them, put 4.
	for i := range uint64(3) {
		if _, err := db.log.Get(i); !errors.Is(err, kololog.ErrDeleted) {
			t.Fatalf("entry %d: %v, want ErrDeleted", i, err)
		}
	}
	if db.Len() != 5 {
		t.Fatalf("Len = %d, want 5", db.Len())
	}
	wantValue(t, db, "k", "v4")
	grow(t, r, db.log)
	applyMarkers(t, r, db.log)
	reader, err := OpenReader(r)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	wantValue(t, reader, "k", "v4")
	db = reopen(t, db, s, id)
	wantValue(t, db, "k", "v4")
	del(t, db, "k")
	wantMissing(t, reopen(t, db, s, id), "k")
}

func TestCorruptAfterOpen(t *testing.T) {
	b := newFaultBackend()
	s := kolostore.New(b)
	defer s.Close()
	l, err := s.Create(newIdentity(t))
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(l)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	put(t, db, "a", "1")
	snap, err := db.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	b.set(func(b *faultBackend) { b.corrupt = true })
	if _, err := db.Get([]byte("a")); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get: %v, want ErrCorrupt", err)
	}
	if _, err := snap.Get([]byte("a")); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Snapshot.Get: %v, want ErrCorrupt", err)
	}
	it := db.Range(nil, nil)
	if it.Next() || !errors.Is(it.Close(), ErrCorrupt) {
		t.Fatalf("Iterator: %v, want ErrCorrupt", it.Err())
	}
}

// failedErase writes two values of k, then makes the second commit from now fail: the one that destroys them.
func failedErase(t *testing.T) (*DB, *kolostore.Store, *koloid.Identity) {
	t.Helper()
	b := newFaultBackend()
	s := kolostore.New(b)
	t.Cleanup(func() { s.Close() })
	id := newIdentity(t)
	l, err := s.Create(id)
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	put(t, db, "k", "v1")
	put(t, db, "k", "v2")
	put(t, db, "j", "x")
	b.set(func(b *faultBackend) { b.failAfter = b.commits + 2 })
	return db, s, id
}

// wantDestroyedOnReopen checks that entries 0 and 1 are readable, then destroyed by the next Open.
func wantDestroyedOnReopen(t *testing.T, db *DB, s *kolostore.Store, id *koloid.Identity) {
	t.Helper()
	for _, i := range []uint64{0, 1} {
		if _, err := db.log.Get(i); err != nil {
			t.Fatalf("entry %d destroyed although its write failed: %v", i, err)
		}
	}
	db = reopen(t, db, s, id)
	wantMissing(t, db, "k")
	wantValue(t, db, "j", "x")
	for _, i := range []uint64{0, 1} {
		if _, err := db.log.Get(i); !errors.Is(err, kololog.ErrDeleted) {
			t.Fatalf("entry %d after reopen: %v, want ErrDeleted", i, err)
		}
	}
}

func TestDeleteWhoseDestroyFails(t *testing.T) {
	db, s, id := failedErase(t)
	if err := db.Delete(context.Background(), []byte("k")); !errors.Is(err, errDisk) {
		t.Fatalf("Delete: %v, want the disk error", err)
	}
	wantMissing(t, db, "k")
	wantLen(t, db, 4)
	wantDestroyedOnReopen(t, db, s, id)
}

func TestPutWhoseAutomaticDeleteFails(t *testing.T) {
	defer func(n int) { maxPuts = n }(maxPuts)
	maxPuts = 2
	db, s, id := failedErase(t)
	if err := db.Put(context.Background(), []byte("k"), []byte("v3")); !errors.Is(err, errDisk) {
		t.Fatalf("Put: %v, want the disk error", err)
	}
	// The delete entry is written and the new value is not.
	wantMissing(t, db, "k")
	wantLen(t, db, 4)
	wantDestroyedOnReopen(t, db, s, id)
}

func TestDeleteDestroysInOneWrite(t *testing.T) {
	b := newFaultBackend()
	s := kolostore.New(b)
	defer s.Close()
	l, err := s.Create(newIdentity(t))
	if err != nil {
		t.Fatal(err)
	}
	db, err := Open(l)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := range 50 {
		put(t, db, "k", fmt.Sprint(i))
	}
	var before int
	b.set(func(b *faultBackend) { before = b.commits })
	del(t, db, "k")
	b.set(func(b *faultBackend) {
		if n := b.commits - before; n != 2 {
			t.Errorf("Delete of 50 values made %d writes, want 2", n)
		}
	})
}

func TestPutHonoursContext(t *testing.T) {
	db, _, _ := newDB(t)
	put(t, db, "a", "1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := db.Put(ctx, []byte("b"), []byte("2")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Put with a done ctx: %v", err)
	}
	if err := db.Put(ctx, []byte("a"), []byte("2")); !errors.Is(err, context.Canceled) {
		t.Fatalf("overwrite with a done ctx: %v", err)
	}
	if err := db.Put(ctx, make([]byte, MaxKeySize+1), nil); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("size errors come before ctx: %v", err)
	}
	if db.Len() != 1 {
		t.Fatalf("Len = %d, want 1", db.Len())
	}
	wantValue(t, db, "a", "1")
	wantMissing(t, db, "b")
}

func TestDeleteHonoursContext(t *testing.T) {
	db, _, _ := newDB(t)
	put(t, db, "a", "1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := db.Delete(ctx, []byte("a")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete with a done ctx: %v", err)
	}
	if db.Len() != 1 {
		t.Fatalf("Len = %d, want 1", db.Len())
	}
	wantValue(t, db, "a", "1")
}

func TestNewIteratorStopsSeq(t *testing.T) {
	errStop := errors.New("stop failed")
	var events []string
	seq := func(n int) iter.Seq2[[]byte, []byte] {
		return func(yield func([]byte, []byte) bool) {
			defer func() { events = append(events, "seq") }()
			for i := range n {
				if !yield([]byte{byte('a' + i)}, []byte{byte('A' + i)}) {
					return
				}
			}
		}
	}
	stop := func() error { events = append(events, "stop"); return errStop }

	// Next to the end releases at once: stop ran, once, and Err reports its result.
	it := NewIterator(seq(2), stop)
	if got := drain(it); !slices.Equal(got, []string{"a=A", "b=B"}) {
		t.Fatalf("pairs = %q", got)
	}
	if !slices.Equal(events, []string{"seq", "stop"}) || !errors.Is(it.Err(), errStop) {
		t.Fatalf("after Next to the end: events %q, Err %v", events, it.Err())
	}
	if err := it.Close(); !errors.Is(err, errStop) || it.Close() != errStop || len(events) != 2 {
		t.Fatalf("Close after the end: events %q", events)
	}

	// Close before the end stops seq first, then calls stop once.
	events = nil
	it = NewIterator(seq(10), stop)
	if !it.Next() || string(it.Key()) != "a" {
		t.Fatal("first Next")
	}
	if err := it.Close(); !errors.Is(err, errStop) {
		t.Fatalf("Close: %v", err)
	}
	if err := it.Close(); !errors.Is(err, errStop) {
		t.Fatalf("second Close: %v", err)
	}
	if !slices.Equal(events, []string{"seq", "stop"}) {
		t.Fatalf("events = %q, want seq then stop, once", events)
	}
	if it.Next() || it.Key() != nil || it.Value() != nil {
		t.Fatal("Next after Close")
	}
}

func TestNewIteratorNilStopAndSeq(t *testing.T) {
	seq := func(yield func([]byte, []byte) bool) {
		for _, k := range []string{"a", "b"} {
			if !yield([]byte(k), []byte(k)) {
				return
			}
		}
	}
	it := NewIterator(seq, nil)
	if got := drain(it); !slices.Equal(got, []string{"a=a", "b=b"}) || it.Err() != nil {
		t.Fatalf("to the end: pairs %q, Err %v", got, it.Err())
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close after the end: %v", err)
	}
	it = NewIterator(seq, nil)
	if !it.Next() {
		t.Fatal("Next")
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close before the end: %v", err)
	}

	it = NewIterator(nil, nil)
	if it.Next() || it.Close() != nil {
		t.Fatal("nil seq must yield nothing")
	}
}

func drain(it *Iterator) []string {
	var out []string
	for it.Next() {
		out = append(out, string(it.Key())+"="+string(it.Value()))
	}
	return out
}
