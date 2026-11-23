// Package kolokvtest checks that a kolokv.Store keeps the contract of the interface.
package kolokvtest

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"

	kolokv "github.com/ciranda-dev/kolo-kv"
)

// TestStore checks the Store contract on stores made by open; settle waits until the writes made so far are readable,
// and nil means they are readable at once.
func TestStore(t *testing.T, open func(t *testing.T) kolokv.Store, settle func(t *testing.T, s kolokv.Store)) {
	t.Run("GetMissing", func(t *testing.T) {
		e := newEnv(t, open, settle)
		e.missing("a")
	})
	t.Run("PutGet", func(t *testing.T) {
		e := newEnv(t, open, settle)
		e.put("a", "1")
		e.put("e", "")
		e.value("a", "1")
		e.value("e", "")
		e.missing("b")
	})
	t.Run("Overwrite", func(t *testing.T) {
		e := newEnv(t, open, settle)
		e.put("k", "v1")
		e.put("k", "v2")
		e.value("k", "v2")
		e.pairs(e.s.Range(nil, nil), "k=v2")
	})
	t.Run("Delete", func(t *testing.T) {
		e := newEnv(t, open, settle)
		e.put("a", "1")
		e.put("b", "2")
		e.del("a")
		e.missing("a")
		e.value("b", "2")
		e.pairs(e.s.Range(nil, nil), "b=2")
		e.del("a")
		e.del("never-written")
		e.put("a", "3")
		e.value("a", "3")
	})
	t.Run("EmptyKey", func(t *testing.T) {
		e := newEnv(t, open, settle)
		e.put("", "v")
		e.put("a", "1")
		e.value("", "v")
		e.pairs(e.s.Range(nil, nil), "=v", "a=1")
		e.del("")
		e.missing("")
	})
	t.Run("Range", func(t *testing.T) {
		cases := []struct {
			name       string
			start, end []byte
			want       []string
		}{
			{"all", nil, nil, []string{"a=va", "b=vb", "c=vc", "d=vd", "\xff=v\xff"}},
			{"end is exclusive", []byte("a"), []byte("c"), []string{"a=va", "b=vb"}},
			{"start is inclusive", []byte("b"), []byte("d"), []string{"b=vb", "c=vc"}},
			{"no end", []byte("c"), nil, []string{"c=vc", "d=vd", "\xff=v\xff"}},
			{"no start", nil, []byte("b"), []string{"a=va"}},
			{"between keys", []byte("bb"), []byte("c"), nil},
			{"start equals end", []byte("b"), []byte("b"), nil},
			{"start after end", []byte("c"), []byte("b"), nil},
			{"empty end", nil, []byte{}, nil},
		}
		e := newEnv(t, open, settle)
		for _, k := range []string{"d", "b", "\xff", "a", "c"} {
			e.put(k, "v"+k)
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				(&env{t: t, s: e.s, settle: e.settle}).pairs(e.s.Range(c.start, c.end), c.want...)
			})
		}
	})
	t.Run("Prefix", func(t *testing.T) {
		e := newEnv(t, open, settle)
		for _, k := range []string{"q/1", "p/2", "p", "p/1", "o"} {
			e.put(k, "v")
		}
		e.pairs(e.s.Prefix([]byte("p/")), "p/1=v", "p/2=v")
		e.pairs(e.s.Prefix([]byte("p")), "p=v", "p/1=v", "p/2=v")
		e.pairs(e.s.Prefix([]byte("none")))
	})
	t.Run("IteratorClose", func(t *testing.T) {
		e := newEnv(t, open, settle)
		e.put("a", "1")
		e.put("b", "2")
		it := e.s.Range(nil, nil)
		if !it.Next() || string(it.Key()) != "a" || string(it.Value()) != "1" {
			t.Fatalf("first Next: key %q value %q", it.Key(), it.Value())
		}
		if err := it.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := it.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		if it.Next() {
			t.Fatal("Next after Close")
		}
		it = e.s.Range(nil, nil)
		for it.Next() {
		}
		if err := it.Err(); err != nil {
			t.Fatalf("Err at the end: %v", err)
		}
		if err := it.Close(); err != nil {
			t.Fatalf("Close after the end: %v", err)
		}
	})
	t.Run("Sizes", func(t *testing.T) {
		e := newEnv(t, open, settle)
		if err := e.s.Put(ctx, make([]byte, kolokv.MaxKeySize+1), nil); !errors.Is(err, kolokv.ErrKeyTooLarge) {
			t.Fatalf("key of MaxKeySize+1: %v", err)
		}
		if err := e.s.Put(ctx, []byte("k"), make([]byte, kolokv.MaxValueSize+1)); !errors.Is(err, kolokv.ErrValueTooLarge) {
			t.Fatalf("value of MaxValueSize+1: %v", err)
		}
		e.settle(t, e.s)
		e.missing("k")
		e.pairs(e.s.Range(nil, nil))
	})
	t.Run("DoneContext", func(t *testing.T) {
		e := newEnv(t, open, settle)
		e.put("a", "1")
		done, cancel := context.WithCancel(ctx)
		cancel()
		if err := e.s.Put(done, []byte("b"), []byte("2")); !errors.Is(err, context.Canceled) {
			t.Fatalf("Put with a done ctx: %v", err)
		}
		if err := e.s.Put(done, []byte("a"), []byte("2")); !errors.Is(err, context.Canceled) {
			t.Fatalf("overwrite with a done ctx: %v", err)
		}
		if err := e.s.Delete(done, []byte("a")); !errors.Is(err, context.Canceled) {
			t.Fatalf("Delete with a done ctx: %v", err)
		}
		if err := e.s.Put(done, make([]byte, kolokv.MaxKeySize+1), nil); !errors.Is(err, kolokv.ErrKeyTooLarge) {
			t.Fatalf("a size error comes before ctx: %v", err)
		}
		e.settle(t, e.s)
		e.pairs(e.s.Range(nil, nil), "a=1")
	})
	t.Run("Closed", func(t *testing.T) {
		e := newEnv(t, open, settle)
		e.put("a", "1")
		if err := e.s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := e.s.Put(ctx, []byte("a"), nil); !errors.Is(err, kolokv.ErrClosed) {
			t.Fatalf("Put after Close: %v", err)
		}
		if err := e.s.Delete(ctx, []byte("a")); !errors.Is(err, kolokv.ErrClosed) {
			t.Fatalf("Delete after Close: %v", err)
		}
		if _, err := e.s.Get([]byte("a")); !errors.Is(err, kolokv.ErrClosed) {
			t.Fatalf("Get after Close: %v", err)
		}
	})
}

var ctx = context.Background()

type env struct {
	t      *testing.T
	s      kolokv.Store
	settle func(t *testing.T, s kolokv.Store)
}

func newEnv(t *testing.T, open func(t *testing.T) kolokv.Store, settle func(t *testing.T, s kolokv.Store)) *env {
	t.Helper()
	if settle == nil {
		settle = func(*testing.T, kolokv.Store) {}
	}
	return &env{t: t, s: open(t), settle: settle}
}

func (e *env) put(k, v string) {
	e.t.Helper()
	if err := e.s.Put(ctx, []byte(k), []byte(v)); err != nil {
		e.t.Fatalf("Put(%q): %v", k, err)
	}
	e.settle(e.t, e.s)
}

func (e *env) del(k string) {
	e.t.Helper()
	if err := e.s.Delete(ctx, []byte(k)); err != nil {
		e.t.Fatalf("Delete(%q): %v", k, err)
	}
	e.settle(e.t, e.s)
}

func (e *env) value(k, want string) {
	e.t.Helper()
	got, err := e.s.Get([]byte(k))
	if err != nil {
		e.t.Fatalf("Get(%q): %v", k, err)
	}
	if !bytes.Equal(got, []byte(want)) {
		e.t.Fatalf("Get(%q) = %q, want %q", k, got, want)
	}
}

func (e *env) missing(k string) {
	e.t.Helper()
	if _, err := e.s.Get([]byte(k)); !errors.Is(err, kolokv.ErrNotFound) {
		e.t.Fatalf("Get(%q) error = %v, want ErrNotFound", k, err)
	}
}

// pairs reads it to its end, closes it and compares its "key=value" strings with want.
func (e *env) pairs(it *kolokv.Iterator, want ...string) {
	e.t.Helper()
	var got []string
	for it.Next() {
		got = append(got, string(it.Key())+"="+string(it.Value()))
	}
	if err := it.Close(); err != nil {
		e.t.Fatalf("iterator: %v", err)
	}
	if !slices.Equal(got, want) {
		e.t.Fatalf("pairs = %q, want %q", got, want)
	}
}
