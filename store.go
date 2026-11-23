package kolokv

import (
	"context"
	"iter"
)

// Store is the key-value API that both writer models implement; *DB is the single-writer one. A nil error from Put or
// Delete means the write was accepted; when it is readable and whether it takes effect depend on the implementation.
// Get of a missing key matches ErrNotFound. Close every Iterator; one whose Next returned false is already released.
type Store interface {
	Put(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Get(key []byte) ([]byte, error)
	Range(start, end []byte) *Iterator
	Prefix(prefix []byte) *Iterator
	Close() error
}

var _ Store = (*DB)(nil)

// NewIterator returns an Iterator over seq, which must yield slices the caller may keep, for a Store outside this package.
// Next calls stop once seq ends; Close stops seq first, then calls stop if Next did not; its result is what Err returns.
// A nil stop releases nothing and a nil seq yields nothing.
func NewIterator(seq iter.Seq2[[]byte, []byte], stop func() error) *Iterator {
	if seq == nil {
		seq = func(func([]byte, []byte) bool) {}
	}
	if stop == nil {
		stop = func() error { return nil }
	}
	pull, cancel := iter.Pull2(seq)
	return &Iterator{pull: pull, cancel: cancel, stop: stop}
}
