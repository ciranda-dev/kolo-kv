package kolokv

import (
	"errors"
	"slices"
)

// Iterator walks keys in byte order, as of when it was made, like database/sql Rows.
// Call Next before each Key and Value, then Err; Close is idempotent. An Iterator is for one goroutine.
type Iterator struct {
	db         *DB
	items      []item
	pull       func() ([]byte, []byte, bool) // set by NewIterator, nil once released
	cancel     func()
	stop       func() error
	key, value []byte
	err        error
}

// Next moves to the next key and reports whether there is one; it is false at the end, on an error and after Close.
// A value destroyed by Delete after the iterator was made is skipped.
func (it *Iterator) Next() bool {
	it.key, it.value = nil, nil
	if it.pull != nil {
		k, v, ok := it.pull()
		if !ok {
			it.release()
			return false
		}
		it.key, it.value = k, v
		return true
	}
	for len(it.items) > 0 {
		x := it.items[0]
		it.items = it.items[1:]
		v, err := it.db.value(x)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			it.err, it.items = err, nil
			return false
		}
		it.key, it.value = []byte(x.key), v
		return true
	}
	return false
}

// Key returns the current key; the caller may keep it.
func (it *Iterator) Key() []byte { return it.key }

// Value returns the current value; the caller may keep it.
func (it *Iterator) Value() []byte { return it.value }

// Err returns the error that stopped the iteration, if any; reaching the end is not an error.
func (it *Iterator) Err() error { return it.err }

// Close stops the iteration and returns Err.
func (it *Iterator) Close() error {
	it.release()
	it.items, it.key, it.value = nil, nil, nil
	return it.err
}

// release stops the sequence of a NewIterator iterator and keeps the result of its stop function.
func (it *Iterator) release() {
	if it.pull == nil {
		return
	}
	it.cancel()
	it.pull = nil
	it.err = it.stop()
}

// Snapshot is a read view of a store at one log length. It is safe for concurrent use and holds no resources.
// A value destroyed by Delete after the snapshot was made reads as ErrNotFound.
type Snapshot struct {
	db    *DB
	index []item
	n     uint64
}

// Get returns a copy of the value key had at the snapshot, or ErrNotFound.
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if s.db.isClosed() {
		return nil, ErrClosed
	}
	i, ok := slices.BinarySearchFunc(s.index, string(key), compareItem)
	if !ok {
		return nil, ErrNotFound
	}
	return s.db.value(s.index[i])
}

// Range returns an iterator over the snapshot's keys in [start, end); a nil end has no upper bound.
func (s *Snapshot) Range(start, end []byte) *Iterator {
	if s.db.isClosed() {
		return &Iterator{err: ErrClosed}
	}
	return &Iterator{db: s.db, items: span(s.index, start, end)}
}

// Prefix returns an iterator over the snapshot's keys that start with prefix.
func (s *Snapshot) Prefix(prefix []byte) *Iterator {
	return s.Range(prefix, upper(prefix))
}

// Len returns the log length the snapshot was made at.
func (s *Snapshot) Len() uint64 { return s.n }
