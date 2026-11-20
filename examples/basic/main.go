// Command basic writes a kolo-kv store in a temp directory, reopens it, and reads it from a replica on another store.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	koloid "github.com/ciranda-dev/kolo-id"
	kolokv "github.com/ciranda-dev/kolo-kv"
	kololog "github.com/ciranda-dev/kolo-log"
	kolostore "github.com/ciranda-dev/kolo-store"
)

func main() {
	dir, err := os.MkdirTemp("", "kolo-kv-example")
	check(err)
	defer os.RemoveAll(dir)

	me, err := koloid.Generate()
	check(err)
	b, err := kolostore.OpenPebble(filepath.Join(dir, "db"))
	check(err)
	s := kolostore.New(b)
	defer s.Close()

	l, err := s.Create(me)
	check(err)
	db, err := kolokv.Open(l)
	check(err)
	check(db.Put([]byte("user/ana"), []byte("admin")))
	check(db.Put([]byte("user/bo"), []byte("guest")))
	check(db.Put([]byte("user/bo"), []byte("editor")))
	check(db.Put([]byte("user/cy"), []byte("guest")))
	check(db.Delete([]byte("user/cy")))
	logID := l.ID()
	check(db.Close())

	// Reopen: the state is rebuilt from the log and checked against its signed head.
	l, err = s.Open(me, logID)
	check(err)
	db, err = kolokv.Open(l)
	check(err)
	defer db.Close()
	it := db.Prefix([]byte("user/"))
	for it.Next() {
		fmt.Printf("%s = %s\n", it.Key(), it.Value())
	}
	check(it.Close())

	// A peer keeps a read-only replica. Here it grows by hand, the way kolo-sync grows it.
	peer := kolostore.New(kolostore.NewMemory())
	defer peer.Close()
	hdr := l.Header()
	r, err := peer.OpenReplica(hdr.LogKey, hdr.LogID, hdr.Encode())
	check(err)
	reader, err := kolokv.OpenReader(r)
	check(err)
	defer reader.Close()
	h, err := l.Head()
	check(err)
	p, err := l.ConsistencyProof(0, h.Size)
	check(err)
	var entries []kololog.Entry
	for i := range h.Size {
		e, err := l.Entry(i)
		check(err)
		entries = append(entries, e)
	}
	check(r.Update(h, p, entries))
	check(reader.Refresh())

	v, err := reader.Get([]byte("user/bo"))
	check(err)
	fmt.Printf("peer: user/bo = %s\n", v)
	if _, err := reader.Get([]byte("user/cy")); errors.Is(err, kolokv.ErrNotFound) {
		fmt.Println("peer: user/cy not found")
	}
	if err := reader.Put([]byte("user/dee"), nil); errors.Is(err, kolokv.ErrReadOnly) {
		fmt.Println("peer: read-only")
	}
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
