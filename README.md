# kolo-kv

An ordered key-value store with one writer, kept in a signed Kolo log.

[![status: experimental](https://img.shields.io/badge/status-experimental-orange)](#status)
[![license: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue)](#license)
[![go: 1.26+](https://img.shields.io/badge/go-1.26%2B-00ADD8)](go.mod)

## What it is

Every `Put` and `Delete` is one entry in a [kolo-log](https://github.com/ciranda-dev/kolo-log) log stored with [kolo-store](https://github.com/ciranda-dev/kolo-store). The current state is an index rebuilt from that log when it opens and checked against the log's signed head. A replica trusts only entries that a verified head covers, as a [Certificate Transparency monitor (RFC 9162, section 8.2)](https://www.rfc-editor.org/info/rfc9162) does.

A peer that holds a replica of the log, kept up to date by [kolo-sync](https://github.com/ciranda-dev/kolo-sync) or anything else that calls `Replica.Update`, opens it read-only and gets the same state. No extra protocol is needed. Keys are ordered bytewise, so you can scan a range or a prefix. Values stay in the log and are read from storage when asked for.

## Install

```sh
go get github.com/ciranda-dev/kolo-kv
```

Pebble, used by kolo-store, links the C zstd library when cgo is enabled. Build with `CGO_ENABLED=0` for a pure Go binary.

## Quick start

Create a store, write and delete keys, then list a prefix. The program takes the store directory as its only argument; use a new directory in a temporary location. Save it outside the repository and run it with `go run . <store dir>` in a module that requires kolo-kv, kolo-store and kolo-id.

```go
package main

import (
	"fmt"
	"log"
	"os"

	koloid "github.com/ciranda-dev/kolo-id"
	kolokv "github.com/ciranda-dev/kolo-kv"
	kolostore "github.com/ciranda-dev/kolo-store"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("usage: myapp <store dir>")
	}
	me, err := koloid.Generate()
	check(err)
	b, err := kolostore.OpenPebble(os.Args[1])
	check(err)
	s := kolostore.New(b)
	defer s.Close()

	l, err := s.Create(me)
	check(err)
	db, err := kolokv.Open(l)
	check(err)
	defer db.Close()

	check(db.Put([]byte("user/ana"), []byte("admin")))
	check(db.Put([]byte("user/bo"), []byte("guest")))
	check(db.Delete([]byte("user/bo")))

	it := db.Prefix([]byte("user/"))
	for it.Next() {
		fmt.Printf("%s = %s\n", it.Key(), it.Value())
	}
	check(it.Close())
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
```

```text
$ go run . "$(mktemp -d)/store"
user/ana = admin
```

To open the store again later, keep `me` with [kolo-id](https://github.com/ciranda-dev/kolo-id) `Save` and `Load` and the log ID (`l.ID()`), then call `s.Open(me, logID)` and `kolokv.Open`. [examples/basic](examples/basic/main.go) also reopens the store and reads it from a replica.

## API

| Symbol | What it does |
|---|---|
| `Open(l)` | Rebuild a writable store from your own kolo-store `Log` |
| `OpenReader(r)` | Rebuild a read-only store from a kolo-store `Replica` of someone else's log |
| `Put(key, value)` | Set a key; the older value stays readable in the log until `Delete` |
| `Get(key)` | A copy of the value, or `ErrNotFound` |
| `Delete(key)` | Remove a key and destroy its older values; a missing key writes nothing |
| `Range(start, end)`, `Prefix(p)` | Iterate over `[start, end)` or over a prefix, in byte order |
| `Iterator` | `Next`, `Key`, `Value`, `Err`, `Close` |
| `Snapshot()` | A read view at the current log length |
| `Len()`, `Refresh()` | Entries the state is built from; rebuild a reader after its replica grew |
| `Close()` | Close the store and its log or replica |

The log must use the `kololog.Bytes` value encoding. A `DB` and a `Snapshot` are safe for concurrent use, and writes are serialized.

Errors work with `errors.Is`: `ErrNotFound`, `ErrReadOnly`, `ErrClosed`, `ErrKeyTooLarge`, `ErrValueTooLarge` and `ErrCorrupt`. Errors from kolo-store and kolo-log pass through wrapped.

Deletion is best effort. It holds on peers that follow the protocol and apply the delete marker, and a peer that already received the key can keep it. A deleted key's name stays in its delete entry, so keep secrets in values, not in keys. A `Put` over an existing key does not destroy the older value; only `Delete` does.

## More

- [docs/guide.md](docs/guide.md): the full API and how readers and deletes work.
- [docs/formats.md](docs/formats.md): the operation encoding and references.
- [docs/limits.md](docs/limits.md): size limits, memory and caveats.
- [SECURITY.md](SECURITY.md): security policy.

## Status

Experimental. The API may change before v1.0.0.

## License

Licensed under either of the [Apache License, Version 2.0](LICENSE) or the [MIT license](LICENSE-MIT), at your option.
