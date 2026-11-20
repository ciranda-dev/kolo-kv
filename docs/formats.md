# Formats

Each log entry holds one op in
[kolo-codec](https://github.com/ciranda-dev/kolo-codec) encoding:

    PUT     string("kolo-kv/op/v0") ‖ uvarint(0) ‖ bytes(key ≤ 32768) ‖ bytes(value ≤ 1048576)
    DELETE  string("kolo-kv/op/v0") ‖ uvarint(1) ‖ bytes(key ≤ 32768) ‖ uvarint(1 ≤ n ≤ 65536) ‖ uvarint(first) ‖ (n-1) × uvarint(gap)

The tag versions the format and marks the log as a kolo-kv log. A delete
lists the entries it destroys in ascending order: the first
index, then for each next one the gap to the previous minus one. Another
tag or kind, a longer key, value or list, a list that overflows,
non-minimal varints or trailing bytes are `ErrCorrupt`. The log's value
encoding is `kololog.Bytes`. There is no JSON form.

## Design notes

- The API follows the usual ordered key-value store: `ErrNotFound`, blind
  deletes, bytewise key order, `[lower, upper)` bounds, the prefix upper
  bound, and point-in-time iterators and snapshots.
- `MaxKeySize` is 32768 bytes, with `ErrKeyTooLarge` and `ErrValueTooLarge`.
- Go [database/sql](https://pkg.go.dev/database/sql#Rows) `Rows`: the
  `Next`, `Err` and idempotent `Close` of `Iterator`.

Deliberate differences:

- Deletion wins over snapshots and iterators: a destroyed value is not
  returned from an older view, because destroying it is the point of
  `Delete`.
- No `context.Context`: no call waits on the network or a peer.
- The empty key is allowed.
- Values are at most 1 MiB: every value is one log entry that each reader
  stores and that a sync sends to each peer.
- `Close` on an iterator is idempotent and never undefined, as in
  database/sql.
- A reader changes state only on `Refresh`, apart from destroyed values,
  which disappear at once, and it can move back to an earlier length
  rather than show a state the writer never had.

## References

- [RFC 9162, Certificate Transparency Version 2.0](https://www.rfc-editor.org/info/rfc9162) (Experimental, December 2021).
  Takes: section 8.2, the monitor that keeps every entry: the replica only trusts entries a verified head covers.
  Not implemented: nothing left out.
