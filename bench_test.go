package kolokv

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	kolostore "github.com/ciranda-dev/kolo-store"
)

// BenchmarkRefresh times a full replay of an unchanged reader on a Pebble replica.
func BenchmarkRefresh(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			w, _, _ := newDB(b)
			ctx := context.Background()
			val := make([]byte, 100)
			for i := range n {
				if err := w.Put(ctx, fmt.Appendf(nil, "key-%08d", i), val); err != nil {
					b.Fatal(err)
				}
			}
			pb, err := kolostore.OpenPebble(filepath.Join(b.TempDir(), "db"))
			if err != nil {
				b.Fatal(err)
			}
			s := kolostore.New(pb)
			b.Cleanup(func() { s.Close() })
			hdr := w.log.Header()
			r, err := s.OpenReplica(hdr.LogKey, hdr.LogID, hdr.Encode())
			if err != nil {
				b.Fatal(err)
			}
			grow(b, r, w.log)
			rd, err := OpenReader(r)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { rd.Close() })
			if rd.Len() != uint64(n) {
				b.Fatalf("reader holds %d entries, want %d", rd.Len(), n)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := rd.Refresh(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
