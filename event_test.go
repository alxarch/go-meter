package meter

import (
	"testing"
)

func BenchmarkEvent_Add(b *testing.B) {
	b.ReportAllocs()
	desc := NewCounterDesc("foo", []string{"bar", "baz"})
	e := NewEvent(desc)
	for i := 0; i <= b.N; i++ {
		e.Add(1, "BAR", "BAZ")
	}
}
func Test_Event(t *testing.T) {
	desc := NewCounterDesc("foo", []string{"bar", "baz"})
	e := NewEvent(desc)
	e.Add(1, "BAR", "BAZ")
	e.AddVolatile(1, "BAR", "BAZ")

	AssertEqual(t, e.Len(), 1)
	s := e.Flush(nil)
	AssertEqual(t, s, Snapshot{{
		Values: []string{"BAR", "BAZ"},
		Count:  2,
	}})
	AssertEqual(t, e.get(0).Count, int64(0))
	e.Merge(s)
	e.Add(1, "BAR", "BAZ")
	AssertEqual(t, e.get(0).Count, int64(3))
	e.Flush(nil)
	e.Pack()
	AssertEqual(t, e.Len(), 0)
	AssertEqual(t, e.index, map[uint64][]int{})
}
