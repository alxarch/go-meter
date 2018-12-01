package meter

import (
	"log"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/alxarch/go-meter/tcodec"
)

func Test_ParseQuery(t *testing.T) {

	q := url.Values{}
	q.Set("event", "foo")
	q.Set("res", "daily")
	qb, err := ParseQuery(q, tcodec.LayoutCodec(DailyDateFormat))
	Assert(t, err != nil, "Err is not nil")
	q.Set("event", "foo")
	q.Set("res", "daily")
	q.Set("start", "2017-10-30")
	q.Set("end", "2017-11-05")
	qb, err = ParseQuery(q, tcodec.LayoutCodec(DailyDateFormat))
	AssertNil(t, err)
	AssertEqual(t, qb.Events, []string{"foo"})
	events := NewRegistry()
	daily := ResolutionDaily.WithTTL(time.Hour)
	fooDesc := NewCounterDesc("foo", []string{"bar", "baz"}, daily)
	fooEvent := NewEvent(fooDesc)
	events.Register(fooEvent)
	qs := qb.Queries(events)
	AssertEqual(t, qs[0], Query{
		Mode:       ModeScan,
		Resolution: daily,
		Values:     []map[string]string{{}},
		Start:      time.Date(2017, 10, 30, 0, 0, 0, 0, time.UTC),
		End:        time.Date(2017, 11, 05, 0, 0, 0, 0, time.UTC),
		Event:      fooEvent,
	})

}

func TestClient_Sync(t *testing.T) {
	conn := rc.Get()
	defer func() {
		conn.Do("FLUSHDB")
		conn.Close()
	}()
	handler := &Controller{
		DB:       NewDB(rc),
		Registry: reg,
		Logger:   log.New(os.Stderr, "", log.LstdFlags),
	}
	s := httptest.NewServer(handler)
	defer s.Close()
	c := &Client{
		URL: s.URL,
	}
	event.Add(2, "Foo", "Bar")
	err := c.Sync(event)
	AssertNil(t, err)
}
