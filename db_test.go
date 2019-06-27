package meter_test

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	meter "github.com/alxarch/go-meter"
	"github.com/go-redis/redis"
)

var reg = meter.NewRegistry()
var daily = meter.ResolutionDaily.WithTTL(meter.Daily)
var hourly = meter.ResolutionHourly.WithTTL(meter.Hourly)
var desc = meter.NewDesc(meter.MetricTypeIncrement, "test", []string{"foo", "bar"}, daily, hourly)
var event = meter.NewEvent(desc)
var rc = redis.NewClient(&redis.Options{
	Addr: ":6379",
	DB:   3,
})

func init() {
	time.Local = time.UTC
	reg.Register(event)
}

func Test_AppendKey(t *testing.T) {
	db := meter.NewDB(rc)
	tm := time.Now()
	k := db.Key(meter.ResolutionHourly, "foo", tm)
	println(tm.String(), k)
}
func Test_ReadWrite(t *testing.T) {
	defer rc.FlushDB()
	db := meter.NewDB(rc)
	n := event.WithLabelValues("bar", "baz").Add(1)
	if n != 1 {
		t.Errorf("Invalid counter %d", n)
	}
	event.WithLabelValues("bax").Add(1)
	now := time.Now().In(time.UTC)
	err := db.Gather(event, now)
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}
	key := db.Key(hourly, "test", now)
	if n := db.Redis.HLen(key).Val(); n != 2 {
		t.Errorf("invalid gather %d", n)
	}
	if n := event.Len(); n != 2 {
		t.Errorf("Wrong collector size %d", n)
	}
	b := meter.NewQueryBuilder()
	b = b.From("test")
	b = b.Between(now, now.Add(time.Hour))
	b = b.GroupBy("foo")
	b = b.At(meter.ResolutionHourly)
	results, err := db.Query(b.Queries(reg)...)
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}
	if len(results) != 2 {
		t.Errorf("Invalid results len %d", len(results))
	}
	for _, r := range results {
		println(fmt.Sprintf("result\n%+v\n", r))
	}

	c := meter.Controller{Q: db, Events: reg, TimeDecoder: hourly}
	s := httptest.NewServer(&c)
	// s.Start()
	defer s.Close()
	dt := now.Format(meter.HourlyDateFormat)
	res, err := s.Client().Get(s.URL + "?event=test&start=" + dt + "&end=" + dt + "&res=hourly&foo=bar")
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	} else if res.StatusCode != http.StatusOK {
		t.Errorf("Invalid response status %d: %s", res.StatusCode, res.Status)
		data, _ := ioutil.ReadAll(res.Body)
		res.Body.Close()
		results = meter.Results{}
		json.Unmarshal(data, &results)
		r := results.Find("test", meter.LabelValues{"foo": "bar"})
		if r == nil {
			t.Errorf("Result not found %v", results)
		}
	}

	results, _ = db.Query(meter.Query{
		Mode:       meter.ModeValues,
		Event:      event,
		Start:      now,
		End:        now.Add(time.Hour),
		Resolution: meter.ResolutionHourly,
	})
	values := results.FrequencyMap()
	if values["foo"] == nil {
		t.Errorf("Missing 'foo'")
	}
	results, _ = db.Query(meter.Query{
		Mode:  meter.ModeValues,
		Event: event,
		Start: now,
		End:   now,
		Values: []map[string]string{
			map[string]string{
				"foo": "bar",
			},
		},
		Resolution: hourly,
	})
	values = results.FrequencyMap()
	if values["foo"] == nil {
		t.Errorf("Missing 'foo'")
	}
	// log.Println(values)
}
