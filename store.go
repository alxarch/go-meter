package meter

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type StoreRequest struct {
	Event    string    `json:"event"`
	Time     time.Time `json:"time,omitempty"`
	Labels   []string  `json:"labels"`
	Counters Snapshot  `json:"counters"`
}

type EventStore interface {
	Store(req *StoreRequest) error
}

func (event *Event) Store(tm time.Time, db EventStore) error {
	s := getSnapshot()
	defer putSnapshot(s)
	if s = event.Flush(s[:0]); len(s) == 0 {
		return nil
	}
	req := StoreRequest{
		Event:    event.Name,
		Labels:   event.Labels,
		Time:     tm,
		Counters: s,
	}
	if err := db.Store(&req); err != nil {
		event.Merge(s)
		return err
	}
	return nil
}

func StoreHandler(s EventStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		req := StoreRequest{}
		dec := json.NewDecoder(r.Body)
		if err := dec.Decode(&req); err != nil {
			code := http.StatusBadRequest
			http.Error(w, http.StatusText(code), code)
			return
		}
		if req.Time.IsZero() {
			req.Time = time.Now()
		}
		if err := s.Store(&req); err != nil {
			code := http.StatusInternalServerError
			http.Error(w, http.StatusText(code), code)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"OK"}`))
	}
}

type HTTPStore struct {
	*http.Client
	URL string
}

func (c *HTTPStore) Store(r *StoreRequest) (err error) {
	body := getSyncBuffer()
	defer putSyncBuffer(body)
	err = body.Encode(r)
	if err != nil {
		return
	}
	req, err := http.NewRequest(http.MethodPost, c.URL, &body.buffer)
	if err != nil {
		return
	}
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Content-Type", "application/json")

	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		err = fmt.Errorf("Invalid HTTP status: [%d] %s", res.StatusCode, res.Status)
	}
	return
}

type syncBuffer struct {
	buffer bytes.Buffer
	gzip   *gzip.Writer
	json   *json.Encoder
}

var syncBuffers sync.Pool

func getSyncBuffer() *syncBuffer {
	if x := syncBuffers.Get(); x != nil {
		return x.(*syncBuffer)
	}
	return new(syncBuffer)
}

func putSyncBuffer(b *syncBuffer) {
	syncBuffers.Put(b)
}

func (b *syncBuffer) Encode(x interface{}) error {
	b.buffer.Reset()
	if b.gzip == nil {
		b.gzip = gzip.NewWriter(&b.buffer)
	} else {
		b.gzip.Reset(&b.buffer)
	}
	if b.json == nil {
		b.json = json.NewEncoder(b.gzip)
	}
	if err := b.json.Encode(x); err != nil {
		return err
	}
	return b.gzip.Close()
}

type MemoryStore struct {
	data  []StoreRequest
	Event string
}

func (m *MemoryStore) Last() *StoreRequest {
	if n := len(m.data) - 1; 0 <= n && n < len(m.data) {
		return &m.data[n]
	}
	return nil

}

func (m *MemoryStore) Store(req *StoreRequest) error {
	if req.Event != m.Event {
		return errors.New("Invalid event")
	}
	last := m.Last()
	if last == nil || req.Time.After(last.Time) {
		m.data = append(m.data, *req)
		return nil
	}
	return errors.New("Invalid time")
}

func (m *MemoryStore) Scanner(event string) Scanner {
	if event == m.Event {
		return m
	}
	return nil
}

func (m *MemoryStore) Scan(ctx context.Context, q *Query) ScanIterator {
	errc := make(chan error)
	items := make(chan ScanItem)
	data := m.data
	ctx, cancel := context.WithCancel(ctx)
	it := scanIterator{
		errc:   errc,
		items:  items,
		cancel: cancel,
	}
	done := ctx.Done()
	match := q.Match.Sorted()
	go func() {
		defer close(items)
		defer close(errc)
		for i := range data {
			d := &data[i]
			if d.Time.Before(q.Start) {
				continue
			}
			for j := range d.Counters {
				c := &d.Counters[j]
				fields := ZipFields(d.Labels, c.Values)
				ok := fields.MatchSorted(match)
				if ok {
					select {
					case items <- ScanItem{
						Fields: fields,
						Time:   d.Time.Unix(),
						Count:  c.Count,
					}:
					case <-done:
						return
					}
				}
			}
		}
	}()
	return &it
}