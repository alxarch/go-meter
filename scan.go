package meter

import (
	"context"
	"sync"
)

// Scanner scans stored data according to a query
type Scanner interface {
	Scan(ctx context.Context, q *Query) (ScanResults, error)
}

// Scanners provides a Scanner for an event
type Scanners interface {
	Scanner(event string) Scanner
}

// Scan executes a query using scanners
func (q *Query) Scan(ctx context.Context, s Scanners, events ...string) (Results, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	done := ctx.Done()
	errc := make(chan error, len(events))
	ch := make(chan Result, len(events))
	wg := new(sync.WaitGroup)
	var agg Results
	go func() {
		defer close(errc)
		for r := range ch {
			agg = append(agg, r)
		}
	}()
	for i := range events {
		event := events[i]
		s := s.Scanner(event)
		if s == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			results, err := s.Scan(ctx, q)
			if err != nil {
				errc <- err
				return
			}
			for i := range results {
				select {
				case ch <- Result{
					Event:      event,
					ScanResult: results[i],
				}:
				case <-done:
					errc <- ctx.Err()
					return
				}
			}
		}()
	}
	wg.Wait()
	close(ch)
	for err := range errc {
		if err != nil {
			return nil, err
		}
	}
	return agg, nil
}

type ScanResult struct {
	Fields Fields     `json:"fields,omitempty"`
	Total  float64    `json:"total"`
	Data   DataPoints `json:"data,omitempty"`
}

// Add ads a
func (r *ScanResult) Add(t int64, v float64) {
	r.Total += v
	r.Data = r.Data.Add(t, v)
}

type ScanResults []ScanResult

func (results ScanResults) Add(fields Fields, t int64, v float64) ScanResults {
	for i := range results {
		r := &results[i]
		if r.Fields.Equal(fields) {
			r.Add(t, v)
			return results
		}
	}
	return append(results, ScanResult{
		Fields: fields,
		Total:  v,
		Data:   []DataPoint{{t, v}},
	})

}

// Reset resets a result
func (r *ScanResult) Reset() {
	*r = ScanResult{
		Data: r.Data[:0],
	}
}
