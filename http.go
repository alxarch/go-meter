package meter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/alxarch/go-meter/tcodec"
)

const (
	QueryParamEvent      = "event"
	QueryParamResolution = "res"
	QueryParamStart      = "start"
	QueryParamEnd        = "end"
	QueryParamGroup      = "group"
	QueryParamMode       = "mode"
)

func ParseQuery(q url.Values, tdec tcodec.TimeDecoder) (s QueryBuilder, err error) {
	eventNames := q[QueryParamEvent]
	delete(q, QueryParamEvent)
	if len(eventNames) == 0 {
		err = fmt.Errorf("Missing query.%s", QueryParamEvent)
		return
	}
	if _, ok := q[QueryParamResolution]; ok {
		s.Resolution = q.Get(QueryParamResolution)
		delete(q, QueryParamResolution)
	} else {
		err = fmt.Errorf("Missing query.%s", QueryParamResolution)
		return
	}
	if _, ok := q[QueryParamGroup]; ok {
		s.Group = q[QueryParamGroup]
		delete(q, QueryParamGroup)
	}

	if start, ok := q[QueryParamStart]; !ok {
		err = fmt.Errorf("Missing query.%s", QueryParamStart)
		return
	} else if s.Start, err = tdec.UnmarshalTime(start[0]); err != nil {
		err = fmt.Errorf("Invalid query.%s: %s", QueryParamStart, err)
		return
	}
	delete(q, QueryParamStart)
	if end, ok := q[QueryParamEnd]; !ok {
		err = fmt.Errorf("Missing query.%s", QueryParamEnd)
		return
	} else if s.End, err = tdec.UnmarshalTime(end[0]); err != nil {
		err = fmt.Errorf("Invalid query.%s: %s", QueryParamEnd, err)
		return
	}
	delete(q, QueryParamEnd)
	s.Query = q
	if now := time.Now(); s.End.After(now) {
		s.End = now
	}
	if s.Start.IsZero() || s.Start.After(s.End) {
		s.Start = s.End
	}
	var mode QueryMode
	if q.Get(QueryParamMode) == "exact" {
		mode = QueryModeExact
	}
	delete(q, QueryParamMode)
	s.Events = eventNames
	s.Mode = mode
	return

}

type Controller struct {
	Store       ReadOnlyStore
	Registry    *Registry
	TimeDecoder tcodec.TimeDecoder
}

func (c *Controller) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		code := http.StatusMethodNotAllowed
		http.Error(w, http.StatusText(code), code)
		return
	}
	q := r.URL.Query()
	qb, err := ParseQuery(q, c.TimeDecoder)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var results interface{}
	switch r.URL.Path {
	case "/values":
		qb.Events = qb.Events[:1]
		q := qb.Queries(c.Registry)[0]
		results = c.Store.Values(q.Event, q.Resolution, q.Start, q.End)
	default:
		queries := qb.Queries(c.Registry)
		results, _ = c.Store.Query(queries...)
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.Encode(results)
	// http.NotFound(w, r)
}
