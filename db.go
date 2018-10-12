package meter

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

const (
	DefaultKeyPrefix = "meter"
	DefaultSeparator = '\x1f'
	DefaultScanSize  = 100
)

type DB struct {
	Redis        *redis.Client
	KeyPrefix    string
	KeySeparator byte
	ScanSize     int64
	concurrency  chan struct{} // controll concurrent scans
	// once        sync.Once // load HADDNX script once
}

func NewDB(r *redis.Client) *DB {
	db := new(DB)
	db.Redis = r
	db.KeyPrefix = DefaultKeyPrefix
	db.KeySeparator = DefaultSeparator
	db.ScanSize = DefaultScanSize
	db.concurrency = make(chan struct{}, r.Options().PoolSize)

	return db
}

func (db DB) Key(r Resolution, event string, t time.Time) (k string) {
	b := getBuffer()
	b = db.AppendKey(b[:0], r, event, t)
	k = string(b)
	putBuffer(b)
	return
}

func (db DB) AppendKey(data []byte, r Resolution, event string, t time.Time) []byte {
	if db.KeyPrefix != "" {
		data = append(data, db.KeyPrefix...)
		data = append(data, db.KeySeparator)
	}
	data = append(data, r.Name()...)
	data = append(data, db.KeySeparator)
	data = append(data, r.MarshalTime(t)...)
	data = append(data, db.KeySeparator)
	data = append(data, event...)
	return data
}

// const NilByte byte = 0
// const sNilByte = "\x00"

const maxValueSize = 255

func packField(data []byte, values, labels []string) []byte {
	for _, v := range values {
		if len(v) > maxValueSize {
			v = v[:maxValueSize]
		}
		data = append(data, byte(len(v)))
		data = append(data, v...)
	}
	for i := len(values); i < len(labels); i++ {
		data = append(data, 0)
	}
	return data
}

func (db *DB) Gather(tm time.Time, e *Event) (err error) {
	var (
		desc        = e.Describe()
		name        = desc.Name()
		t           = desc.Type()
		labels      = desc.Labels()
		data        = make([]byte, 64*len(labels))
		pipeline    = db.Redis.Pipeline()
		resolutions = desc.Resolutions()
		keys        = make(map[string]Resolution, len(resolutions))
		snapshot    = e.Flush(nil)
		size        = 0
	)
	defer pipeline.Close()
	// lmap, err := db.labelMap(name, labels, s)
	// if err != nil {
	// 	return err
	// }
	for _, res := range resolutions {
		key := db.Key(res, name, tm)
		keys[key] = res
	}
	for _, m := range snapshot {
		n := m.Count()
		if n == 0 {
			continue
		}
		data = packField(data[:0], m.values, labels)
		field := string(data)
		for key := range keys {
			switch t {
			case MetricTypeIncrement:
				pipeline.HIncrBy(key, field, n)
			case MetricTypeUpdateOnce:
				pipeline.HSetNX(key, field, n)
			case MetricTypeUpdate:
				pipeline.HSet(key, field, n)
			default:
				continue
			}
			size++
		}
	}
	if size == 0 {
		return
	}
	for key, res := range keys {
		pipeline.Expire(key, res.TTL())
	}
	_, err = pipeline.Exec()
	return
}

func (db *DB) Query(queries ...Query) (Results, error) {
	if len(queries) == 0 {
		return Results{}, nil
	}
	mode := queries[0].Mode
	results := new(scanResults)
	wg := new(sync.WaitGroup)
	for _, q := range queries {
		if err := q.Error(); err != nil {
			continue
		}
		wg.Add(1)
		go func(q Query) {
			switch mode {
			case ModeExact:
				db.exactQuery(results, q)
			case ModeScan:
				db.scanQuery(results, q)
			case ModeValues:
				db.valueQuery(results, q)
			}
			wg.Done()
		}(q)
	}
	wg.Wait()
	rr := results.results
	if rr != nil {
		for i := range rr {
			rr[i].Data.Sort()
		}
		return rr, nil
	}
	return Results{}, nil
}

func (db *DB) exactQuery(results *scanResults, q Query) error {
	var replies []*redis.StringCmd
	if err := q.Error(); err != nil {
		results.Add(scanResult{err: err})
		return err
	}
	if len(q.Values) == 0 {
		// Exact query without values is pointless
		return fmt.Errorf("Invalid query")
	}
	data := []byte{}
	res := q.Resolution
	ts := res.TimeSequence(q.Start, q.End)
	desc := q.Event.Describe()
	labels := desc.Labels()
	pipeline := db.Redis.Pipeline()
	values := make([]string, len(labels))
	defer pipeline.Close()
	for _, v := range q.Values {
		for i, label := range labels {
			values[i] = v[label]
		}
		data = packField(data[:0], values, labels)
		field := string(data)
		for _, tm := range ts {
			key := db.Key(res, desc.Name(), tm)
			replies = append(replies, pipeline.HGet(key, field))
		}
	}
	if len(replies) == 0 {
		return nil
	}
	db.acquireQueue()
	defer db.releaseQueue()
	_, err := pipeline.Exec()
	if err != nil && err != redis.Nil {
		return err
	}

	for i, values := range q.Values {
		for j, tm := range ts {
			reply := replies[i*len(ts)+j]
			n, err := reply.Int64()
			if err == redis.Nil {
				err = nil
			}
			results.Add(scanResult{
				Event:  desc.Name(),
				Time:   tm,
				Values: values,
				count:  n,
				err:    err,
			})
		}
	}
	return nil
}

func matchField(field string, labels []string, values map[string]string) bool {
	if len(values) == 0 {
		// Match all
		return true
	}
	n := 0
	size := 0
	for _, label := range labels {
		if len(field) > 0 {
			size = int(field[0])
			field = field[1:]
			if 0 <= size && size <= len(field) {
				if v, ok := values[label]; ok {
					if field[:size] != v {
						return false
					}
					n++
				}
				field = field[size:]
				continue
			}
		}
		return false
	}
	return n == len(values)
}

func (db *DB) acquireQueue() {
	db.concurrency <- struct{}{}
}
func (db *DB) releaseQueue() {
	<-db.concurrency
}

func (db *DB) scanQuery(results *scanResults, q Query) (err error) {
	desc := q.Event.Describe()
	if e := desc.Error(); e != nil {
		return e
	}
	labels := desc.Labels()
	res := q.Resolution
	ts := res.TimeSequence(q.Start, q.End)
	if len(ts) == 0 {
		return
	}
	wg := new(sync.WaitGroup)
	for _, tm := range ts {
		wg.Add(1)
		db.acquireQueue()
		go func(tm time.Time) {
			defer wg.Done()
			defer db.releaseQueue()
			key := db.Key(res, desc.Name(), tm)
			scan := db.Redis.HScan(key, 0, "", db.ScanSize).Iterator()
			var count int64
			i := 0
			j := 0
			var field string
			// group := len(r.Group) != 0
			for scan.Next() {
				if i%2 == 0 {
					field = scan.Val()
				} else {
					count, err = strconv.ParseInt(scan.Val(), 10, 64)
					if err != nil {
						err = nil
						continue
					}
					for _, v := range q.Values {
						if len(v) == 0 {
							v = fieldValues(field, labels)
						} else if matchField(field, labels, v) {
							v = copyValues(v)
						} else {
							continue
						}
						results.Add(scanResult{
							Event:  desc.Name(),
							Group:  q.Group,
							Time:   tm,
							Values: v,
							count:  count,
						})
						j++
					}
				}
				i++
			}
			if err = scan.Err(); err != nil {
				return
			}
			if j == 0 {
				// Report an empty result
				results.Add(scanResult{
					Event: desc.Name(),
					Group: q.Group,
					Time:  tm,
				})
			}
			return
		}(tm)
	}
	wg.Wait()
	return nil

}

func copyValues(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	m := make(map[string]string, len(values))
	for k, v := range values {
		m[k] = v
	}
	return m
}

func fieldValues(field string, labels []string) map[string]string {
	m := make(map[string]string, len(labels))
	for _, label := range labels {
		if len(field) > 0 {
			size := int(field[0])
			field = field[1:]
			if 0 < size && size <= len(field) {
				m[label] = field[:size]
				field = field[size:]
				continue
			}
		}
		break
	}
	return m
}

// valueQuery return a frequency map of event label values
func (db *DB) valueQuery(results *scanResults, q Query) error {
	wg := new(sync.WaitGroup)
	ts := q.Resolution.TimeSequence(q.Start, q.End)
	for _, t := range ts {
		db.acquireQueue()
		wg.Add(1)
		go func(t time.Time) {
			defer wg.Done()
			defer db.releaseQueue()
			var n int64
			desc := q.Event.Describe()
			key := db.Key(q.Resolution, desc.Name(), t)
			reply, err := db.Redis.HGetAll(key).Result()
			// if err == redis.Nil {
			// 	return
			// }
			if err != nil {
				return
			}
			labels := desc.Labels()
			for key, value := range reply {
				if n, _ = strconv.ParseInt(value, 10, 64); n == 0 {
					continue
				}
				match := false
				for _, values := range q.Values {
					match = matchField(key, labels, values)
					if match {
						break
					}
				}
				if match || len(q.Values) == 0 {
					for k, v := range fieldValues(key, labels) {
						results.Add(scanResult{
							Event:  desc.Name(),
							Group:  q.Group,
							Time:   t,
							Values: map[string]string{k: v},
							count:  n,
						})
					}
				}
			}
		}(t)
	}
	wg.Wait()
	return nil
}

var bufferPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 256)
	},
}

func getBuffer() []byte {
	return bufferPool.Get().([]byte)
}
func putBuffer(b []byte) {
	bufferPool.Put(b)
}

type scanResults struct {
	mu      sync.Mutex
	results Results
}

type scanResult struct {
	Event  string
	Group  []string
	Time   time.Time
	Values map[string]string
	err    error
	count  int64
}

func (r scanResult) AppendTo(results Results) Results {
	values := r.Values
	if r.Group != nil {
		values = make(map[string]string, len(r.Group))
		for _, g := range r.Group {
			if v, ok := r.Values[g]; ok {
				values[g] = v
			}
		}
	}
	if len(values) == 0 {
		return results
	}
	p := DataPoint{Timestamp: r.Time.Unix(), Value: r.count}
	if i := results.IndexOf(r.Event, values); i < 0 {
		return append(results, Result{
			Event:  r.Event,
			Labels: values,
			Data:   DataPoints{p},
		})
	} else if j := results[i].Data.IndexOf(r.Time); j < 0 {
		results[i].Data = append(results[i].Data, p)
	} else {
		results[i].Data[j].Value += r.count
	}
	return results
}

func (rs *scanResults) Add(r scanResult) {
	rs.mu.Lock()
	rs.results = r.AppendTo(rs.results)
	rs.mu.Unlock()
}
func (rs *scanResults) Snapshot(r Results) Results {
	rs.mu.Lock()
	r = append(r, rs.results...)
	rs.mu.Unlock()
	return r
}

// var cmdHADDNX = redis.NewScript(`
// 	-- HADDNX key member
// 	local id = redis.call('HGET', KEYS[1], ARGV[1])
// 	if id == false then
// 		id = redis.call('INCR', KEYS[1] .. ':__nextid__')
// 		redis.call('HSET', KEYS[1], ARGV[1], id)
// 	end
// 	return id
// `)

// type labelMap map[string]uint64

// func (db *DB) labelMap(event string, labels []string, s Snapshot) ([]labelMap, error) {
// 	keys := make([]string, len(labels))
// 	m := make([]labelMap, len(labels))
// 	for i, label := range labels {
// 		keys[i] = fmt.Sprintf("event:%s:label:%s:index", event, label)
// 		m[i] = labelMap{}
// 	}
// 	replies := make([]*redis.Cmd, 0, 64)
// 	pipe := db.Redis.Pipeline()
// 	defer pipe.Close()
// 	for i := range s {
// 		c := &s[i]
// 		for i := range labels {
// 			var v string
// 			if 0 <= i && i < len(c.values) {
// 				v = c.values[i]
// 			}
// 			if v == "" {
// 				continue
// 			}
// 			mm := m[i]
// 			if _, ok := mm[v]; !ok {
// 				cmd := cmdHADDNX.Eval(pipe, []string{keys[i]}, v)
// 				mm[v] = uint64(len(replies))
// 				replies = append(replies, cmd)
// 			}
// 			m[i] = mm
// 		}
// 	}
// 	_, err := pipe.Exec()
// 	if err != nil {
// 		if strings.HasPrefix(err.Error(), "NOSCRIPT ") {
// 			cmdHADDNX.Load(db.Redis)
// 			return db.labelMap(event, labels, s)
// 		}
// 		return nil, err
// 	}
// 	for _, mm := range m {
// 		for v, index := range mm {
// 			if index < uint64(len(replies)) {
// 				reply, err := replies[index].Result()
// 				if err != nil {
// 					return nil, err
// 				}
// 				switch r := reply.(type) {
// 				case string:
// 					n, err := strconv.ParseUint(r, 10, 64)
// 					if err != nil {
// 						return nil, err
// 					}
// 					mm[v] = n
// 				case int64:
// 					mm[v] = uint64(r)
// 				default:
// 					return nil, fmt.Errorf("invalid reply")
// 				}

// 			} else {
// 				return nil, fmt.Errorf("invalid index")
// 			}
// 		}
// 	}
// 	return m, nil
// }

// func CollectResults(scan <-chan ScanResult) <-chan Results {
// 	out := make(chan Results)
// 	go func() {
// 		var results Results
// 		for r := range scan {
// 			if r.err != nil {
// 				continue
// 			}

// 			results = r.AppendTo(results)
// 		}
// 		out <- results
// 	}()
// 	return out

// }

// func matchFieldToValues(field []string, values map[string]string) bool {
// 	if values == nil {
// 		return true
// 	}
// 	n := 0
// 	for i := 0; i < len(field); i += 2 {
// 		key := field[i]
// 		if v, ok := values[key]; ok {
// 			if v == field[i+1] {
// 				n++
// 			} else {
// 				return false
// 			}
// 		}
// 	}
// 	return n == len(values)
// }

// func appendFieldValues(field string, labels, values []string) []string {
// 	if len(field) > 0 {
// 		size := int(field[0])
// 		field = field[1:]
// 		if 0 <= size && size <= len(field) {
// 			values = append(values, field[:size])
// 			field = field[size:]
// 		}
// 	}
// 	return values
// }

// func AppendMatch(data []byte, s string) []byte {
// 	for i := 0; i < len(s); i++ {
// 		switch b := s[i]; b {
// 		case '*', '[', ']', '?', '^':
// 			data = append(data, '\\', b)
// 		default:
// 			data = append(data, b)
// 		}
// 	}
// 	return data
// }

// func MatchField(labels []string, group []string, q map[string]string) (f string) {
// 	b := getBuffer()
// 	b = AppendMatchField(b[:0], labels, group, q)
// 	f = string(b)
// 	putBuffer(b)
// 	return
// }

// func AppendMatchField(data []byte, labels []string, group []string, q map[string]string) []byte {
// 	if len(group) == 0 && len(q) == 0 {
// 		return append(data, '*')
// 	}
// 	for i := 0; i < len(labels); i++ {
// 		if i != 0 {
// 			data = append(data, LabelSeparator)
// 		}
// 		label := labels[i]
// 		data = AppendMatch(data, label)
// 		data = append(data, LabelSeparator)
// 		if indexOf(group, label) >= 0 {
// 			data = append(data, '[', '^', NilByte, ']', '*')
// 			continue
// 		}
// 		if q != nil {
// 			if v, ok := q[label]; ok {
// 				data = AppendMatch(data, v)
// 				continue
// 			}
// 		}
// 		data = append(data, '*')

// 	}
// 	data = append(data, FieldTerminator)
// 	return data
// }
