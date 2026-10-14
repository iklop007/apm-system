package agent

import (
	"sort"
	"strings"
	"sync"

	"apm-system/internal/model"
)

// 延迟直方图的默认桶上界,单位毫秒,覆盖从快接口到秒级慢请求。
var defaultBounds = []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

type Registry struct {
	mu         sync.Mutex
	counters   map[string]*Counter
	gauges     map[string]*Gauge
	histograms map[string]*Histogram
}

func newRegistry() *Registry {
	return &Registry{
		counters:   make(map[string]*Counter),
		gauges:     make(map[string]*Gauge),
		histograms: make(map[string]*Histogram),
	}
}

func labelsKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	b.WriteByte('}')
	return b.String()
}

// Counter 只增不减的计数,服务端按相邻采样点差值算速率。
type Counter struct {
	r      *Registry
	name   string
	labels map[string]string
	val    float64
}

func (r *Registry) Counter(name string, labels map[string]string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := labelsKey(name, labels)
	c, ok := r.counters[k]
	if !ok {
		c = &Counter{r: r, name: name, labels: labels}
		r.counters[k] = c
	}
	return c
}

func (c *Counter) Inc()          { c.Add(1) }
func (c *Counter) Add(v float64) { c.r.mu.Lock(); c.val += v; c.r.mu.Unlock() }

type Gauge struct {
	r      *Registry
	name   string
	labels map[string]string
	val    float64
}

func (r *Registry) Gauge(name string, labels map[string]string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := labelsKey(name, labels)
	g, ok := r.gauges[k]
	if !ok {
		g = &Gauge{r: r, name: name, labels: labels}
		r.gauges[k] = g
	}
	return g
}

func (g *Gauge) Set(v float64) { g.r.mu.Lock(); g.val = v; g.r.mu.Unlock() }
func (g *Gauge) Add(v float64) { g.r.mu.Lock(); g.val += v; g.r.mu.Unlock() }
func (g *Gauge) Sub(v float64) { g.Add(-v) }

type Histogram struct {
	r      *Registry
	name   string
	labels map[string]string
	bounds []float64

	count   int64
	sum     float64
	buckets []int64 // buckets[i] 累计 <= bounds[i] 的个数
}

func (r *Registry) Histogram(name string, labels map[string]string) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := labelsKey(name, labels)
	if h, ok := r.histograms[k]; ok {
		return h
	}
	h := &Histogram{
		r:       r,
		name:    name,
		labels:  labels,
		bounds:  defaultBounds,
		buckets: make([]int64, len(defaultBounds)),
	}
	r.histograms[k] = h
	return h
}

func (h *Histogram) Observe(v float64) {
	h.r.mu.Lock()
	defer h.r.mu.Unlock()
	h.count++
	h.sum += v
	for i, b := range h.bounds {
		if v <= b {
			h.buckets[i]++
		}
	}
}

// snapshot 把当前所有指标拍平,Reporter 周期调用它拿增量上报。
func (r *Registry) snapshot() []model.Metric {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]model.Metric, 0, len(r.counters)+len(r.gauges)+len(r.histograms))
	for _, c := range r.counters {
		out = append(out, model.Metric{Name: c.name, Labels: c.labels, Type: model.MetricCounter, Value: c.val})
	}
	for _, g := range r.gauges {
		out = append(out, model.Metric{Name: g.name, Labels: g.labels, Type: model.MetricGauge, Value: g.val})
	}
	for _, h := range r.histograms {
		if h.count == 0 {
			continue
		}
		out = append(out, model.Metric{
			Name:    h.name,
			Labels:  h.labels,
			Type:    model.MetricHistogram,
			Value:   h.sum / float64(h.count),
			Count:   h.count,
			Sum:     h.sum,
			Bounds:  h.bounds,
			Buckets: h.buckets,
		})
	}
	return out
}
