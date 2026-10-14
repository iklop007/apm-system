// Package model 是 agent 和服务端共用的数据结构。
// 时间戳统一用 unix 微秒,耗时也统一用微秒,避免混用 float 秒出精度问题。
package model

const (
	StatusOK    = "ok"
	StatusError = "error"
)

type Span struct {
	SpanID      string            `json:"span_id"`
	ParentID    string            `json:"parent_id,omitempty"`
	Name        string            `json:"name"`
	Kind        string            `json:"kind,omitempty"` // server / client / db / cache / internal
	StartOffset int64             `json:"start_offset"`   // 相对所属事务起点的偏移,微秒
	Duration    int64             `json:"duration"`       // 微秒
	Status      string            `json:"status"`
	Tags        map[string]string `json:"tags,omitempty"`
}

type Transaction struct {
	Service    string            `json:"service"`
	Name       string            `json:"name"` // 比如 "GET /api/orders"
	TraceID    string            `json:"trace_id"`
	StartTime  int64             `json:"start_time"`
	Duration   int64             `json:"duration"`
	Status     string            `json:"status"`
	HTTPStatus int               `json:"http_status,omitempty"`
	Spans      []Span            `json:"spans,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
}

// Trace 是一条完整链路。跨服务调用时,下游服务会带着同一个 trace_id
// 上报自己的事务,服务端负责按 trace_id 归并成一条。
type Trace struct {
	TraceID      string        `json:"trace_id"`
	Transactions []Transaction `json:"transactions"`
}

type MetricType string

const (
	MetricCounter   MetricType = "counter"
	MetricGauge     MetricType = "gauge"
	MetricHistogram MetricType = "histogram"
)

type Metric struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Type   MetricType        `json:"type"`
	Value  float64           `json:"value"`
	// 以下字段只有 histogram 才会带
	Count   int64     `json:"count,omitempty"`
	Sum     float64   `json:"sum,omitempty"`
	Bounds  []float64 `json:"bounds,omitempty"`  // 桶上界,升序
	Buckets []int64   `json:"buckets,omitempty"` // 累计计数,与 Bounds 一一对应
}

type LogEntry struct {
	Time    int64             `json:"time"`
	Level   string            `json:"level"` // debug / info / warn / error
	Service string            `json:"service"`
	Message string            `json:"message"`
	TraceID string            `json:"trace_id,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
}

// IngestPayload 是一次上报的批量载荷,三类数据都塞在这里。
type IngestPayload struct {
	Service string     `json:"service"`
	Host    string     `json:"host,omitempty"`
	Version string     `json:"version,omitempty"`
	Traces  []Trace    `json:"traces,omitempty"`
	Metrics []Metric   `json:"metrics,omitempty"`
	Logs    []LogEntry `json:"logs,omitempty"`
}
