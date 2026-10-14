package store

import (
	"math"
	"path/filepath"
	"testing"
	"time"

	"apm-system/internal/model"
)

func tx(service, name string, start, dur int64, status string, spans ...model.Span) model.Transaction {
	if status == "" {
		status = model.StatusOK
	}
	return model.Transaction{
		Service: service, Name: name, TraceID: "t1",
		StartTime: start, Duration: dur, Status: status,
		Spans: spans,
	}
}

// 同一个 traceID 的两个事务要归并成一条链路
func TestIngestMergesByTraceID(t *testing.T) {
	s := New(DefaultConfig())
	p := &model.IngestPayload{Service: "svc-a", Traces: []model.Trace{
		{TraceID: "t1", Transactions: []model.Transaction{tx("svc-a", "GET /a", 1000, 500, "")}},
		{TraceID: "t1", Transactions: []model.Transaction{tx("svc-b", "GET /b", 1300, 200, "")}},
	}}
	s.Ingest(p)

	rec, ok := s.Trace("t1")
	if !ok {
		t.Fatal("trace not found")
	}
	if len(rec.Transactions) != 2 {
		t.Fatalf("want 2 transactions, got %d", len(rec.Transactions))
	}
	if rec.Start != 1000 || rec.End != 1500 {
		t.Fatalf("bounds wrong: start=%d end=%d", rec.Start, rec.End)
	}

	// 重复上报同一个事务,不能重复计数
	s.Ingest(p)
	if got := len(s.Traces(TraceFilter{})); got != 1 {
		t.Fatalf("want 1 trace, got %d", got)
	}
	sums := s.Traces(TraceFilter{})
	if sums[0].TxCount != 2 {
		t.Fatalf("want TxCount=2, got %d", sums[0].TxCount)
	}
}

func TestPurgeRetention(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TraceRetention = time.Hour
	s := New(cfg)

	now := time.Now().UnixMicro()
	s.Ingest(&model.IngestPayload{Traces: []model.Trace{
		{TraceID: "old", Transactions: []model.Transaction{tx("svc", "old", now-2*int64(time.Hour/time.Microsecond), 100, "")}},
		{TraceID: "new", Transactions: []model.Transaction{tx("svc", "new", now, 100, "")}},
	}})
	s.mu.Lock()
	s.purgeLocked(now)
	s.mu.Unlock()

	if _, ok := s.Trace("old"); ok {
		t.Error("expired trace should be purged")
	}
	if _, ok := s.Trace("new"); !ok {
		t.Error("fresh trace should survive")
	}
}

// 同一秒内的指标点只保留最后一个
func TestMetricPointDedup(t *testing.T) {
	s := New(DefaultConfig())
	s.Ingest(&model.IngestPayload{Metrics: []model.Metric{
		{Name: "m", Type: model.MetricGauge, Value: 1},
	}})
	time.Sleep(10 * time.Millisecond)
	s.Ingest(&model.IngestPayload{Metrics: []model.Metric{
		{Name: "m", Type: model.MetricGauge, Value: 2},
	}})
	series := s.Metrics(MetricQuery{Names: []string{"m"}})
	if len(series) != 1 || len(series[0].Points) != 1 {
		t.Fatalf("want 1 point, got %+v", series)
	}
	if series[0].Points[0][1] != 2 {
		t.Fatalf("want latest value 2, got %v", series[0].Points[0][1])
	}
}

func TestLogCap(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LogCap = 5
	s := New(cfg)
	p := &model.IngestPayload{}
	for i := 0; i < 8; i++ {
		p.Logs = append(p.Logs, model.LogEntry{Message: "x", Time: int64(i + 1)})
	}
	s.Ingest(p)
	logs := s.Logs(LogFilter{Limit: 100})
	if len(logs) != 5 {
		t.Fatalf("want 5 logs, got %d", len(logs))
	}
	// 最旧的两条被挤掉了,最新的在最前
	if logs[0].Time != 8 || logs[4].Time != 4 {
		t.Fatalf("wrong window: first=%d last=%d", logs[0].Time, logs[4].Time)
	}
}

// 桶上界 [10,20,30],累计桶 [1,2,3] 表示 3 个样本分别落在三个桶里。
func TestQuantileFromBuckets(t *testing.T) {
	bounds := []float64{10, 20, 30}
	buckets := []int64{1, 2, 3}
	// p50:target=1.5 落在 (10,20] 桶,10 + 0.25*10 = 12.5
	got := quantileFromBuckets(buckets, bounds, 0.5)
	if math.Abs(got-12.5) > 0.001 {
		t.Fatalf("p50 want ~12.5, got %v", got)
	}
	// p99:target=2.97 落在 (10,20] 桶,10 + 0.985*10 = 19.85
	if v := quantileFromBuckets(buckets, bounds, 0.99); math.Abs(v-19.85) > 0.001 {
		t.Fatalf("p99 want ~19.85, got %v", v)
	}
	// p10:第一个桶内插值,0 + 0.3*10 = 3
	if v := quantileFromBuckets(buckets, bounds, 0.1); math.Abs(v-3) > 0.001 {
		t.Fatalf("p10 want ~3, got %v", v)
	}
}

// 用真实上报形态的累计桶做回归:9 个请求分布在 (0,50]ms 内,
// 桶计数求和(89)远大于总数(9),分位数必须按总数算而不是求和。
// 曾经有个 bug 把累计桶加起来当总数,结果 p95 从 9.6ms 变成 7500ms。
func TestQuantileWithRealisticCumulativeBuckets(t *testing.T) {
	bounds := []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}
	buckets := []int64{3, 6, 8, 9, 9, 9, 9, 9, 9, 9, 9} // count = 9
	got := quantileFromBuckets(buckets, bounds, 0.95)
	// target = 8.55,落在 (5,10] 桶:5 + (8.55-3)/6*5 = 9.625
	if math.Abs(got-9.625) > 0.001 {
		t.Fatalf("p95 want ~9.625, got %v", got)
	}
}

func TestSnapshotRoundtrip(t *testing.T) {
	s := New(DefaultConfig())
	now := time.Now().UnixMicro()
	s.Ingest(&model.IngestPayload{
		Service: "svc",
		Traces:  []model.Trace{{TraceID: "t1", Transactions: []model.Transaction{tx("svc", "GET /", now, 100, "")}}},
		Metrics: []model.Metric{{Name: "m", Type: model.MetricGauge, Value: 42}},
		Logs:    []model.LogEntry{{Time: now, Level: "info", Message: "hi"}},
	})

	path := filepath.Join(t.TempDir(), "snap.json")
	if err := s.Save(path); err != nil {
		t.Fatal(err)
	}

	s2 := New(DefaultConfig())
	if err := s2.Load(path); err != nil {
		t.Fatal(err)
	}
	if _, ok := s2.Trace("t1"); !ok {
		t.Error("trace lost after reload")
	}
	series := s2.Metrics(MetricQuery{})
	if len(series) != 1 || series[0].Points[0][1] != 42 {
		t.Errorf("metric lost after reload: %+v", series)
	}
	if got := len(s2.Logs(LogFilter{Limit: 10})); got != 1 {
		t.Errorf("logs lost after reload: %d", got)
	}
}
