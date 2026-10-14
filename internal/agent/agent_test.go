package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"apm-system/internal/model"
)

// 起一个假的 apm-server,把收到的 payload 都记下来
func newCollector(t *testing.T) (*httptest.Server, func() []model.IngestPayload) {
	t.Helper()
	var mu sync.Mutex
	var got []model.IngestPayload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var p model.IngestPayload
		if err := json.Unmarshal(body, &p); err != nil {
			t.Errorf("collector decode: %v", err)
		}
		mu.Lock()
		got = append(got, p)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []model.IngestPayload {
		mu.Lock()
		defer mu.Unlock()
		return got
	}
}

func TestTransactionSpanNesting(t *testing.T) {
	srv, payloads := newCollector(t)
	a := New(Config{Service: "svc", ServerURL: srv.URL, FlushInterval: time.Hour})
	a.Start()
	defer a.Close()

	ctx, tx := a.StartTransaction(context.Background(), "root")
	_, s1 := tx.StartSpan(ctx, "child-1", "db")
	_, s2 := tx.StartSpan(ctx, "child-2", "cache")
	time.Sleep(5 * time.Millisecond)
	s1.SetTag("db.system", "postgres")
	s1.End()
	s2.End()
	tx.SetTag("user_id", "42")
	tx.End()

	a.reporter.flush()
	all := payloads()
	if len(all) != 1 || len(all[0].Traces) != 1 {
		t.Fatalf("want 1 trace, got %+v", all)
	}
	tr := all[0].Traces[0]
	if tr.Transactions[0].Tags["user_id"] != "42" {
		t.Error("transaction tag lost")
	}
	spans := tr.Transactions[0].Spans
	if len(spans) != 3 { // root + 2 子 span
		t.Fatalf("want 3 spans, got %d", len(spans))
	}
	byName := map[string]model.Span{}
	for _, s := range spans {
		byName[s.Name] = s
	}
	root := byName["root"]
	if root.ParentID != "" || root.Kind != "server" {
		t.Errorf("root span wrong: %+v", root)
	}
	if byName["child-1"].ParentID != root.SpanID {
		t.Error("child span should hang under root")
	}
	if byName["child-1"].Tags["db.system"] != "postgres" {
		t.Error("span tag lost")
	}
	if tr.Transactions[0].Duration <= 0 {
		t.Error("transaction duration not set")
	}
}

// 没 End 的 span 也要被兜底收尾,不能丢
func TestUnendedSpan(t *testing.T) {
	srv, payloads := newCollector(t)
	a := New(Config{Service: "svc", ServerURL: srv.URL, FlushInterval: time.Hour})
	a.Start()
	defer a.Close()

	_, tx := a.StartTransaction(context.Background(), "t")
	_, _ = tx.StartSpan(context.Background(), "leak", "internal")
	time.Sleep(2 * time.Millisecond)
	tx.End()
	a.reporter.flush()

	tr := payloads()[0].Traces[0]
	for _, s := range tr.Transactions[0].Spans {
		if s.Name == "leak" && s.Duration <= 0 {
			t.Errorf("un-ended span should get a duration, got %d", s.Duration)
		}
	}
}

func TestStartSpanWithoutTransaction(t *testing.T) {
	// ctx 里没有事务时,StartSpan 应该静默跳过,不能 panic
	ctx, span := StartSpan(context.Background(), "x", "db")
	if span != nil || ctx == nil {
		t.Fatal("want nil span")
	}
}

func TestHistogramSnapshot(t *testing.T) {
	srv, payloads := newCollector(t)
	a := New(Config{Service: "svc", ServerURL: srv.URL, FlushInterval: time.Hour})
	a.Start()
	defer a.Close()

	h := a.Metrics().Histogram("latency_ms", a.Labels())
	for i := 0; i < 10; i++ {
		h.Observe(20)
	}
	h.Observe(99999) // 超过最后一个桶上界

	a.reporter.AddMetrics(a.metrics.snapshot())
	a.reporter.flush()
	all := payloads()
	if len(all) != 1 {
		t.Fatalf("want 1 payload, got %d", len(all))
	}
	var m *model.Metric
	for i := range all[0].Metrics {
		if all[0].Metrics[i].Name == "latency_ms" {
			m = &all[0].Metrics[i]
		}
	}
	if m == nil {
		t.Fatal("histogram metric not reported")
	}
	if m.Count != 11 || len(m.Buckets) != len(m.Bounds) {
		t.Fatalf("histogram fields wrong: %+v", m)
	}
	// 20ms 落在 (10, 25] 桶,也就是 bounds[2];99999 超上界,只算进 count
	if m.Buckets[2] != 10 {
		t.Fatalf("bucket[2] want 10, got %d", m.Buckets[2])
	}
	if m.Sum != 10*20+99999 {
		t.Fatalf("sum wrong: %v", m.Sum)
	}
}

func TestInjectExtractRoundtrip(t *testing.T) {
	_, tx := (&Agent{}).StartTransaction(context.Background(), "t") // 空 agent 也够用
	ctx, _ := tx.StartSpan(context.Background(), "s", "client")

	h := http.Header{}
	Inject(ctx, h)
	if h.Get(headerTraceID) != tx.TraceID() {
		t.Error("trace id not injected")
	}
	traceID, spanID := extract(h)
	if traceID != tx.TraceID() || spanID == "" {
		t.Errorf("extract mismatch: %s %s", traceID, spanID)
	}

	// 空 ctx 不写任何头
	h2 := http.Header{}
	Inject(context.Background(), h2)
	if h2.Get(headerTraceID) != "" {
		t.Error("empty ctx should not inject headers")
	}
}

func TestMiddleware(t *testing.T) {
	srv, payloads := newCollector(t)
	a := New(Config{Service: "svc", ServerURL: srv.URL, FlushInterval: time.Hour})
	a.Start()
	defer a.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ok", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("fine"))
	})
	mux.HandleFunc("GET /boom", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	})
	ts := httptest.NewServer(a.Middleware(mux))
	defer ts.Close()

	mustGet := func(path string) int {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}
	if code := mustGet("/ok"); code != 200 {
		t.Fatalf("GET /ok = %d", code)
	}
	if code := mustGet("/boom"); code != 500 {
		t.Fatalf("GET /boom = %d", code)
	}

	a.reporter.flush()
	var traces []model.Trace
	var metrics []model.Metric
	var logs []model.LogEntry
	for _, p := range payloads() {
		traces = append(traces, p.Traces...)
		metrics = append(metrics, p.Metrics...)
		logs = append(logs, p.Logs...)
	}
	if len(traces) != 2 {
		t.Fatalf("want 2 traces, got %d", len(traces))
	}
	byStatus := map[int]model.Transaction{}
	for _, tr := range traces {
		tx := tr.Transactions[0]
		byStatus[tx.HTTPStatus] = tx
	}
	if byStatus[200].Status != model.StatusOK {
		t.Error("200 should be ok")
	}
	if byStatus[500].Status != model.StatusError {
		t.Error("500 should be error")
	}
	// 路由名应该是注册的 pattern 而不是带参数的 path
	if byStatus[200].Name != "GET /ok" {
		t.Errorf("route name wrong: %s", byStatus[200].Name)
	}
	if len(metrics) == 0 {
		t.Error("no metrics reported by middleware")
	}
	var errLog bool
	for _, l := range logs {
		if l.Level == "error" && strings.Contains(l.Message, "/boom") {
			errLog = true
		}
	}
	if !errLog {
		t.Error("expected an error log for /boom")
	}
}
