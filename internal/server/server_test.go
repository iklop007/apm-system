package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"apm-system/internal/model"
	"apm-system/internal/store"
)

func newTestServer(t *testing.T) (*httptest.Server, *store.Store) {
	t.Helper()
	st := store.New(store.DefaultConfig())
	srv := New(st, nil, log.New(io.Discard, "", 0), "test")
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, st
}

func postJSON(t *testing.T, url string, v any) *http.Response {
	t.Helper()
	body, _ := json.Marshal(v)
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func decodeJSON[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()
	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return out
}

func TestEndToEnd(t *testing.T) {
	ts, _ := newTestServer(t)
	now := time.Now().UnixMicro()

	payload := model.IngestPayload{
		Service: "order-service",
		Host:    "devbox",
		Version: "1.0",
		Traces: []model.Trace{
			{
				TraceID: "trace-1",
				Transactions: []model.Transaction{
					{
						Service: "order-service", Name: "GET /api/orders", TraceID: "trace-1",
						StartTime: now, Duration: 25000, Status: model.StatusOK, HTTPStatus: 200,
						Spans: []model.Span{
							{SpanID: "s1", Name: "db.query", Kind: "db", StartOffset: 1000, Duration: 18000, Status: model.StatusOK},
						},
					},
				},
			},
			{
				TraceID: "trace-2",
				Transactions: []model.Transaction{
					{
						Service: "order-service", Name: "POST /api/checkout", TraceID: "trace-2",
						StartTime: now, Duration: 50000, Status: model.StatusError, HTTPStatus: 500,
					},
				},
			},
		},
		Metrics: []model.Metric{
			{Name: "http_requests_total", Type: model.MetricCounter, Value: 10, Labels: map[string]string{"service": "order-service"}},
			{Name: "go_goroutines", Type: model.MetricGauge, Value: 42},
		},
		Logs: []model.LogEntry{
			{Time: now, Level: "error", Service: "order-service", Message: "payment gateway timeout", TraceID: "trace-2"},
		},
	}

	resp := postJSON(t, ts.URL+"/api/v1/ingest", payload)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ingest status = %d", resp.StatusCode)
	}
	got := decodeJSON[map[string]int](t, resp)
	if got["traces"] != 2 || got["logs"] != 1 {
		t.Fatalf("ingest counts wrong: %+v", got)
	}

	// 总览
	resp2 := decodeJSON[store.Overview](t, mustGet(t, ts.URL+"/api/v1/overview?window=10m"))
	if resp2.Totals.Requests != 2 || resp2.Totals.Errors != 1 {
		t.Fatalf("overview totals wrong: %+v", resp2.Totals)
	}
	if resp2.Totals.ErrorRate != 0.5 {
		t.Fatalf("error rate want 0.5, got %v", resp2.Totals.ErrorRate)
	}
	if len(resp2.TopServices) != 1 || resp2.TopServices[0].Service != "order-service" {
		t.Fatalf("top services wrong: %+v", resp2.TopServices)
	}
	if len(resp2.Series.Buckets) != 60 { // 10m 窗口 10s 分桶
		t.Fatalf("want 60 buckets, got %d", len(resp2.Series.Buckets))
	}

	// 链路列表和详情
	list := decodeJSON[struct {
		Items []store.TraceSummary `json:"items"`
		Count int                  `json:"count"`
	}](t, mustGet(t, ts.URL+"/api/v1/traces?limit=10"))
	if len(list.Items) != 2 {
		t.Fatalf("want 2 traces, got %d", len(list.Items))
	}
	detail := decodeJSON[store.TraceRecord](t, mustGet(t, ts.URL+"/api/v1/traces/trace-1"))
	if len(detail.Transactions[0].Spans) != 1 {
		t.Fatalf("span lost: %+v", detail)
	}
	if resp := mustGet(t, ts.URL+"/api/v1/traces/nope"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("missing trace should 404, got %d", resp.StatusCode)
	}

	// 指标名和指标值
	names := decodeJSON[map[string][]store.MetricNameInfo](t, mustGet(t, ts.URL+"/api/v1/metrics/names"))
	if len(names["metrics"]) != 2 {
		t.Fatalf("metric names wrong: %+v", names)
	}
	series := decodeJSON[map[string][]store.MetricSeriesOut](t, mustGet(t, ts.URL+"/api/v1/metrics?names=go_goroutines"))
	if len(series["series"]) != 1 || series["series"][0].Points[0][1] != 42 {
		t.Fatalf("metric value wrong: %+v", series)
	}

	// 日志
	logs := decodeJSON[map[string][]model.LogEntry](t, mustGet(t, ts.URL+"/api/v1/logs?level=error"))
	if len(logs["logs"]) != 1 {
		t.Fatalf("logs wrong: %+v", logs)
	}

	// 健康检查 + 仪表盘首页
	health := decodeJSON[map[string]any](t, mustGet(t, ts.URL+"/healthz"))
	if health["status"] != "ok" {
		t.Fatalf("healthz wrong: %+v", health)
	}
	page := mustGet(t, ts.URL+"/")
	html, _ := io.ReadAll(page.Body)
	page.Body.Close()
	if !strings.Contains(string(html), "<!DOCTYPE html>") {
		t.Fatal("dashboard index not served")
	}
}

func mustGet(t *testing.T, url string) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// 窗口参数越界要报 400
func TestBadWindow(t *testing.T) {
	ts, _ := newTestServer(t)
	resp := mustGet(t, ts.URL+"/api/v1/overview?window=5h")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestIngestBadJSON(t *testing.T) {
	ts, _ := newTestServer(t)
	resp, err := http.Post(ts.URL+"/api/v1/ingest", "application/json", strings.NewReader("{not json"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", resp.StatusCode)
	}
}
