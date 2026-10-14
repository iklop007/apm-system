package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"apm-system/internal/model"
	"apm-system/internal/store"
)

// handleIngest 接收 agent 的批量上报。
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 8<<20)
	var p model.IngestPayload
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeError(w, http.StatusBadRequest, "bad payload: "+err.Error())
		return
	}
	t, m, l := s.store.Ingest(&p)
	writeJSON(w, http.StatusOK, map[string]int{
		"traces": t, "metrics": m, "logs": l,
	})
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	window, err := parseWindow(r.URL.Query().Get("window"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, s.store.Overview(window))
}

func (s *Server) handleTraces(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	minDur, _ := strconv.ParseFloat(q.Get("min_duration_ms"), 64)
	before, _ := strconv.ParseInt(q.Get("before"), 10, 64)

	items := s.store.Traces(store.TraceFilter{
		Service:  q.Get("service"),
		Status:   q.Get("status"),
		Q:        q.Get("q"),
		MinDurMS: minDur,
		Before:   before,
		Limit:    limit,
	})
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

func (s *Server) handleTraceDetail(w http.ResponseWriter, r *http.Request) {
	rec, ok := s.store.Trace(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "trace not found")
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) handleServices(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"services": s.store.Services()})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var names []string
	for _, part := range strings.Split(q.Get("names"), ",") {
		if part = strings.TrimSpace(part); part != "" {
			names = append(names, part)
		}
	}
	if len(names) == 0 {
		writeError(w, http.StatusBadRequest, "names is required")
		return
	}
	since, _ := strconv.ParseInt(q.Get("since"), 10, 64)
	quantile, _ := strconv.ParseFloat(q.Get("quantile"), 64)

	series := s.store.Metrics(store.MetricQuery{
		Names:    names,
		Since:    since,
		Rate:     q.Get("rate") == "true",
		Quantile: quantile,
	})
	writeJSON(w, http.StatusOK, map[string]any{"series": series})
}

func (s *Server) handleMetricNames(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"metrics": s.store.MetricNames()})
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	logs := s.store.Logs(store.LogFilter{
		Level:   q.Get("level"),
		Service: q.Get("service"),
		Q:       q.Get("q"),
		Limit:   limit,
	})
	writeJSON(w, http.StatusOK, map[string]any{"logs": logs})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"version":        s.version,
		"now":            time.Now().UnixMicro(),
		"uptime_seconds": int(time.Since(s.startedAt).Seconds()),
		"store":          s.store.Stats(),
	})
}

// parseWindow 解析窗口参数,限制在 1 分钟到 1 小时之间。
func parseWindow(s string) (time.Duration, error) {
	if s == "" {
		return 15 * time.Minute, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("bad window: %s", s)
	}
	if d < time.Minute || d > time.Hour {
		return 0, fmt.Errorf("window must be between 1m and 1h")
	}
	return d, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
