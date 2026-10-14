package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"apm-system/internal/model"
)

// Reporter 把三类数据攒在本地缓冲里,按固定周期批量 POST 出去。
// 上报失败只影响监控,不影响业务,所以重试几次还不行就丢。
type Reporter struct {
	cfg    Config
	client *http.Client
	reg    *Registry

	mu      sync.Mutex
	pending pendingBuf
}

type pendingBuf struct {
	traces  []model.Trace
	metrics []model.Metric
	logs    []model.LogEntry
}

func (b *pendingBuf) empty() bool {
	return len(b.traces) == 0 && len(b.metrics) == 0 && len(b.logs) == 0
}

func newReporter(cfg Config, reg *Registry) *Reporter {
	return &Reporter{
		cfg: cfg,
		reg: reg,
		client: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        4,
				IdleConnTimeout:     30 * time.Second,
				TLSHandshakeTimeout: 5 * time.Second,
			},
		},
	}
}

func (r *Reporter) AddTrace(t model.Trace) {
	r.mu.Lock()
	r.pending.traces = append(r.pending.traces, t)
	r.mu.Unlock()
}

func (r *Reporter) AddMetrics(ms []model.Metric) {
	if len(ms) == 0 {
		return
	}
	r.mu.Lock()
	r.pending.metrics = append(r.pending.metrics, ms...)
	r.mu.Unlock()
}

func (r *Reporter) AddLog(e model.LogEntry) {
	r.mu.Lock()
	r.pending.logs = append(r.pending.logs, e)
	r.mu.Unlock()
}

func (r *Reporter) run(ctx context.Context) {
	t := time.NewTicker(r.cfg.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.flush()
		}
	}
}

func (r *Reporter) flush() {
	r.mu.Lock()
	// 指标是快照型的,每次 flush 直接拉注册表当前值,不占用 pending 缓冲
	ms := r.reg.snapshot()
	if r.pending.empty() && len(ms) == 0 {
		r.mu.Unlock()
		return
	}
	p := r.pending
	p.metrics = append(p.metrics, ms...)
	r.pending = pendingBuf{}
	r.mu.Unlock()

	payload := model.IngestPayload{
		Service: r.cfg.Service,
		Host:    r.cfg.Host,
		Version: r.cfg.Version,
		Traces:  p.traces,
		Metrics: p.metrics,
		Logs:    p.logs,
	}
	body, err := json.Marshal(&payload)
	if err != nil {
		r.cfg.Logger.Printf("marshal payload: %v", err)
		return
	}

	var lastErr error
	for i := 0; i < 3; i++ {
		if i > 0 {
			time.Sleep(time.Duration(i) * time.Second)
		}
		req, err := http.NewRequest(http.MethodPost, r.cfg.ServerURL, bytes.NewReader(body))
		if err != nil {
			lastErr = err
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := r.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			return
		}
		lastErr = fmt.Errorf("server returned %d", resp.StatusCode)
	}
	r.cfg.Logger.Printf("report failed after retries: %v, dropped %d traces %d metrics %d logs",
		lastErr, len(p.traces), len(p.metrics), len(p.logs))
}
