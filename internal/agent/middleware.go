package agent

import (
	"net/http"
	"strconv"
	"time"

	"apm-system/internal/model"
)

// statusWriter 包一层 ResponseWriter,把实际写出去的 status code 记下来。
// 很多 handler 不显式调 WriteHeader,默认 200 也要能拿到。
type statusWriter struct {
	http.ResponseWriter
	code  int
	wrote bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wrote {
		w.code = code
		w.wrote = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.code = http.StatusOK
		w.wrote = true
	}
	return w.ResponseWriter.Write(b)
}

// Middleware 自动为每个 http 请求生成事务和请求指标:
//   - 事务:链路 + span,状态码 >= 400 记为 error
//   - 指标:http_requests_total(按路由/状态码)、http_request_duration_ms、http_active_requests
func (a *Agent) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ctx, tx := a.StartTransactionFromRequest(r)
		r = r.WithContext(ctx)

		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		active := a.metrics.Gauge("http_active_requests", a.Labels())
		active.Add(1)
		defer active.Sub(1)

		next.ServeHTTP(sw, r)

		tx.SetHTTPStatus(sw.code)
		if sw.code >= 400 {
			tx.SetStatus(model.StatusError)
			a.LogContextf(ctx, "error", "%s %s -> %d", r.Method, r.URL.Path, sw.code)
		}
		tx.End()

		durMS := float64(time.Since(start).Microseconds()) / 1000
		route := r.Pattern
		if route == "" {
			route = r.URL.Path
		}
		a.metrics.Counter("http_requests_total", map[string]string{
			"service": a.cfg.Service, "route": route, "status": strconv.Itoa(sw.code),
		}).Inc()
		a.metrics.Histogram("http_request_duration_ms", map[string]string{
			"service": a.cfg.Service, "route": route,
		}).Observe(durMS)
	})
}
