// Package server 是 apm-server 的 HTTP 层:接收上报、提供查询 API、
// 顺便把内嵌的 Web 仪表盘吐给浏览器。
package server

import (
	"embed"
	"io/fs"
	"log"
	"net/http"
	"time"

	"apm-system/internal/agent"
	"apm-system/internal/store"
)

//go:embed web
var webFS embed.FS

type Server struct {
	store     *store.Store
	logger    *log.Logger
	startedAt time.Time
	version   string
	// 自监控的 agent,非 nil 时所有请求都会带上埋点中间件
	selfAgent *agent.Agent
}

func New(st *store.Store, selfAgent *agent.Agent, logger *log.Logger, version string) *Server {
	return &Server{
		store:     st,
		logger:    logger,
		startedAt: time.Now(),
		version:   version,
		selfAgent: selfAgent,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// 写入接口
	mux.HandleFunc("POST /api/v1/ingest", s.handleIngest)

	// 查询接口
	mux.HandleFunc("GET /api/v1/overview", s.handleOverview)
	mux.HandleFunc("GET /api/v1/traces", s.handleTraces)
	mux.HandleFunc("GET /api/v1/traces/{id}", s.handleTraceDetail)
	mux.HandleFunc("GET /api/v1/services", s.handleServices)
	mux.HandleFunc("GET /api/v1/metrics", s.handleMetrics)
	mux.HandleFunc("GET /api/v1/metrics/names", s.handleMetricNames)
	mux.HandleFunc("GET /api/v1/logs", s.handleLogs)

	mux.HandleFunc("GET /healthz", s.handleHealth)

	// 静态仪表盘兜底
	web, err := fs.Sub(webFS, "web")
	if err != nil {
		// embed 的内容在编译期就固定了,这里理论上走不到
		panic(err)
	}
	mux.Handle("/", http.FileServer(http.FS(web)))

	var h http.Handler = mux
	if s.selfAgent != nil {
		h = s.selfAgent.Middleware(h)
	}
	return h
}
