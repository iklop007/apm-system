// Package agent 是埋点 SDK。业务进程引入它之后,流量、指标、日志
// 会统一异步上报到 apm-server,对业务本身的侵入很小。
package agent

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"apm-system/internal/model"
)

type Config struct {
	Service   string // 服务名,必填
	ServerURL string // apm-server 的 ingest 地址
	// 以下都有默认值,可以不填
	Host          string
	Version       string
	FlushInterval time.Duration
	Logger        *log.Logger
}

type Agent struct {
	cfg      Config
	metrics  *Registry
	reporter *Reporter
	tracer   *Tracer
	logger   *log.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func New(cfg Config) *Agent {
	if cfg.Host == "" {
		cfg.Host, _ = os.Hostname()
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = log.New(os.Stderr, "[apm-agent] ", log.LstdFlags)
	}
	ctx, cancel := context.WithCancel(context.Background())
	a := &Agent{
		cfg:    cfg,
		logger: cfg.Logger,
		ctx:    ctx,
		cancel: cancel,
	}
	a.metrics = newRegistry()
	a.reporter = newReporter(cfg, a.metrics)
	a.tracer = &Tracer{agent: a}
	return a
}

// Start 起后台协程:周期性上报 + 采集自身运行指标。一般 New 完就调。
func (a *Agent) Start() {
	a.wg.Add(2)
	go func() {
		defer a.wg.Done()
		a.reporter.run(a.ctx)
	}()
	go func() {
		defer a.wg.Done()
		a.collectRuntime(a.ctx)
	}()
	a.logger.Printf("agent started, service=%s server=%s", a.cfg.Service, a.cfg.ServerURL)
}

// Close 停掉后台协程,把缓冲里的数据全部发完再退出。
func (a *Agent) Close() {
	a.cancel()
	a.wg.Wait()
	a.reporter.flush()
}

func (a *Agent) Metrics() *Registry { return a.metrics }

// Labels 返回带 service 标签的 map,业务代码打自定义指标时直接用它。
func (a *Agent) Labels() map[string]string {
	return map[string]string{"service": a.cfg.Service}
}

func (a *Agent) LogContextf(ctx context.Context, level, format string, args ...any) {
	e := model.LogEntry{
		Time:    time.Now().UnixMicro(),
		Level:   level,
		Service: a.cfg.Service,
		Message: fmt.Sprintf(format, args...),
	}
	if tx := TransactionFromContext(ctx); tx != nil {
		e.TraceID = tx.TraceID()
	}
	a.reporter.AddLog(e)
}

func (a *Agent) Logf(level, format string, args ...any) {
	a.LogContextf(context.Background(), level, format, args...)
}
