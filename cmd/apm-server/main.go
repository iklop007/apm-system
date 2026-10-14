// apm-server:接收 agent 上报、存储、查询 API、内嵌仪表盘。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"apm-system/internal/agent"
	"apm-system/internal/server"
	"apm-system/internal/store"
	"apm-system/internal/version"
)

func main() {
	var (
		addr      = flag.String("addr", ":8080", "监听地址")
		dataDir   = flag.String("data", "./data", "数据快照目录,启动时恢复、定期落盘")
		svcName   = flag.String("service", "apm-server", "自监控上报用的服务名")
		retTrace  = flag.Duration("retention-trace", time.Hour, "链路数据保留时长")
		retMetric = flag.Duration("retention-metric", 24*time.Hour, "指标数据保留时长")
	)
	flag.Parse()

	logger := log.New(os.Stdout, "[apm-server] ", log.LstdFlags)
	logger.Printf("apm-server v%s, listening on %s", version.Version, *addr)

	st := store.New(store.Config{
		TraceRetention:  *retTrace,
		MetricRetention: *retMetric,
		LogCap:          5000,
	})

	// 恢复上次的快照(存在才恢复)
	snapshotPath := filepath.Join(*dataDir, "snapshot.json")
	if _, err := os.Stat(snapshotPath); err == nil {
		if err := st.Load(snapshotPath); err != nil {
			logger.Printf("恢复快照失败,忽略: %v", err)
		} else {
			stats := st.Stats()
			logger.Printf("已从快照恢复: %d 条链路, %d 个指标序列, %d 条日志",
				stats["traces"], stats["metric_series"], stats["logs"])
		}
	}

	// 自监控:server 自己也是 agent,把自身流量和运行指标上报给自己,
	// 打开仪表盘就能看到 apm-server 这个服务
	selfAgent := agent.New(agent.Config{
		Service:   *svcName,
		ServerURL: ingestURL(*addr),
		Version:   version.Version,
		Logger:    log.New(os.Stdout, "[apm-self] ", log.LstdFlags),
	})
	selfAgent.Start()

	srv := server.New(st, selfAgent, logger, version.Version)
	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 定期清理过期数据 + 落盘
	go st.Run(ctx, snapshotPath)

	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Printf("收到退出信号,保存数据...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	httpSrv.Shutdown(shutdownCtx)
	selfAgent.Close()
	if err := st.Save(snapshotPath); err != nil {
		logger.Printf("保存快照失败: %v", err)
	} else {
		logger.Printf("数据已保存到 %s,bye", snapshotPath)
	}
}

// ingestURL 把监听地址拼成自监控上报用的完整 URL。
func ingestURL(addr string) string {
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	return fmt.Sprintf("http://%s/api/v1/ingest", addr)
}
