# apm-system

一个用 Go 写的 APM 系统,零第三方依赖,`go build` 一把过。

```
┌──────────────┐   trace/metrics/logs   ┌───────────────┐
│  业务进程     │ ── 5s 批量 HTTP 上报 ──▶ │  apm-server   │
│ (agent SDK)  │                        │  内存存储      │
│ demo-app     │                        │  查询 API      │
└──────────────┘                        │  内嵌仪表盘    │
                                        │  定期快照落盘  │
                                        └───────┬───────┘
                                                │ data/snapshot.json
                                                ▼
                                         重启后自动恢复
```

## 快速开始

```bash
make build
./bin/apm-server        # 终端 1,默认 :8080
./bin/demo-app          # 终端 2,默认 :8081,自带模拟流量
```

浏览器打开 <http://127.0.0.1:8080> 就能看到仪表盘。demo-app 是一个模拟的
订单服务,接口有快有慢、有错误、有跨进程调用,几分钟内就能把各个页面喂满。

## 功能

**agent 侧(埋点 SDK)**

- 事务 + span 埋点,支持 db / cache / client / internal 等 span 类型
- 跨进程链路传播:outgoing 请求带上 `X-Apm-Trace-Id` / `X-Apm-Span-Id`,
  下游事务会归并到同一条链路
- 一行代码接入 http 服务:`http.Server{Handler: agent.Middleware(mux)}`,
  自动生成事务、请求计数、延迟直方图和活跃请求数
- counter / gauge / histogram 三类指标,histogram 桶内线性插值算分位数
- 结构化日志,自动关联 trace id
- 进程自监控:goroutine 数、堆内存、GC 暂停、CPU 秒数(runtime/metrics,不依赖 cgo)
- 异步批量上报,5s 一个周期,失败重试三次后丢弃,绝不阻塞业务

**服务端**

- 链路按 trace_id 归并,同一链路多个事务(跨服务)合并存储
- 总览聚合:QPS / 平均 / P50 / P95 / P99 / 错误率,10s 分桶时间序列
- counter 差值算速率,histogram 按桶差分算窗口内分位数
- 保留策略:链路默认 1 小时,指标 24 小时,日志环形缓冲 5000 条
- 快照落盘:每 30s 一次 + 退出时保存,重启自动恢复
- 自监控:server 自己也是 agent,自己的流量和运行指标同样上报

**仪表盘(内嵌,无 CDN 依赖)**

- 总览:状态卡 + 延迟/请求/错误率趋势图 + Top 服务 + 最慢链路
- 链路:按服务/状态/耗时筛选,瀑布图展示 span 分布
- 指标:按名称查看,counter 可切速率,histogram 可切 p50/p95/p99
- 日志、服务列表

## 配置

apm-server:

| 参数 | 默认 | 说明 |
|---|---|---|
| `-addr` | `:8080` | 监听地址 |
| `-data` | `./data` | 快照目录 |
| `-service` | `apm-server` | 自监控上报的服务名 |
| `-retention-trace` | `1h` | 链路保留时长 |
| `-retention-metric` | `24h` | 指标保留时长 |

demo-app:

| 参数 | 默认 | 说明 |
|---|---|---|
| `-addr` | `:8081` | 监听地址 |
| `-server` | `http://127.0.0.1:8080/api/v1/ingest` | 上报地址 |
| `-service` | `order-service` | 服务名 |

## 埋点示例

```go
a := agent.New(agent.Config{
    Service:   "my-service",
    ServerURL: "http://127.0.0.1:8080/api/v1/ingest",
})
a.Start()
defer a.Close()

mux := http.NewServeMux()
mux.HandleFunc("GET /api/users", func(w http.ResponseWriter, r *http.Request) {
    ctx, span := agent.StartSpan(r.Context(), "db.query users", "db")
    defer span.End()
    span.SetTag("db.system", "postgres")

    a.Metrics().Counter("users_fetched_total", a.Labels()).Inc()
    a.LogContextf(ctx, "info", "users listed")
    // ...
})
http.ListenAndServe(":8082", a.Middleware(mux))
```

定时任务没有 http 请求,用 `a.StartTransaction(ctx, name)` 手动开事务即可,
demo-app 里的 `processQueueBatch` 是个现成例子。

## API

| 接口 | 说明 |
|---|---|
| `POST /api/v1/ingest` | agent 批量上报 |
| `GET /api/v1/overview?window=15m` | 总览聚合 |
| `GET /api/v1/traces?service=&status=&q=&min_duration_ms=&before=&limit=` | 链路列表(游标分页) |
| `GET /api/v1/traces/{id}` | 链路详情 |
| `GET /api/v1/services` | 服务列表 |
| `GET /api/v1/metrics?names=a,b&rate=true&quantile=0.95&since=` | 指标序列 |
| `GET /api/v1/metrics/names` | 指标清单 |
| `GET /api/v1/logs?level=&service=&q=&limit=` | 日志 |
| `GET /healthz` | 健康检查 |

## 开发

```bash
make test     # go test ./...
make run      # 编译并启动 apm-server
make run-demo # 编译并启动 demo-app
```

代码结构:

```
cmd/apm-server/    服务入口
cmd/demo-app/      模拟业务 + 埋点示例
internal/agent/    埋点 SDK(事务/指标/上报/中间件/自监控)
internal/store/    内存存储 + 聚合查询 + 快照
internal/server/   HTTP API + 内嵌仪表盘(web/)
internal/model/    数据结构
```

两个已知的取舍,写在这里免得踩坑:

- 存储是单机内存 + JSON 快照,单实例够用,上规模要换时序库;
- 跨进程传播头是自定义的 `X-Apm-*`,没实现 w3c traceparent,接入存量系统时
  改 `internal/agent/tracer.go` 里的两个常量即可。
