// demo-app:模拟的订单服务,用来演示 agent 的埋点效果。
// 接口里有快有慢、有错误有跨进程调用,数据量足够把仪表盘喂起来。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"apm-system/internal/agent"
	"apm-system/internal/version"
)

var (
	a             *agent.Agent
	svcName       string
	queueDepth    atomic.Int64
	ordersCreated atomic.Int64
)

func main() {
	var (
		addr      = flag.String("addr", ":8081", "监听地址")
		serverURL = flag.String("server", "http://127.0.0.1:8080/api/v1/ingest", "apm-server 上报地址")
		service   = flag.String("service", "order-service", "服务名")
	)
	flag.Parse()
	svcName = *service

	a = agent.New(agent.Config{
		Service:   svcName,
		ServerURL: *serverURL,
		Version:   version.Version,
	})
	a.Start()
	defer a.Close()
	a.Logf("info", "demo app started on %s", *addr)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/orders", handleListOrders)
	mux.HandleFunc("POST /api/orders", handleCreateOrder)
	mux.HandleFunc("GET /api/products", handleProducts)
	mux.HandleFunc("POST /api/checkout", handleCheckout)
	mux.HandleFunc("GET /api/outbound", handleOutbound)

	// 后台 worker,模拟消费队列
	go workerLoop()

	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           a.Middleware(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("demo app listening on %s", *addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("demo app error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	log.Printf("demo app shutting down")
	httpSrv.Shutdown(context.Background())
}

/* ---------- 业务接口 ---------- */

func handleListOrders(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orders := dbQuery(ctx, "SELECT id, amount FROM orders ORDER BY id DESC LIMIT 20")
	writeJSON(w, http.StatusOK, map[string]any{"orders": orders, "count": len(orders)})
}

func handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req struct {
		UserID int     `json:"user_id"`
		Items  []int   `json:"items"`
		Amount float64 `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Items) == 0 {
		http.Error(w, "bad request: need items", http.StatusBadRequest)
		return
	}

	orderID := 50000 + rand.Intn(10000)
	dbWrite(ctx, "INSERT INTO orders (user_id, amount) VALUES (...)")

	_, span := agent.StartSpan(ctx, "cache.set order", "cache")
	if span != nil {
		defer span.End()
		span.SetTag("cache.key", "order:"+strconv.Itoa(orderID))
	}
	sleep(1, 5)

	publishEvent(ctx, "order.created")

	ordersCreated.Add(1)
	a.Metrics().Counter("orders_created_total", a.Labels()).Inc()
	a.LogContextf(ctx, "info", "order %d created, user=%d amount=%.2f", orderID, req.UserID, req.Amount)

	writeJSON(w, http.StatusCreated, map[string]any{"order_id": orderID, "items": len(req.Items)})
}

func handleProducts(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	hit := cacheGet(ctx, "products:all")
	if !hit {
		// 缓存没命中,回源查库再回填
		dbQuery(ctx, "SELECT id, name, price FROM products")
		cacheSet(ctx, "products:all")
	}

	products := make([]map[string]any, 0, 8)
	for i := 0; i < 8; i++ {
		products = append(products, map[string]any{
			"id": 100 + i, "name": fmt.Sprintf("商品-%d", i),
			"price": 19.9 + rand.Float64()*80,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"products": products, "cache_hit": hit})
}

func handleCheckout(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	dbQuery(ctx, "SELECT cart FROM carts WHERE user_id = $1")
	dbWrite(ctx, "INSERT INTO orders (...) VALUES (...)")

	// 5% 的概率撞上一次慢查询
	if rand.Float64() < 0.05 {
		_, span := agent.StartSpan(ctx, "db.slow_query", "db")
		if span != nil {
			defer span.End()
			span.SetTag("db.slow", "true")
			span.SetTag("db.plan", "seq scan")
		}
		sleep(500, 1200)
		a.LogContextf(ctx, "warn", "checkout hit a slow query")
	}

	dbWrite(ctx, "UPDATE inventory SET stock = stock - 1 WHERE ...")

	// 2% 的概率支付网关超时
	if rand.Float64() < 0.02 {
		a.LogContextf(ctx, "error", "payment gateway timeout, checkout failed")
		a.Metrics().Counter("checkout_failures_total", a.Labels()).Inc()
		http.Error(w, "payment gateway timeout", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"result": "ok", "order_id": 60000 + rand.Intn(9999)})
}

// handleOutbound 演示跨进程链路传播:带着 trace 头再调一次本服务的接口,
// 仪表盘里能看到两个事务挂在同一条链路下。
func handleOutbound(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	ctx, span := agent.StartSpan(ctx, "http.call GET /api/products", "client")
	if span != nil {
		defer span.End()
		span.SetTag("http.remote", "self")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+r.Host+"/api/products", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	agent.Inject(ctx, req.Header)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if span != nil {
			span.SetStatus("error")
		}
		http.Error(w, "downstream call failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	var body map[string]any
	json.NewDecoder(resp.Body).Decode(&body)
	writeJSON(w, http.StatusOK, map[string]any{
		"downstream": "/api/products",
		"status":     resp.StatusCode,
		"products":   body["products"],
	})
}

/* ---------- 模拟的存储与中间件 ---------- */

func dbQuery(ctx context.Context, sql string) []int {
	_, span := agent.StartSpan(ctx, "db.query", "db")
	if span != nil {
		defer span.End()
		span.SetTag("db.system", "postgres")
		span.SetTag("db.statement", sql)
	}
	sleep(5, 50)
	rows := make([]int, rand.Intn(20))
	for i := range rows {
		rows[i] = 1000 + rand.Intn(9000)
	}
	return rows
}

func dbWrite(ctx context.Context, sql string) {
	_, span := agent.StartSpan(ctx, "db.execute", "db")
	if span != nil {
		defer span.End()
		span.SetTag("db.system", "postgres")
		span.SetTag("db.statement", sql)
	}
	sleep(10, 80)
}

func cacheGet(ctx context.Context, key string) bool {
	_, span := agent.StartSpan(ctx, "cache.get", "cache")
	if span != nil {
		defer span.End()
		span.SetTag("cache.key", key)
	}
	sleep(1, 10)
	hit := rand.Float64() < 0.9
	a.Metrics().Counter("cache_ops_total", a.Labels()).Inc()
	if hit {
		a.Metrics().Counter("cache_hits_total", a.Labels()).Inc()
	}
	return hit
}

func cacheSet(ctx context.Context, key string) {
	_, span := agent.StartSpan(ctx, "cache.set", "cache")
	if span != nil {
		defer span.End()
		span.SetTag("cache.key", key)
	}
	sleep(1, 5)
}

func publishEvent(ctx context.Context, event string) {
	_, span := agent.StartSpan(ctx, "queue.publish "+event, "internal")
	if span != nil {
		defer span.End()
		span.SetTag("queue", "orders-events")
	}
	sleep(5, 20)
}

// workerLoop 模拟后台任务,每 2 秒处理一批队列消息,给"非 http 事务"做演示。
func workerLoop() {
	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		processQueueBatch()
	}
}

func processQueueBatch() {
	ctx, tx := a.StartTransaction(context.Background(), "worker.process_queue")
	defer tx.End()

	_, deq := agent.StartSpan(ctx, "worker.dequeue", "internal")
	if deq != nil {
		defer deq.End()
	}
	queueDepth.Store(int64(5 + rand.Intn(30)))

	n := 1 + rand.Intn(4)
	for i := 0; i < n; i++ {
		_, s := agent.StartSpan(ctx, "worker.handle_item", "internal")
		sleep(5, 40)
		if s != nil {
			s.End()
		}
	}
	queueDepth.Store(queueDepth.Load() - int64(n))

	a.Metrics().Gauge("queue_depth", a.Labels()).Set(float64(queueDepth.Load()))
	if rand.Float64() < 0.15 {
		a.LogContextf(ctx, "warn", "queue backlog=%d, processing slowly", queueDepth.Load())
	}
}

func sleep(minMs, maxMs int) {
	time.Sleep(time.Duration(minMs+rand.Intn(maxMs-minMs+1)) * time.Millisecond)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
