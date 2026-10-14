// Package store 是服务端的内存存储:把上报的数据攒起来,对外提供查询。
// 数据量控制在内存能承受的范围,靠保留策略 + 定期落盘兜底。
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"apm-system/internal/model"
)

type Config struct {
	TraceRetention  time.Duration // 链路保留时长
	MetricRetention time.Duration // 指标保留时长
	LogCap          int           // 日志最多留多少条
}

func DefaultConfig() Config {
	return Config{
		TraceRetention:  time.Hour,
		MetricRetention: 24 * time.Hour,
		LogCap:          5000,
	}
}

// TraceRecord 是归并之后的链路,Transactions 按开始时间排好序。
type TraceRecord struct {
	TraceID      string              `json:"trace_id"`
	Transactions []model.Transaction `json:"transactions"`
	Start        int64               `json:"start"`
	End          int64               `json:"end"`
}

type metricPoint struct {
	T       int64   `json:"t"`
	V       float64 `json:"v"`
	Count   int64   `json:"count,omitempty"`
	Sum     float64 `json:"sum,omitempty"`
	Buckets []int64 `json:"buckets,omitempty"`
}

type metricSeries struct {
	Name   string            `json:"name"`
	Type   model.MetricType  `json:"type"`
	Labels map[string]string `json:"labels"`
	Bounds []float64         `json:"bounds,omitempty"` // histogram 桶上界,整个序列共用
	Points []metricPoint     `json:"points"`
}

type ServiceMeta struct {
	Name       string  `json:"name"`
	Host       string  `json:"host"`
	Version    string  `json:"version"`
	LastSeen   int64   `json:"last_seen"`
	Requests   int64   `json:"requests"`
	Errors     int64   `json:"errors"`
	TotalDurMS float64 `json:"-"` // 累计耗时,算平均用
}

type Store struct {
	mu       sync.RWMutex
	traces   map[string]*TraceRecord
	metrics  map[string]*metricSeries
	logs     []model.LogEntry
	services map[string]*ServiceMeta
	cfg      Config
}

func New(cfg Config) *Store {
	return &Store{
		traces:   make(map[string]*TraceRecord),
		metrics:  make(map[string]*metricSeries),
		services: make(map[string]*ServiceMeta),
		cfg:      cfg,
	}
}

// Ingest 把一批上报写进存储,返回各类实际写入的条数。
func (s *Store) Ingest(p *model.IngestPayload) (traces, metrics, logs int) {
	now := time.Now().UnixMicro()

	s.mu.Lock()
	defer s.mu.Unlock()

	if p.Service != "" {
		meta := s.services[p.Service]
		if meta == nil {
			meta = &ServiceMeta{Name: p.Service}
			s.services[p.Service] = meta
		}
		meta.Host = p.Host
		meta.Version = p.Version
		meta.LastSeen = now
	}

	for _, tr := range p.Traces {
		if len(tr.Transactions) == 0 {
			continue
		}
		rec := s.traces[tr.TraceID]
		if rec == nil {
			rec = &TraceRecord{TraceID: tr.TraceID}
			s.traces[tr.TraceID] = rec
		}
		for i := range tr.Transactions {
			tx := tr.Transactions[i]
			if tx.StartTime == 0 || tx.Duration <= 0 {
				continue
			}
			// 重试重复上报时,同一个事务只算一次
			dup := false
			for _, exist := range rec.Transactions {
				if exist.Service == tx.Service && exist.Name == tx.Name && exist.StartTime == tx.StartTime {
					dup = true
					break
				}
			}
			if dup {
				continue
			}
			rec.Transactions = append(rec.Transactions, tx)
			traces++
		}
		sort.Slice(rec.Transactions, func(i, j int) bool {
			return rec.Transactions[i].StartTime < rec.Transactions[j].StartTime
		})
		first, last := rec.Transactions[0], rec.Transactions[len(rec.Transactions)-1]
		rec.Start = first.StartTime
		rec.End = last.StartTime + last.Duration

		for _, tx := range rec.Transactions {
			meta := s.services[tx.Service]
			if meta == nil {
				meta = &ServiceMeta{Name: tx.Service}
				s.services[tx.Service] = meta
			}
			meta.Requests++
			meta.TotalDurMS += float64(tx.Duration) / 1000
			if tx.Status == model.StatusError {
				meta.Errors++
			}
			meta.LastSeen = now
		}
	}

	for _, m := range p.Metrics {
		if m.Name == "" {
			continue
		}
		key := metricKey(m.Name, m.Labels)
		ser := s.metrics[key]
		if ser == nil {
			ser = &metricSeries{Name: m.Name, Type: m.Type, Labels: m.Labels, Bounds: m.Bounds}
			s.metrics[key] = ser
		}
		pt := metricPoint{T: now, V: m.Value, Count: m.Count, Sum: m.Sum, Buckets: m.Buckets}
		n := len(ser.Points)
		// 同一个 flush 窗口内重复到的点直接覆盖,免得序列被撑爆
		if n > 0 && now-ser.Points[n-1].T < time.Second.Microseconds() {
			ser.Points[n-1] = pt
		} else {
			ser.Points = append(ser.Points, pt)
		}
		metrics++
	}

	for _, l := range p.Logs {
		if l.Time == 0 {
			l.Time = now
		}
		s.logs = append(s.logs, l)
		logs++
	}
	if len(s.logs) > s.cfg.LogCap {
		s.logs = s.logs[len(s.logs)-s.cfg.LogCap:]
	}
	return
}

// Run 起一个清理协程;配了快照路径的话顺便定期落盘。
func (s *Store) Run(ctx context.Context, snapshotPath string) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.mu.Lock()
			s.purgeLocked(time.Now().UnixMicro())
			s.mu.Unlock()
			if snapshotPath != "" {
				_ = s.Save(snapshotPath)
			}
		}
	}
}

// purgeLocked 清掉超过保留期的链路和指标点,调用方必须持锁。
func (s *Store) purgeLocked(now int64) {
	cutTrace := now - s.cfg.TraceRetention.Microseconds()
	cutMetric := now - s.cfg.MetricRetention.Microseconds()

	for id, rec := range s.traces {
		if rec.End < cutTrace {
			delete(s.traces, id)
		}
	}
	for k, ser := range s.metrics {
		pts := ser.Points
		i := sort.Search(len(pts), func(i int) bool { return pts[i].T >= cutMetric })
		if i >= len(pts) {
			delete(s.metrics, k)
			continue
		}
		if i > 0 {
			ser.Points = append([]metricPoint(nil), pts[i:]...)
		}
	}
}

// ---------- 查询 ----------

type TraceFilter struct {
	Service  string
	Status   string
	Q        string // 按事务名模糊匹配
	MinDurMS float64
	Before   int64 // 游标,只返回开始时间早于它的,做分页
	Limit    int
}

type TraceSummary struct {
	TraceID    string  `json:"trace_id"`
	Service    string  `json:"service"`
	Name       string  `json:"name"`
	Start      int64   `json:"start"`
	DurationMS float64 `json:"duration_ms"`
	Status     string  `json:"status"`
	HTTPStatus int     `json:"http_status"`
	TxCount    int     `json:"tx_count"`
	SpanCount  int     `json:"span_count"`
}

func summarize(rec *TraceRecord) TraceSummary {
	primary := rec.Transactions[0]
	sum := TraceSummary{
		TraceID:    rec.TraceID,
		Service:    primary.Service,
		Name:       primary.Name,
		Start:      rec.Start,
		DurationMS: float64(rec.End-rec.Start) / 1000,
		Status:     model.StatusOK,
		HTTPStatus: primary.HTTPStatus,
	}
	for _, tx := range rec.Transactions {
		if tx.Status == model.StatusError {
			sum.Status = model.StatusError
		}
		sum.SpanCount += len(tx.Spans)
		sum.TxCount++
	}
	return sum
}

func (s *Store) Traces(f TraceFilter) []TraceSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 50
	}
	out := make([]TraceSummary, 0, f.Limit)
	for _, rec := range s.traces {
		sum := summarize(rec)
		if f.Service != "" && sum.Service != f.Service {
			continue
		}
		if f.Status != "" && sum.Status != f.Status {
			continue
		}
		if f.Q != "" && !strings.Contains(sum.Name, f.Q) {
			continue
		}
		if f.MinDurMS > 0 && sum.DurationMS < f.MinDurMS {
			continue
		}
		if f.Before > 0 && sum.Start >= f.Before {
			continue
		}
		out = append(out, sum)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start > out[j].Start })
	if len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out
}

func (s *Store) Trace(id string) (*TraceRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.traces[id]
	if !ok {
		return nil, false
	}
	cp := *rec
	cp.Transactions = append([]model.Transaction(nil), rec.Transactions...)
	return &cp, true
}

func (s *Store) Stats() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return map[string]int{
		"traces":        len(s.traces),
		"metric_series": len(s.metrics),
		"logs":          len(s.logs),
		"services":      len(s.services),
	}
}

// ---------- 总览 ----------

type Overview struct {
	WindowSeconds int                  `json:"window_seconds"`
	GeneratedAt   int64                `json:"generated_at"`
	Totals        OverviewTotals       `json:"totals"`
	Series        OverviewSeries       `json:"series"`
	TopServices   []ServiceStat        `json:"top_services"`
	TopSlow       []TraceSummary       `json:"top_slow"`
	ServiceSeries map[string][]float64 `json:"service_series"` // 按服务拆的请求数曲线,取 top3
}

type OverviewTotals struct {
	Requests  int64   `json:"requests"`
	Errors    int64   `json:"errors"`
	QPS       float64 `json:"qps"`
	ErrorRate float64 `json:"error_rate"`
	AvgMS     float64 `json:"avg_ms"`
	P50MS     float64 `json:"p50_ms"`
	P95MS     float64 `json:"p95_ms"`
	P99MS     float64 `json:"p99_ms"`
	SlowestMS float64 `json:"slowest_ms"`
}

type OverviewSeries struct {
	Buckets   []int64   `json:"buckets"`
	Requests  []float64 `json:"requests"`
	AvgMS     []float64 `json:"avg_ms"`
	P95MS     []float64 `json:"p95_ms"`
	ErrorRate []float64 `json:"error_rate"`
}

type ServiceStat struct {
	Service  string  `json:"service"`
	Requests int64   `json:"requests"`
	Errors   int64   `json:"errors"`
	QPS      float64 `json:"qps"`
	AvgMS    float64 `json:"avg_ms"`
	P95MS    float64 `json:"p95_ms"`
	LastSeen int64   `json:"last_seen"`
}

// Overview 统计一个时间窗口内的总览数据,分桶固定 10s。
func (s *Store) Overview(window time.Duration) Overview {
	const bucketWidth = 10 * time.Second
	if window <= 0 || window > time.Hour {
		window = 15 * time.Minute
	}
	nowUS := time.Now().UnixMicro()
	cutoff := nowUS - window.Microseconds()
	nb := int(window / bucketWidth)
	if nb < 1 {
		nb = 1
	}
	bucketUS := bucketWidth.Microseconds()

	ov := Overview{
		WindowSeconds: int(window / time.Second),
		GeneratedAt:   nowUS,
		Series: OverviewSeries{
			Buckets:   make([]int64, nb),
			Requests:  make([]float64, nb),
			AvgMS:     make([]float64, nb),
			P95MS:     make([]float64, nb),
			ErrorRate: make([]float64, nb),
		},
		ServiceSeries: make(map[string][]float64),
	}
	for i := range ov.Series.Buckets {
		ov.Series.Buckets[i] = cutoff + int64(i)*bucketUS
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var allDur []float64
	var errTotal int64
	bucketDur := make([][]float64, nb)
	bucketErr := make([]int64, nb)
	svcReq := make(map[string][]int64)
	svcDur := make(map[string][]float64)
	svcErr := make(map[string]int64)

	for _, rec := range s.traces {
		if rec.End < cutoff {
			continue
		}
		durMS := float64(rec.End-rec.Start) / 1000
		allDur = append(allDur, durMS)
		idx := int((rec.Start - cutoff) / bucketUS)
		if idx < 0 {
			idx = 0
		}
		if idx >= nb {
			idx = nb - 1
		}
		ov.Series.Requests[idx]++
		bucketDur[idx] = append(bucketDur[idx], durMS)

		status := model.StatusOK
		for _, tx := range rec.Transactions {
			if tx.Status == model.StatusError {
				status = model.StatusError
				break
			}
		}
		if status == model.StatusError {
			bucketErr[idx]++
			errTotal++
		}

		svc := rec.Transactions[0].Service
		if _, ok := svcReq[svc]; !ok {
			svcReq[svc] = make([]int64, nb)
		}
		svcReq[svc][idx]++
		svcDur[svc] = append(svcDur[svc], durMS)
		if status == model.StatusError {
			svcErr[svc]++
		}
	}

	// 全窗口汇总
	ov.Totals.Requests = int64(len(allDur))
	ov.Totals.Errors = errTotal
	if n := len(allDur); n > 0 {
		sort.Float64s(allDur)
		ov.Totals.QPS = float64(n) / float64(ov.WindowSeconds)
		ov.Totals.ErrorRate = float64(errTotal) / float64(n)
		ov.Totals.AvgMS = mean(allDur)
		ov.Totals.P50MS = percentile(allDur, 0.50)
		ov.Totals.P95MS = percentile(allDur, 0.95)
		ov.Totals.P99MS = percentile(allDur, 0.99)
		ov.Totals.SlowestMS = allDur[n-1]
	}

	// 每个分桶的统计
	for i := 0; i < nb; i++ {
		if n := ov.Series.Requests[i]; n > 0 {
			sort.Float64s(bucketDur[i])
			ov.Series.AvgMS[i] = mean(bucketDur[i])
			ov.Series.P95MS[i] = percentile(bucketDur[i], 0.95)
			ov.Series.ErrorRate[i] = float64(bucketErr[i]) / n
		}
	}

	// top 服务:按窗口内请求数排序
	type svcAgg struct {
		name string
		req  int64
		err  int64
		durs []float64
	}
	svcs := make([]svcAgg, 0, len(svcReq))
	for name, req := range svcReq {
		var total int64
		for _, c := range req {
			total += c
		}
		svcs = append(svcs, svcAgg{name: name, req: total, err: svcErr[name], durs: svcDur[name]})
	}
	sort.Slice(svcs, func(i, j int) bool { return svcs[i].req > svcs[j].req })
	for i, a := range svcs {
		if i >= 5 {
			break
		}
		sort.Float64s(a.durs)
		stat := ServiceStat{
			Service:  a.name,
			Requests: a.req,
			Errors:   a.err,
			QPS:      float64(a.req) / float64(ov.WindowSeconds),
			AvgMS:    mean(a.durs),
			P95MS:    percentile(a.durs, 0.95),
		}
		if meta, ok := s.services[a.name]; ok {
			stat.LastSeen = meta.LastSeen
		}
		ov.TopServices = append(ov.TopServices, stat)

		// 请求数曲线,前三个服务才画,多了图会乱
		if i < 3 {
			line := make([]float64, nb)
			for j, c := range svcReq[a.name] {
				line[j] = float64(c)
			}
			ov.ServiceSeries[a.name] = line
		}
	}

	// 最慢的十条链路
	all := make([]TraceSummary, 0, len(allDur))
	for _, rec := range s.traces {
		if rec.End < cutoff {
			continue
		}
		all = append(all, summarize(rec))
	}
	sort.Slice(all, func(i, j int) bool { return all[i].DurationMS > all[j].DurationMS })
	if len(all) > 10 {
		all = all[:10]
	}
	ov.TopSlow = all

	return ov
}

// ---------- 指标 ----------

func metricKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	b.WriteByte('}')
	return b.String()
}

type MetricNameInfo struct {
	Name   string            `json:"name"`
	Type   model.MetricType  `json:"type"`
	Labels map[string]string `json:"labels"`
}

// MetricNames 返回所有指标名,labels 取第一个序列做代表。
func (s *Store) MetricNames() []MetricNameInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	seen := make(map[string]bool)
	out := make([]MetricNameInfo, 0, len(s.metrics))
	for _, ser := range s.metrics {
		if seen[ser.Name] {
			continue
		}
		seen[ser.Name] = true
		out = append(out, MetricNameInfo{Name: ser.Name, Type: ser.Type, Labels: ser.Labels})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

type MetricQuery struct {
	Names    []string
	Since    int64   // 起点 unix 微秒,0 表示全部
	Rate     bool    // counter 换算成每秒速率
	Quantile float64 // histogram 分位数,比如 0.95
}

type MetricSeriesOut struct {
	Name   string            `json:"name"`
	Type   model.MetricType  `json:"type"`
	Labels map[string]string `json:"labels"`
	Points [][2]float64      `json:"points"` // [时间微秒, 值]
}

func (s *Store) Metrics(q MetricQuery) []MetricSeriesOut {
	s.mu.RLock()
	defer s.mu.RUnlock()

	want := make(map[string]bool, len(q.Names))
	for _, n := range q.Names {
		want[n] = true
	}

	out := make([]MetricSeriesOut, 0, len(q.Names))
	for _, ser := range s.metrics {
		if len(want) > 0 && !want[ser.Name] {
			continue
		}
		pts := ser.Points
		if q.Since > 0 {
			i := sort.Search(len(pts), func(i int) bool { return pts[i].T >= q.Since })
			pts = pts[i:]
		}
		if len(pts) == 0 {
			continue
		}

		// 显式初始化,避免 JSON 序列化出 null
		item := MetricSeriesOut{
			Name:   ser.Name,
			Type:   ser.Type,
			Labels: ser.Labels,
			Points: make([][2]float64, 0, len(pts)),
		}

		switch {
		case ser.Type == model.MetricHistogram && q.Quantile > 0:
			// 相邻点做差分,得到每个上报窗口内的分布,再在分布上算分位数
			for i, p := range pts {
				if i == 0 {
					item.Points = append(item.Points, [2]float64{float64(p.T), quantileFromBuckets(p.Buckets, ser.Bounds, q.Quantile)})
					continue
				}
				prev := pts[i-1]
				item.Points = append(item.Points, [2]float64{float64(p.T), quantileDelta(prev.Buckets, p.Buckets, ser.Bounds, q.Quantile)})
			}
		case ser.Type == model.MetricCounter && q.Rate:
			// 第一个点算不出速率,丢掉
			for i := 1; i < len(pts); i++ {
				dt := float64(pts[i].T-pts[i-1].T) / 1e6
				if dt <= 0 {
					continue
				}
				v := (pts[i].V - pts[i-1].V) / dt
				if v < 0 { // 计数器被重置过,这个窗口的速率没法算
					v = 0
				}
				item.Points = append(item.Points, [2]float64{float64(pts[i].T), v})
			}
		default:
			for _, p := range pts {
				item.Points = append(item.Points, [2]float64{float64(p.T), p.V})
			}
		}
		out = append(out, item)
	}
	return out
}

// quantileFromBuckets 在一组累计桶上算分位数,桶内做线性插值。
func quantileFromBuckets(buckets []int64, bounds []float64, q float64) float64 {
	if len(buckets) == 0 || len(bounds) != len(buckets) {
		return 0
	}
	// buckets 是累计计数,总数就是最后一个桶的值
	total := buckets[len(buckets)-1]
	if total == 0 {
		return 0
	}
	target := q * float64(total)
	var cum int64
	for i, c := range buckets {
		cum += c
		if float64(cum) >= target && c > 0 {
			prevBound := 0.0
			if i > 0 {
				prevBound = bounds[i-1]
			}
			frac := (target - float64(cum-c)) / float64(c)
			return prevBound + frac*(bounds[i]-prevBound)
		}
	}
	// 全部落在最后一个桶外面,只能给上界
	return bounds[len(bounds)-1]
}

func quantileDelta(prevBuckets, buckets []int64, bounds []float64, q float64) float64 {
	if len(prevBuckets) != len(buckets) || len(buckets) != len(bounds) {
		return quantileFromBuckets(buckets, bounds, q)
	}
	delta := make([]int64, len(buckets))
	var reset bool
	for i := range buckets {
		if buckets[i] < prevBuckets[i] {
			reset = true // 进程重启,计数器从头开始
			break
		}
		delta[i] = buckets[i] - prevBuckets[i]
	}
	if reset {
		return quantileFromBuckets(buckets, bounds, q)
	}
	return quantileFromBuckets(delta, bounds, q)
}

// ---------- 服务 ----------

func (s *Store) Services() []ServiceMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]ServiceMeta, 0, len(s.services))
	for _, m := range s.services {
		item := *m
		if m.Requests > 0 {
			item.TotalDurMS = m.TotalDurMS / float64(m.Requests) // 复用字段存平均耗时
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Requests > out[j].Requests })
	return out
}

// ---------- 日志 ----------

type LogFilter struct {
	Level   string
	Service string
	Q       string
	Limit   int
}

func (s *Store) Logs(f LogFilter) []model.LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if f.Limit <= 0 || f.Limit > 1000 {
		f.Limit = 200
	}
	out := make([]model.LogEntry, 0, f.Limit)
	for i := len(s.logs) - 1; i >= 0 && len(out) < f.Limit; i-- {
		l := s.logs[i]
		if f.Level != "" && l.Level != f.Level {
			continue
		}
		if f.Service != "" && l.Service != f.Service {
			continue
		}
		if f.Q != "" && !strings.Contains(l.Message, f.Q) {
			continue
		}
		out = append(out, l)
	}
	return out
}

// ---------- 快照持久化 ----------

type snapshot struct {
	SavedAt  int64                    `json:"saved_at"`
	Traces   map[string]*TraceRecord  `json:"traces"`
	Metrics  map[string]*metricSeries `json:"metrics"`
	Logs     []model.LogEntry         `json:"logs"`
	Services map[string]*ServiceMeta  `json:"services"`
}

func (s *Store) Save(path string) error {
	s.mu.RLock()
	snap := snapshot{
		SavedAt:  time.Now().UnixMicro(),
		Traces:   s.traces,
		Metrics:  s.metrics,
		Logs:     s.logs,
		Services: s.services,
	}
	s.mu.RUnlock()

	data, err := json.Marshal(&snap)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Load 启动时恢复快照,文件不存在不算错。
func (s *Store) Load(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("corrupted snapshot: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if snap.Traces != nil {
		s.traces = snap.Traces
	}
	if snap.Metrics != nil {
		s.metrics = snap.Metrics
	}
	if snap.Logs != nil {
		s.logs = snap.Logs
	}
	if snap.Services != nil {
		s.services = snap.Services
	}
	s.purgeLocked(time.Now().UnixMicro())
	return nil
}

// ---------- 小工具 ----------

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}

// percentile 用最近秩法,xs 需要已排序。
func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(xs)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(xs) {
		idx = len(xs) - 1
	}
	return xs[idx]
}
