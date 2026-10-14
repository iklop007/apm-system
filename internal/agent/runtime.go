package agent

import (
	"context"
	"runtime"
	rtmetrics "runtime/metrics"
	"time"
)

// collectRuntime 周期性采集进程自身的运行指标:
// goroutine 数、堆内存、GC 暂停、累计 CPU 秒数。
// CPU 秒数用 runtime/metrics 拿,不依赖 cgo,跨平台。
func (a *Agent) collectRuntime(ctx context.Context) {
	svc := a.Labels()
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()

	var mem runtime.MemStats
	cpuSamples := []rtmetrics.Sample{
		{Name: "/cpu/classes/user:cpu-seconds"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
	}
	var lastCPU float64

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			runtime.ReadMemStats(&mem)

			a.metrics.Gauge("go_goroutines", svc).Set(float64(runtime.NumGoroutine()))
			a.metrics.Gauge("go_heap_alloc_mb", svc).Set(float64(mem.HeapAlloc) / 1e6)
			a.metrics.Gauge("go_heap_inuse_mb", svc).Set(float64(mem.HeapInuse) / 1e6)
			a.metrics.Gauge("go_heap_objects", svc).Set(float64(mem.HeapObjects))
			a.metrics.Gauge("go_gc_count_total", svc).Set(float64(mem.NumGC))
			if mem.NumGC > 0 {
				// PauseNs 是个环形缓冲,最新一次 GC 的暂停在 (NumGC-1) 位置
				idx := (mem.NumGC - 1) % 256
				a.metrics.Gauge("go_gc_pause_ms", svc).Set(float64(mem.PauseNs[idx]) / 1e6)
			}

			rtmetrics.Read(cpuSamples)
			var cpu float64
			for _, s := range cpuSamples {
				if s.Value.Kind() != rtmetrics.KindBad {
					cpu += s.Value.Float64()
				}
			}
			if cpu >= lastCPU {
				a.metrics.Counter("go_cpu_seconds_total", svc).Add(cpu - lastCPU)
				lastCPU = cpu
			}
		}
	}
}
