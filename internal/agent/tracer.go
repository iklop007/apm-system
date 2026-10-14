package agent

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"apm-system/internal/ids"
	"apm-system/internal/model"
)

type ctxKey int

const (
	txKey ctxKey = iota
	spanKey
)

// 跨进程传播用的请求头。兼容不了业界的 w3c traceparent 格式,
// 自己定义一套,反正 agent 和 server 是配套的。
const (
	headerTraceID = "X-Apm-Trace-Id"
	headerSpanID  = "X-Apm-Span-Id"
)

type Tracer struct {
	agent *Agent
}

type Transaction struct {
	agent   *Agent
	started time.Time
	root    *Span
	data    model.Transaction

	mu    sync.Mutex
	spans []*Span
	ended atomic.Bool
}

type Span struct {
	tx      *Transaction
	started time.Time
	data    model.Span

	mu    sync.Mutex
	ended bool
}

// startTransaction 是底层入口,traceID/spanID 用于继承上游链路,空的话自建。
func (a *Agent) startTransaction(parent context.Context, name, traceID, parentSpanID string) (context.Context, *Transaction) {
	tx := &Transaction{
		agent:   a,
		started: time.Now(),
		data: model.Transaction{
			Service:    a.cfg.Service,
			Name:       name,
			TraceID:    traceID,
			StartTime:  time.Now().UnixMicro(),
			Status:     model.StatusOK,
			HTTPStatus: http.StatusOK,
		},
	}
	if tx.data.TraceID == "" {
		tx.data.TraceID = ids.NewTraceID()
	}
	tx.root = &Span{
		tx:      tx,
		started: tx.started,
		data: model.Span{
			SpanID:   ids.NewSpanID(),
			ParentID: parentSpanID,
			Name:     name,
			Kind:     "server",
			Status:   model.StatusOK,
		},
	}
	ctx := context.WithValue(parent, txKey, tx)
	ctx = context.WithValue(ctx, spanKey, tx.root)
	return ctx, tx
}

// StartTransaction 手动开一个事务,适合定时任务、消息消费这类非 http 场景。
func (a *Agent) StartTransaction(parent context.Context, name string) (context.Context, *Transaction) {
	traceID, spanID := traceInfoFromCtx(parent)
	return a.startTransaction(parent, name, traceID, spanID)
}

// StartTransactionFromRequest 从 http 请求开事务,Middleware 内部用它。
// 请求头里带了上游链路信息的话会直接继承。
func (a *Agent) StartTransactionFromRequest(r *http.Request) (context.Context, *Transaction) {
	pattern := r.Pattern // go 1.22+ 的 ServeMux 才有,老版本是空串
	if pattern == "" {
		pattern = r.URL.Path
	}
	traceID, spanID := extract(r.Header)
	return a.startTransaction(r.Context(), r.Method+" "+pattern, traceID, spanID)
}

func (tx *Transaction) TraceID() string { return tx.data.TraceID }

func (tx *Transaction) SetTag(k, v string) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.data.Tags == nil {
		tx.data.Tags = make(map[string]string)
	}
	tx.data.Tags[k] = v
}

func (tx *Transaction) SetHTTPStatus(code int) {
	tx.mu.Lock()
	tx.data.HTTPStatus = code
	tx.mu.Unlock()
}

func (tx *Transaction) SetStatus(status string) {
	tx.mu.Lock()
	tx.data.Status = status
	tx.mu.Unlock()
}

// StartSpan 在事务里开一个子 span。parent ctx 里带 span 的话就挂它下面,
// 否则挂在根 span 下。ctx 里没有事务时返回 nil,业务代码不用判空。
func (tx *Transaction) StartSpan(parent context.Context, name, kind string) (context.Context, *Span) {
	s := &Span{
		tx:      tx,
		started: time.Now(),
		data: model.Span{
			SpanID: ids.NewSpanID(),
			Name:   name,
			Kind:   kind,
			Status: model.StatusOK,
		},
	}
	tx.mu.Lock()
	s.data.StartOffset = time.Since(tx.started).Microseconds()
	if ps, ok := parent.Value(spanKey).(*Span); ok {
		s.data.ParentID = ps.data.SpanID
	} else {
		s.data.ParentID = tx.root.data.SpanID
	}
	tx.spans = append(tx.spans, s)
	tx.mu.Unlock()
	return context.WithValue(parent, spanKey, s), s
}

// End 结束事务,把数据交给上报队列。重复调用是安全的,只有第一次生效。
func (tx *Transaction) End() {
	if !tx.ended.CompareAndSwap(false, true) {
		return
	}
	dur := time.Since(tx.started).Microseconds()
	if dur <= 0 {
		dur = 1
	}

	tx.mu.Lock()
	tx.data.Duration = dur
	tx.root.data.Duration = dur

	spans := make([]model.Span, 0, len(tx.spans)+1)
	spans = append(spans, tx.root.data)
	for _, s := range tx.spans {
		// 忘记 End 的 span 按事务结束时间兜底收尾,总比丢数据强
		if !s.ended {
			d := dur - s.data.StartOffset
			if d <= 0 {
				d = 1
			}
			s.data.Duration = d
		}
		spans = append(spans, s.data)
	}
	tx.data.Spans = spans
	tx.mu.Unlock()

	tx.agent.reporter.AddTrace(model.Trace{
		TraceID:      tx.data.TraceID,
		Transactions: []model.Transaction{tx.data},
	})
}

func (s *Span) End() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return
	}
	s.ended = true
	s.data.Duration = time.Since(s.started).Microseconds()
	if s.data.Duration <= 0 {
		s.data.Duration = 1
	}
}

func (s *Span) SetTag(k, v string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data.Tags == nil {
		s.data.Tags = make(map[string]string)
	}
	s.data.Tags[k] = v
}

func (s *Span) SetStatus(status string) {
	s.mu.Lock()
	s.data.Status = status
	s.mu.Unlock()
}

// ---------- context 帮助函数 ----------

// StartSpan 包一层,从 ctx 拿当前事务开 span。没事务时静默跳过。
func StartSpan(ctx context.Context, name, kind string) (context.Context, *Span) {
	tx, _ := ctx.Value(txKey).(*Transaction)
	if tx == nil {
		return ctx, nil
	}
	return tx.StartSpan(ctx, name, kind)
}

func TransactionFromContext(ctx context.Context) *Transaction {
	tx, _ := ctx.Value(txKey).(*Transaction)
	return tx
}

func traceInfoFromCtx(ctx context.Context) (traceID, spanID string) {
	if s, ok := ctx.Value(spanKey).(*Span); ok {
		return s.tx.data.TraceID, s.data.SpanID
	}
	if tx, ok := ctx.Value(txKey).(*Transaction); ok {
		return tx.data.TraceID, tx.root.data.SpanID
	}
	return "", ""
}

// Inject 把 ctx 里的链路信息写进请求头,发下游请求之前调一下。
func Inject(ctx context.Context, h http.Header) {
	traceID, spanID := traceInfoFromCtx(ctx)
	if traceID == "" {
		return
	}
	h.Set(headerTraceID, traceID)
	if spanID != "" {
		h.Set(headerSpanID, spanID)
	}
}

func extract(h http.Header) (traceID, spanID string) {
	return h.Get(headerTraceID), h.Get(headerSpanID)
}
