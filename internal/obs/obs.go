// Package obs 放可觀測性：slog 結構化日誌（帶 request id）、Prometheus HTTP 指標、OpenTelemetry tracing。
package obs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const Tracer = "vod-gateway"

// SlowRequest 以上的請求不管有沒有被抽樣都會寫 access log。
const SlowRequest = 200 * time.Millisecond

// tracer 啟動時取一次。每次呼叫 otel.Tracer() 都要過全域 provider 的 mutex，熱路徑上不值得。
// 之後 SetTracerProvider 會讓這個 tracer 自動委派給新的 provider。
var tracer = otel.Tracer(Tracer)

type ctxKey struct{}

// L 取出 context 裡帶 request id 的 logger。
func L(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}

func RequestID(ctx context.Context) string {
	return ctx.Value(reqIDKey{}).(string)
}

type reqIDKey struct{}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

type HTTPMetrics struct {
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
	inflight prometheus.Gauge
}

func NewHTTPMetrics(reg prometheus.Registerer) *HTTPMetrics {
	m := &HTTPMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "vod_http_requests_total", Help: "HTTP 請求數"}, []string{"route", "method", "code"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "vod_http_request_duration_seconds", Help: "HTTP 請求延遲",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, []string{"route"}),
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{Name: "vod_http_inflight_requests", Help: "處理中的請求數"}),
	}
	reg.MustRegister(m.requests, m.latency, m.inflight)
	return m
}

type statusWriter struct {
	http.ResponseWriter
	code  int
	bytes int
}

func (w *statusWriter) WriteHeader(c int) { w.code = c; w.ResponseWriter.WriteHeader(c) }
func (w *statusWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Middleware：產生／沿用 X-Request-Id、把帶 request id 的 logger 放進 context、記指標、寫 access log。
//
// sample：成功的請求每 sample 筆才寫一筆 access log（1 = 全寫）；4xx／5xx 與超過 SlowRequest 的一律寫。
// 指標與 tracing 不受影響，每個請求都會記。
func Middleware(log *slog.Logger, m *HTTPMetrics, sample int, next http.Handler) http.Handler {
	var seq atomic.Uint64
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := r.Header.Get("X-Request-Id")
		if id == "" {
			id = newID()
		}
		w.Header().Set("X-Request-Id", id)
		l := log.With("request_id", id)
		if sc := trace.SpanContextFromContext(r.Context()); sc.IsValid() {
			l = l.With("trace_id", sc.TraceID().String())
		}
		ctx := context.WithValue(context.WithValue(r.Context(), ctxKey{}, l), reqIDKey{}, id)
		r = r.WithContext(ctx)

		m.inflight.Inc()
		sw := &statusWriter{ResponseWriter: w, code: 200}
		next.ServeHTTP(sw, r)
		m.inflight.Dec()

		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		dur := time.Since(start)
		m.requests.WithLabelValues(route, r.Method, strconv.Itoa(sw.code)).Inc()
		m.latency.WithLabelValues(route).Observe(dur.Seconds())
		trace.SpanFromContext(ctx).SetName(route)
		if sample > 1 && sw.code < 400 && dur < SlowRequest && seq.Add(1)%uint64(sample) != 0 {
			return
		}
		l.Info("request", "method", r.Method, "path", r.URL.Path, "route", route, "status", sw.code,
			"bytes", sw.bytes, "dur_ms", float64(dur.Microseconds())/1000, "ip", r.RemoteAddr)
	})
}

// NewLogger 回傳 JSON 結構化 logger。
func NewLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, nil))
}

// SetupTracing 依 mode 設定全域 TracerProvider：off | stdout | otlp。
// otlp 讀標準環境變數 OTEL_EXPORTER_OTLP_ENDPOINT。回傳 shutdown。
func SetupTracing(ctx context.Context, mode string, ratio float64, w *os.File) (func(context.Context) error, error) {
	if mode == "" || mode == "off" {
		return func(context.Context) error { return nil }, nil
	}
	var exp sdktrace.SpanExporter
	var err error
	switch mode {
	case "stdout":
		exp, err = stdouttrace.New(stdouttrace.WithWriter(w))
	case "otlp":
		exp, err = otlptracehttp.New(ctx)
	}
	if err != nil {
		return nil, err
	}
	tp := NewProvider(exp, ratio)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

func NewProvider(exp sdktrace.SpanExporter, ratio float64) *sdktrace.TracerProvider {
	res, _ := resource.Merge(resource.Default(), resource.NewSchemaless(semconv.ServiceName("vod-gateway")))
	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
}

// Span 開一個子 span。
func Span(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}
