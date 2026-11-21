package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestMiddleware_RequestIDLogsMetricsAndTraces(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	reg := prometheus.NewRegistry()
	m := NewHTTPMetrics(reg)

	exp := tracetest.NewInMemoryExporter()
	tp := NewProvider(exp, 1.0)
	otel.SetTracerProvider(tp)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /videos/{id}", func(w http.ResponseWriter, r *http.Request) {
		_, span := Span(r.Context(), "db.load")
		span.End()
		L(r.Context()).Info("handler ran")
		w.Write([]byte("ok"))
	})
	h := otelhttp.NewHandler(Middleware(log, m, 1, mux), "vod-api")

	// 沿用呼叫端帶來的 request id
	r := httptest.NewRequest("GET", "/videos/42", nil)
	r.Header.Set("X-Request-Id", "req-abc")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if got := w.Header().Get("X-Request-Id"); got != "req-abc" {
		t.Fatalf("request id not propagated: %q", got)
	}
	// 沒帶就產生一個
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, httptest.NewRequest("GET", "/videos/43", nil))
	if len(w2.Header().Get("X-Request-Id")) != 16 {
		t.Fatalf("generated id %q", w2.Header().Get("X-Request-Id"))
	}

	// 日誌：每行是 JSON，handler 內的 log 也帶同一個 request_id 與 trace_id
	var sawHandler, sawAccess bool
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if rec["request_id"] != "req-abc" {
			continue
		}
		if rec["trace_id"] == nil || rec["trace_id"] == "" {
			t.Fatalf("log line missing trace_id: %v", rec)
		}
		switch rec["msg"] {
		case "handler ran":
			sawHandler = true
		case "request":
			sawAccess = true
			if rec["route"] != "GET /videos/{id}" || rec["status"] != float64(200) {
				t.Fatalf("access log: %v", rec)
			}
		}
	}
	if !sawHandler || !sawAccess {
		t.Fatalf("handler=%v access=%v\n%s", sawHandler, sawAccess, logs.String())
	}

	// 指標：用路由樣板當 label（不是實際路徑），避免 label 爆量
	if n := testutil.CollectAndCount(m.requests); n != 1 {
		t.Fatalf("want 1 time series (route template), got %d", n)
	}

	// tracing：HTTP span 與子 span 在同一條 trace，HTTP span 以路由樣板命名
	_ = tp.ForceFlush(context.Background())
	spans := exp.GetSpans()
	var root, child bool
	for _, s := range spans {
		if s.Name == "GET /videos/{id}" {
			root = true
		}
		if s.Name == "db.load" {
			child = true
		}
	}
	if !root || !child {
		t.Fatalf("root=%v child=%v spans=%d", root, child, len(spans))
	}
	tids := map[string]bool{}
	for _, s := range spans {
		tids[s.SpanContext.TraceID().String()] = true
	}
	if len(tids) != 2 { // 兩個請求兩條 trace，每條 trace 內的 span 共用 trace id
		t.Fatalf("want 2 traces, got %d", len(tids))
	}
}

// access log 抽樣：成功請求 1/100，錯誤與慢請求一律寫；指標每個請求都記。
func TestMiddleware_AccessLogSampling(t *testing.T) {
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, nil))
	m := NewHTTPMetrics(prometheus.NewRegistry())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ok", func(w http.ResponseWriter, r *http.Request) {})
	mux.HandleFunc("GET /bad", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) })
	mux.HandleFunc("GET /slow", func(w http.ResponseWriter, r *http.Request) { time.Sleep(SlowRequest + 20*time.Millisecond) })
	h := Middleware(log, m, 100, mux)
	for i := 0; i < 1000; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/ok", nil))
	}
	for i := 0; i < 50; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/bad", nil))
	}
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/slow", nil))
	n := func(route string) int { return strings.Count(logs.String(), `"route":"`+route+`"`) }
	if n("GET /ok") != 10 || n("GET /bad") != 50 || n("GET /slow") != 1 {
		t.Fatalf("ok=%d bad=%d slow=%d, want 10/50/1", n("GET /ok"), n("GET /bad"), n("GET /slow"))
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("GET /ok", "GET", "200")); got != 1000 {
		t.Fatalf("metrics must count every request, got %v", got)
	}
}
