package cache

import "github.com/prometheus/client_golang/prometheus"

// Metrics 是快取的 Prometheus 指標。reg 為 nil 時只在記憶體內計數（測試用）。
type Metrics struct {
	requests  *prometheus.CounterVec
	remoteErr prometheus.Counter
	hot       prometheus.Counter
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "vod_cache_requests_total",
			Help: "快取查詢結果：l1／l2 命中、load（回源）、stale（降級回舊值）、null（空值快取命中）",
		}, []string{"result"}),
		remoteErr: prometheus.NewCounter(prometheus.CounterOpts{Name: "vod_cache_remote_errors_total", Help: "Redis 操作失敗次數"}),
		hot:       prometheus.NewCounter(prometheus.CounterOpts{Name: "vod_cache_hot_promotions_total", Help: "被判定為熱 key 並延長 L1 存活的次數"}),
	}
	if reg != nil {
		reg.MustRegister(m.requests, m.remoteErr, m.hot)
	}
	return m
}

func (m *Metrics) Request(s Source) { m.requests.WithLabelValues(string(s)).Inc() }
func (m *Metrics) RemoteError()     { m.remoteErr.Inc() }
func (m *Metrics) HotPromoted()     { m.hot.Inc() }
