// Package resilience 放熔斷與限流。
package resilience

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/Chunweiwu518/vod-gateway/internal/cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sony/gobreaker/v2"
)

// ErrOpen 熔斷開啟時回傳。
var ErrOpen = gobreaker.ErrOpenState

// Breaker 包住對 MySQL 的呼叫：連續失敗太多就直接拒絕，不再把慢掉的 DB 打得更慘。
type Breaker struct {
	cb    *gobreaker.CircuitBreaker[[]byte]
	state prometheus.Gauge
}

type BreakerConfig struct {
	Name        string
	MinRequests uint32        // 觀察窗內至少幾個請求才會評估
	FailRatio   float64       // 失敗比例超過就開
	Window      time.Duration // 統計窗
	OpenFor     time.Duration // 開啟多久後進半開
	Reg         prometheus.Registerer
	Logger      *slog.Logger
}

func NewBreaker(c BreakerConfig) *Breaker {
	if c.MinRequests == 0 {
		c.MinRequests = 20
	}
	if c.FailRatio == 0 {
		c.FailRatio = 0.5
	}
	if c.Window == 0 {
		c.Window = 10 * time.Second
	}
	if c.OpenFor == 0 {
		c.OpenFor = 5 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	b := &Breaker{state: prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "vod_breaker_state", Help: "熔斷狀態：0 closed、1 half-open、2 open", ConstLabels: prometheus.Labels{"name": c.Name}})}
	if c.Reg != nil {
		c.Reg.MustRegister(b.state)
	}
	b.cb = gobreaker.NewCircuitBreaker[[]byte](gobreaker.Settings{
		Name:        c.Name,
		MaxRequests: 3,
		Interval:    c.Window,
		Timeout:     c.OpenFor,
		ReadyToTrip: func(cn gobreaker.Counts) bool {
			return cn.Requests >= c.MinRequests && float64(cn.TotalFailures)/float64(cn.Requests) >= c.FailRatio
		},
		// 找不到資料和呼叫端取消不是下游的錯，不計入失敗。
		IsSuccessful: func(err error) bool {
			return err == nil || errors.Is(err, cache.ErrNotFound) || errors.Is(err, context.Canceled)
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			b.state.Set(float64(stateNum(to)))
			c.Logger.Warn("circuit breaker state change", "name", name, "from", from.String(), "to", to.String())
		},
	})
	return b
}

func stateNum(s gobreaker.State) int {
	switch s {
	case gobreaker.StateHalfOpen:
		return 1
	case gobreaker.StateOpen:
		return 2
	}
	return 0
}

func (b *Breaker) Do(fn func() ([]byte, error)) ([]byte, error) { return b.cb.Execute(fn) }

func (b *Breaker) State() string { return b.cb.State().String() }
