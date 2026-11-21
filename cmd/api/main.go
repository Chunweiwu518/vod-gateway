// 影片服務 API。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/Chunweiwu518/vod-gateway/internal/api"
	"github.com/Chunweiwu518/vod-gateway/internal/cache"
	"github.com/Chunweiwu518/vod-gateway/internal/config"
	"github.com/Chunweiwu518/vod-gateway/internal/events"
	"github.com/Chunweiwu518/vod-gateway/internal/hls"
	"github.com/Chunweiwu518/vod-gateway/internal/obs"
	"github.com/Chunweiwu518/vod-gateway/internal/rank"
	"github.com/Chunweiwu518/vod-gateway/internal/resilience"
	"github.com/Chunweiwu518/vod-gateway/internal/store"
	"github.com/Chunweiwu518/vod-gateway/internal/web"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func main() {
	log := obs.NewLogger()
	slog.SetDefault(log)
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if config.Bool("ENABLE_PPROF", true) {
		// 預設關閉；壓測時設 MUTEX_FRACTION=5 才會有 mutex profile，平常不付這個成本。
		runtime.SetMutexProfileFraction(config.Int("MUTEX_FRACTION", 0))
		runtime.SetBlockProfileRate(config.Int("BLOCK_RATE", 0))
	}
	shutdownTrace, err := obs.SetupTracing(ctx, config.Str("OTEL_TRACES", "off"), config.Float("OTEL_SAMPLE_RATIO", 0.01), os.Stderr)
	if err != nil {
		return err
	}
	defer shutdownTrace(context.Background())

	st, err := store.Open(config.Str("MYSQL_DSN", "app:app@tcp(127.0.0.1:13306)/vod?parseTime=true&charset=utf8mb4"))
	if err != nil {
		return err
	}
	if err := st.Migrate(ctx); err != nil {
		return err
	}
	rdb := redis.NewClient(&redis.Options{Addr: config.Str("REDIS_ADDR", "127.0.0.1:16379"), PoolSize: 128})

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	cm := cache.NewMetrics(reg)
	c := cache.New(cache.RedisRemote{C: rdb}, cache.Options{
		L1Size:       config.Int("L1_SIZE", 200_000),
		L1TTL:        config.Dur("L1_TTL", time.Second),
		HotL1TTL:     config.Dur("HOT_L1_TTL", 30*time.Second),
		HotThreshold: int64(config.Int("HOT_THRESHOLD", 200)),
		SoftTTL:      config.Dur("CACHE_TTL", time.Minute),
		NullTTL:      config.Dur("NULL_TTL", 30*time.Second),
		Jitter:       config.Float("TTL_JITTER", 0.2),
		Metrics:      cm, Logger: log,
	})
	br := resilience.NewBreaker(resilience.BreakerConfig{Name: "mysql", Reg: reg, Logger: log,
		MinRequests: uint32(config.Int("BREAKER_MIN_REQUESTS", 20)), OpenFor: config.Dur("BREAKER_OPEN_FOR", 5*time.Second)})

	ids, err := st.AllVideoIDs(ctx)
	if err != nil {
		return err
	}
	bl := api.NewBloom(ids, 0.01)
	log.Info("bloom filter loaded", "videos", len(ids))

	hs := hls.NewServer(&hls.Server{
		Signer:  hls.NewSigner(config.Str("HLS_SECRET", "dev-secret-change-me")),
		Hotlink: hls.Hotlink{Hosts: config.List("ALLOWED_HOSTS", []string{"localhost", "127.0.0.1"}), AllowEmpty: config.Bool("ALLOW_EMPTY_REFERER", false)},
		HLSDir:  config.Str("HLS_DIR", "./data/hls"), KeyDir: config.Str("KEY_DIR", "./data/keys"),
		TTL: config.Dur("HLS_TTL", 5*time.Minute), Log: log,
	}, reg)

	srv, h := api.New(api.Config{
		IPRate: config.Float("IP_RATE", 200), IPBurst: config.Int("IP_BURST", 400),
		UserRate: config.Float("USER_RATE", 5), UserBurst: config.Int("USER_BURST", 20),
		TrustProxy: config.Bool("TRUST_PROXY", false), AdminToken: config.Str("ADMIN_TOKEN", ""),
		EnablePprof: config.Bool("ENABLE_PPROF", true), StaticFS: web.FS(),
	}, api.Deps{
		Store: st, Cache: c, Breaker: br, Bloom: bl, Board: rank.New(rdb, "rank:hot"),
		Publisher: events.NewProducer(rdb),
		UserLim:   resilience.NewUserLimiter(rdb, config.Float("USER_RATE", 5), config.Int("USER_BURST", 20)),
		Registry:  reg, HLS: hs,
	})
	go srv.RunBackground(ctx)

	hm := obs.NewHTTPMetrics(reg)
	handler := otelhttp.NewHandler(obs.Middleware(log, hm, h), "vod-api")
	httpSrv := &http.Server{Addr: config.Str("ADDR", ":8080"), Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(sctx)
	}()
	log.Info("listening", "addr", httpSrv.Addr)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
