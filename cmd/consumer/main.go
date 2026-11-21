// 播放事件 consumer：從 Redis Stream 批次讀事件，聚合後更新排行榜與 MySQL 播放數。
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Chunweiwu518/vod-gateway/internal/config"
	"github.com/Chunweiwu518/vod-gateway/internal/events"
	"github.com/Chunweiwu518/vod-gateway/internal/obs"
	"github.com/Chunweiwu518/vod-gateway/internal/rank"
	"github.com/Chunweiwu518/vod-gateway/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

func main() {
	log := obs.NewLogger()
	slog.SetDefault(log)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(config.Str("MYSQL_DSN", "app:app@tcp(127.0.0.1:13306)/vod?parseTime=true&charset=utf8mb4"))
	if err != nil {
		log.Error("mysql", "err", err)
		os.Exit(1)
	}
	if err := st.Migrate(ctx); err != nil {
		log.Error("migrate", "err", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(&redis.Options{Addr: config.Str("REDIS_ADDR", "127.0.0.1:16379")})
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	name, _ := os.Hostname()
	c := events.NewConsumer(rdb, st, rank.New(rdb, "rank:hot"), events.ConsumerConfig{
		Name: name, Batch: int64(config.Int("BATCH", 500)), Reg: reg, Logger: log,
	})

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	go func() {
		s := &http.Server{Addr: config.Str("ADDR", ":8081"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "err", err)
		}
	}()
	log.Info("consumer started", "name", name)
	if err := c.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("consumer stopped", "err", err)
		os.Exit(1)
	}
}
