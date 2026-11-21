// Package api 是 HTTP 層：路由、快取查詢、限流、播放事件與排行榜。
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/Chunweiwu518/vod-gateway/internal/cache"
	"github.com/Chunweiwu518/vod-gateway/internal/events"
	"github.com/Chunweiwu518/vod-gateway/internal/obs"
	"github.com/Chunweiwu518/vod-gateway/internal/rank"
	"github.com/Chunweiwu518/vod-gateway/internal/resilience"
	"github.com/Chunweiwu518/vod-gateway/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/sync/errgroup"
)

// Store 是 API 需要的資料存取介面（測試用假實作取代 MySQL，可以精確計算回源次數）。
type Store interface {
	GetVideo(ctx context.Context, id int64) (store.Video, error)
	GetCreator(ctx context.Context, id int64) (store.Creator, error)
	ListByCreator(ctx context.Context, creatorID int64, limit int) ([]store.Video, error)
	SearchVideos(ctx context.Context, q string, limit int) ([]store.Video, error)
	SearchCreators(ctx context.Context, prefix string, limit int) ([]store.Creator, error)
	ListHistory(ctx context.Context, userID int64, limit int) ([]store.HistoryItem, error)
	AddFavorite(ctx context.Context, userID, videoID int64) (bool, error)
	RemoveFavorite(ctx context.Context, userID, videoID int64) (bool, error)
	ListFavorites(ctx context.Context, userID int64, limit int) ([]store.Video, error)
	CreateVideo(ctx context.Context, v store.Video) (int64, error)
}

type Publisher interface {
	Publish(ctx context.Context, e events.Event) error
}

type Config struct {
	IPRate      float64 // 每 IP 每秒請求數
	IPBurst     int
	UserRate    float64 // 每使用者每秒寫入請求數
	UserBurst   int
	TrustProxy  bool // 信任 X-Forwarded-For
	AdminToken  string
	EnablePprof bool
	StaticFS    http.FileSystem
}

type Server struct {
	cfg     Config
	store   Store
	cache   *cache.Cache
	breaker *resilience.Breaker
	bloom   *Bloom
	board   *rank.Board
	pub     Publisher
	ipLim   *resilience.IPLimiter
	userLim *resilience.UserLimiter
	reg     *prometheus.Registry
	limited *prometheus.CounterVec
	bloomRj prometheus.Counter
}

type Deps struct {
	Store     Store
	Cache     *cache.Cache
	Breaker   *resilience.Breaker
	Bloom     *Bloom
	Board     *rank.Board
	Publisher Publisher
	UserLim   *resilience.UserLimiter
	Registry  *prometheus.Registry
	HLS       interface{ Mount(*http.ServeMux) }
}

func New(cfg Config, d Deps) (*Server, http.Handler) {
	s := &Server{cfg: cfg, store: d.Store, cache: d.Cache, breaker: d.Breaker, bloom: d.Bloom, board: d.Board,
		pub: d.Publisher, userLim: d.UserLim, reg: d.Registry,
		ipLim: resilience.NewIPLimiter(cfg.IPRate, cfg.IPBurst)}
	s.limited = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "vod_ratelimited_total", Help: "被限流擋下的請求數"}, []string{"scope"})
	s.bloomRj = prometheus.NewCounter(prometheus.CounterOpts{Name: "vod_bloom_rejects_total", Help: "被布隆過濾器直接擋下的請求（不碰快取與 DB）"})
	d.Registry.MustRegister(s.limited, s.bloomRj)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("GET /metrics", promhttp.HandlerFor(d.Registry, promhttp.HandlerOpts{}))
	if cfg.EnablePprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	mux.HandleFunc("GET /api/videos/{id}", s.getVideo)
	mux.HandleFunc("POST /api/videos", s.createVideo)
	mux.HandleFunc("GET /api/creators/{id}", s.getCreator)
	mux.HandleFunc("GET /api/creators/{id}/videos", s.creatorVideos)
	mux.HandleFunc("GET /api/search", s.search)
	mux.HandleFunc("GET /api/rank/hot", s.hotRank)
	mux.HandleFunc("POST /api/play-events", s.playEvent)
	mux.HandleFunc("GET /api/me/history", s.history)
	mux.HandleFunc("GET /api/me/favorites", s.listFavorites)
	mux.HandleFunc("PUT /api/me/favorites/{vid}", s.addFavorite)
	mux.HandleFunc("DELETE /api/me/favorites/{vid}", s.removeFavorite)
	if d.HLS != nil {
		d.HLS.Mount(mux)
	}
	if cfg.StaticFS != nil {
		fs := http.FileServer(cfg.StaticFS)
		mux.Handle("GET /{$}", fs)
		mux.Handle("GET /hls.min.js", fs)
	}
	return s, s.ipLimit(mux)
}

func (s *Server) RunBackground(ctx context.Context) { s.ipLim.Run(ctx) }

// ---- 限流 ----

func (s *Server) clientIP(r *http.Request) string {
	if s.cfg.TrustProxy {
		if xf := r.Header.Get("X-Forwarded-For"); xf != "" {
			ip, _, _ := strings.Cut(xf, ",")
			return strings.TrimSpace(ip)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *Server) ipLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if p == "/healthz" || p == "/metrics" || strings.HasPrefix(p, "/debug/") {
			next.ServeHTTP(w, r)
			return
		}
		if !s.ipLim.Allow(s.clientIP(r)) {
			s.limited.WithLabelValues("ip").Inc()
			w.Header().Set("Retry-After", "1")
			writeErr(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) userAllowed(w http.ResponseWriter, r *http.Request) (int64, bool) {
	uid, err := strconv.ParseInt(r.Header.Get("X-User-Id"), 10, 64)
	if err != nil || uid <= 0 {
		writeErr(w, http.StatusUnauthorized, "X-User-Id required")
		return 0, false
	}
	return uid, true
}

func (s *Server) userWriteAllowed(w http.ResponseWriter, r *http.Request) (int64, bool) {
	uid, ok := s.userAllowed(w, r)
	if !ok {
		return 0, false
	}
	if s.userLim != nil && !s.userLim.Allow(r.Context(), strconv.FormatInt(uid, 10)) {
		s.limited.WithLabelValues("user").Inc()
		w.Header().Set("Retry-After", "1")
		writeErr(w, http.StatusTooManyRequests, "user rate limit exceeded")
		return 0, false
	}
	return uid, true
}

// ---- 共用 ----

func writeJSON(w http.ResponseWriter, code int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		writeErr(w, 500, "encode failed")
		return
	}
	writeRaw(w, code, b)
}

func writeRaw(w http.ResponseWriter, code int, b []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(b)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func pathID(r *http.Request, name string) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue(name), 10, 64)
	return id, err == nil && id > 0
}

func limitParam(r *http.Request, def, max int) int {
	n, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

// guard 把回源包上 tracing span 與熔斷：熔斷開啟時直接回錯誤，由快取層決定要不要降級回舊值。
func (s *Server) guard(key string, load cache.Loader) cache.Loader {
	return func(ctx context.Context) ([]byte, error) {
		ctx, sp := obs.Span(ctx, "db.load", attribute.String("cache.key", key))
		defer sp.End()
		return s.breaker.Do(func() ([]byte, error) { return load(ctx) })
	}
}

func (s *Server) videoLoader(id int64) cache.Loader {
	return func(ctx context.Context) ([]byte, error) {
		v, err := s.store.GetVideo(ctx, id)
		if err != nil {
			return nil, err
		}
		return json.Marshal(v)
	}
}

// cached 是所有讀取端點共用的路徑：兩層快取 + 熔斷包住的回源。
func (s *Server) cached(w http.ResponseWriter, r *http.Request, key string, soft time.Duration, load func(ctx context.Context) ([]byte, error)) {
	ctx, span := obs.Span(r.Context(), "cache.get", attribute.String("cache.key", key))
	defer span.End()
	res, err := s.cache.Get(ctx, key, soft, s.guard(key, load))
	switch {
	case errors.Is(err, cache.ErrNotFound):
		span.SetAttributes(attribute.String("cache.result", "null"))
		writeErr(w, http.StatusNotFound, "not found")
	case err != nil:
		span.RecordError(err)
		obs.L(r.Context()).Warn("load failed", "key", key, "err", err)
		w.Header().Set("Retry-After", "5")
		writeErr(w, http.StatusServiceUnavailable, "temporarily unavailable")
	default:
		span.SetAttributes(attribute.String("cache.result", string(res.Source)))
		w.Header().Set("X-Cache", string(res.Source))
		if res.Source == cache.SourceStale {
			w.Header().Set("X-Degraded", "stale")
		}
		writeRaw(w, http.StatusOK, res.Val)
	}
}

// ---- 影片與創作者 ----

func (s *Server) getVideo(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r, "id")
	if !ok {
		writeErr(w, http.StatusBadRequest, "bad id")
		return
	}
	if !s.bloom.Test(id) { // 布隆過濾器說一定沒有：不碰快取也不碰 DB
		s.bloomRj.Inc()
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	s.cached(w, r, "video:"+strconv.FormatInt(id, 10), 0, s.videoLoader(id))
}

func (s *Server) getCreator(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r, "id")
	if !ok {
		writeErr(w, http.StatusBadRequest, "bad id")
		return
	}
	s.cached(w, r, "creator:"+strconv.FormatInt(id, 10), 0, func(ctx context.Context) ([]byte, error) {
		c, err := s.store.GetCreator(ctx, id)
		if err != nil {
			return nil, err
		}
		return json.Marshal(c)
	})
}

func (s *Server) creatorVideos(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r, "id")
	if !ok {
		writeErr(w, http.StatusBadRequest, "bad id")
		return
	}
	limit := limitParam(r, 20, 50)
	key := "cvideos:" + strconv.FormatInt(id, 10) + ":" + strconv.Itoa(limit)
	s.cached(w, r, key, 0, func(ctx context.Context) ([]byte, error) {
		vs, err := s.store.ListByCreator(ctx, id, limit)
		if err != nil {
			return nil, err
		}
		return json.Marshal(vs)
	})
}

func (s *Server) createVideo(w http.ResponseWriter, r *http.Request) {
	if s.cfg.AdminToken == "" || r.Header.Get("X-Admin-Token") != s.cfg.AdminToken {
		writeErr(w, http.StatusUnauthorized, "admin token required")
		return
	}
	var in store.Video
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&in); err != nil || in.CreatorID <= 0 || in.Title == "" {
		writeErr(w, http.StatusBadRequest, "creator_id and title required")
		return
	}
	id, err := s.store.CreateVideo(r.Context(), in)
	if err != nil {
		obs.L(r.Context()).Error("create video failed", "err", err)
		writeErr(w, http.StatusInternalServerError, "create failed")
		return
	}
	s.bloom.Add(id)
	// 清掉可能殘留的空值快取與該創作者的列表快取
	_ = s.cache.Invalidate(r.Context(), "video:"+strconv.FormatInt(id, 10),
		"cvideos:"+strconv.FormatInt(in.CreatorID, 10)+":20")
	writeJSON(w, http.StatusCreated, map[string]int64{"id": id})
}

// ---- 搜尋：多來源並行聚合 ----

func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if len([]rune(q)) < 2 {
		writeErr(w, http.StatusBadRequest, "q must be at least 2 characters")
		return
	}
	limit := limitParam(r, 10, 30)
	key := "search:" + strings.ToLower(q) + ":" + strconv.Itoa(limit)
	s.cached(w, r, key, 30*time.Second, func(ctx context.Context) ([]byte, error) {
		var videos []store.Video
		var creators []store.Creator
		g, gctx := errgroup.WithContext(ctx)
		g.Go(func() (err error) { videos, err = s.store.SearchVideos(gctx, q, limit); return })
		g.Go(func() (err error) { creators, err = s.store.SearchCreators(gctx, q, limit); return })
		if err := g.Wait(); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{"query": q, "videos": videos, "creators": creators})
	})
}

// ---- 排行榜 ----

func (s *Server) hotRank(w http.ResponseWriter, r *http.Request) {
	limit := limitParam(r, 20, 100)
	s.cached(w, r, "rank:top:"+strconv.Itoa(limit), 2*time.Second, func(ctx context.Context) ([]byte, error) {
		top, err := s.board.Top(ctx, limit)
		if err != nil {
			return nil, err
		}
		type row struct {
			VideoID int64   `json:"video_id"`
			Title   string  `json:"title"`
			Plays   float64 `json:"plays"`
		}
		out := make([]row, 0, len(top))
		for _, it := range top {
			key := "video:" + strconv.FormatInt(it.VideoID, 10)
			res, err := s.cache.Get(ctx, key, 0, s.guard(key, s.videoLoader(it.VideoID)))
			if err != nil && !errors.Is(err, cache.ErrNotFound) {
				return nil, err
			}
			var v store.Video
			if err == nil {
				_ = json.Unmarshal(res.Val, &v)
			}
			out = append(out, row{VideoID: it.VideoID, Title: v.Title, Plays: it.Score})
		}
		return json.Marshal(out)
	})
}

// ---- 播放事件（非同步）----

type playEventReq struct {
	VideoID     int64 `json:"video_id"`
	PositionSec int   `json:"position_sec"`
}

func (s *Server) playEvent(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.userWriteAllowed(w, r)
	if !ok {
		return
	}
	var in playEventReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<12)).Decode(&in); err != nil || in.VideoID <= 0 || in.PositionSec < 0 {
		writeErr(w, http.StatusBadRequest, "video_id and position_sec required")
		return
	}
	if !s.bloom.Test(in.VideoID) {
		s.bloomRj.Inc()
		writeErr(w, http.StatusNotFound, "video not found")
		return
	}
	var idb [12]byte
	_, _ = rand.Read(idb[:])
	e := events.Event{ID: hex.EncodeToString(idb[:]), UserID: uid, VideoID: in.VideoID, PositionSec: in.PositionSec}
	if err := s.pub.Publish(r.Context(), e); err != nil {
		obs.L(r.Context()).Error("publish play event failed", "err", err)
		writeErr(w, http.StatusServiceUnavailable, "event queue unavailable")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"event_id": e.ID})
}

// ---- 使用者：紀錄與收藏（直接讀寫 MySQL，不走共享快取，因為是每人專屬資料）----

func (s *Server) history(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.userAllowed(w, r)
	if !ok {
		return
	}
	items, err := s.breakerList(func() (any, error) { return s.store.ListHistory(r.Context(), uid, limitParam(r, 20, 100)) })
	s.respond(w, r, items, err)
}

func (s *Server) listFavorites(w http.ResponseWriter, r *http.Request) {
	uid, ok := s.userAllowed(w, r)
	if !ok {
		return
	}
	items, err := s.breakerList(func() (any, error) { return s.store.ListFavorites(r.Context(), uid, limitParam(r, 20, 100)) })
	s.respond(w, r, items, err)
}

func (s *Server) breakerList(fn func() (any, error)) (any, error) {
	var out any
	_, err := s.breaker.Do(func() ([]byte, error) {
		v, err := fn()
		out = v
		return nil, err
	})
	return out, err
}

func (s *Server) respond(w http.ResponseWriter, r *http.Request, v any, err error) {
	if err != nil {
		obs.L(r.Context()).Warn("query failed", "err", err)
		writeErr(w, http.StatusServiceUnavailable, "temporarily unavailable")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Server) addFavorite(w http.ResponseWriter, r *http.Request) { s.setFavorite(w, r, true) }
func (s *Server) removeFavorite(w http.ResponseWriter, r *http.Request) {
	s.setFavorite(w, r, false)
}

func (s *Server) setFavorite(w http.ResponseWriter, r *http.Request, add bool) {
	uid, ok := s.userWriteAllowed(w, r)
	if !ok {
		return
	}
	vid, ok := pathID(r, "vid")
	if !ok {
		writeErr(w, http.StatusBadRequest, "bad video id")
		return
	}
	if !s.bloom.Test(vid) {
		s.bloomRj.Inc()
		writeErr(w, http.StatusNotFound, "video not found")
		return
	}
	var changed bool
	_, err := s.breaker.Do(func() ([]byte, error) {
		var e error
		if add {
			changed, e = s.store.AddFavorite(r.Context(), uid, vid)
		} else {
			changed, e = s.store.RemoveFavorite(r.Context(), uid, vid)
		}
		return nil, e
	})
	if err != nil {
		obs.L(r.Context()).Warn("favorite failed", "err", err)
		writeErr(w, http.StatusServiceUnavailable, "temporarily unavailable")
		return
	}
	if changed {
		_ = s.cache.Invalidate(r.Context(), "video:"+strconv.FormatInt(vid, 10))
	}
	writeJSON(w, http.StatusOK, map[string]bool{"changed": changed})
}
