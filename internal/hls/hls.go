// Package hls 負責播放鏈路的鑑權：
//   - 簽章網址：sig = base64url(HMAC-SHA256(secret, path + "\n" + exp))，exp 是 Unix 秒。
//     Cloudflare Worker（edge/）用同一套演算法在邊緣驗 TS。
//   - m3u8 由本服務動態輸出：每個 TS 與 AES key 的 URI 都改寫成帶簽章與過期時間的網址，
//     所以拿到 m3u8 的人只能在有效期間內取得這一支影片的分段。
//   - AES-128 金鑰檔放在公開目錄之外，只有 /key/{id} 能發，而且要簽章加 Referer／Origin 白名單。
package hls

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	ErrBadSig  = errors.New("bad signature")
	ErrExpired = errors.New("expired")
)

type Signer struct {
	secret []byte
	now    func() time.Time
}

func NewSigner(secret string) *Signer { return &Signer{secret: []byte(secret), now: time.Now} }

func (s *Signer) mac(path string, exp int64) string {
	m := hmac.New(sha256.New, s.secret)
	m.Write([]byte(path))
	m.Write([]byte{'\n'})
	m.Write([]byte(strconv.FormatInt(exp, 10)))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

// SignedURL 回傳 path?exp=...&sig=...
func (s *Signer) SignedURL(path string, exp time.Time) string {
	e := exp.Unix()
	return fmt.Sprintf("%s?exp=%d&sig=%s", path, e, s.mac(path, e))
}

func (s *Signer) Verify(path, expStr, sig string) error {
	exp, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil {
		return ErrBadSig
	}
	if !hmac.Equal([]byte(s.mac(path, exp)), []byte(sig)) {
		return ErrBadSig
	}
	if s.now().Unix() > exp {
		return ErrExpired
	}
	return nil
}

// Hotlink 是 Referer／Origin 白名單（防盜鏈）。
type Hotlink struct {
	Hosts      []string // "example.com"（任何 port）、"localhost:8090"（精確）、"*.example.com"
	AllowEmpty bool     // 沒有 Origin 也沒有 Referer 時是否放行
}

func (h Hotlink) Allowed(r *http.Request) bool {
	raw := r.Header.Get("Origin")
	if raw == "" {
		raw = r.Header.Get("Referer")
	}
	if raw == "" {
		return h.AllowEmpty
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return false
	}
	for _, p := range h.Hosts {
		switch {
		case strings.HasPrefix(p, "*."):
			if strings.HasSuffix(u.Hostname(), p[1:]) {
				return true
			}
		case strings.Contains(p, ":"):
			if u.Host == p {
				return true
			}
		default:
			if u.Hostname() == p {
				return true
			}
		}
	}
	return false
}

var (
	uriAttr  = regexp.MustCompile(`URI="[^"]*"`)
	safeFile = regexp.MustCompile(`^[A-Za-z0-9_-]+\.(m3u8|ts)$`)
)

// RewritePlaylist 把 ffmpeg 產生的 m3u8 改寫成帶簽章的網址。
func RewritePlaylist(raw []byte, id string, s *Signer, exp time.Time) []byte {
	var sb strings.Builder
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimRight(line, "\r")
		switch {
		case strings.HasPrefix(line, "#EXT-X-KEY"):
			line = uriAttr.ReplaceAllString(line, `URI="`+s.SignedURL("/key/"+id, exp)+`"`)
		case line != "" && !strings.HasPrefix(line, "#"):
			line = s.SignedURL("/hls/"+id+"/"+filepath.Base(line), exp)
		}
		sb.WriteString(line)
		sb.WriteByte('\n')
	}
	return []byte(sb.String())
}

type Server struct {
	Signer  *Signer
	Hotlink Hotlink
	HLSDir  string // {HLSDir}/{id}/index.m3u8 與分段
	KeyDir  string // {KeyDir}/{id}.key（16 bytes，不在公開目錄）
	TTL     time.Duration
	Log     *slog.Logger

	denied *prometheus.CounterVec
	mu     sync.RWMutex
	pl     map[string][]byte
}

func NewServer(s *Server, reg prometheus.Registerer) *Server {
	s.denied = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "vod_hls_denied_total", Help: "HLS 請求被拒絕次數（依原因）"}, []string{"reason"})
	if reg != nil {
		reg.MustRegister(s.denied)
	}
	s.pl = map[string][]byte{}
	if s.Log == nil {
		s.Log = slog.Default()
	}
	return s
}

func (s *Server) Mount(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/videos/{id}/play", s.issue)
	mux.HandleFunc("GET /hls/{id}/{file}", s.serveFile)
	mux.HandleFunc("GET /key/{id}", s.serveKey)
}

func validID(id string) bool {
	n, err := strconv.ParseUint(id, 10, 64)
	return err == nil && n > 0
}

func (s *Server) deny(w http.ResponseWriter, reason string, code int) {
	s.denied.WithLabelValues(reason).Inc()
	http.Error(w, reason, code)
}

// issue 發播放網址：防盜鏈通過才給簽章後的 m3u8 網址。
func (s *Server) issue(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID(id) {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}
	if !s.Hotlink.Allowed(r) {
		s.deny(w, "hotlink", http.StatusForbidden)
		return
	}
	if _, err := os.Stat(filepath.Join(s.HLSDir, id, "index.m3u8")); err != nil {
		http.Error(w, "video has no hls stream", http.StatusNotFound)
		return
	}
	exp := time.Now().Add(s.TTL)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"url": s.Signer.SignedURL("/hls/"+id+"/index.m3u8", exp), "expires_at": exp.Unix(),
	})
}

func (s *Server) check(w http.ResponseWriter, r *http.Request, strictReferer bool) bool {
	q := r.URL.Query()
	if err := s.Signer.Verify(r.URL.Path, q.Get("exp"), q.Get("sig")); err != nil {
		reason := "bad_sig"
		if errors.Is(err, ErrExpired) {
			reason = "expired"
		}
		s.deny(w, reason, http.StatusForbidden)
		return false
	}
	h := s.Hotlink
	if strictReferer {
		h.AllowEmpty = false
	}
	if !h.Allowed(r) {
		s.deny(w, "hotlink", http.StatusForbidden)
		return false
	}
	return true
}

func (s *Server) serveFile(w http.ResponseWriter, r *http.Request) {
	id, file := r.PathValue("id"), r.PathValue("file")
	if !validID(id) || !safeFile.MatchString(file) {
		http.Error(w, "bad path", http.StatusBadRequest)
		return
	}
	if !s.check(w, r, false) {
		return
	}
	full := filepath.Join(s.HLSDir, id, file)
	if strings.HasSuffix(file, ".m3u8") {
		raw, err := s.playlist(full)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(RewritePlaylist(raw, id, s.Signer, time.Now().Add(s.TTL)))
		return
	}
	w.Header().Set("Content-Type", "video/mp2t")
	w.Header().Set("Cache-Control", "public, max-age=300")
	http.ServeFile(w, r, full)
}

func (s *Server) playlist(path string) ([]byte, error) {
	s.mu.RLock()
	b, ok := s.pl[path]
	s.mu.RUnlock()
	if ok {
		return b, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.pl[path] = b
	s.mu.Unlock()
	return b, nil
}

// serveKey 發 AES-128 金鑰：簽章有效、Referer／Origin 在白名單（必須帶，不允許空）。
func (s *Server) serveKey(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID(id) {
		http.Error(w, "bad id", http.StatusBadRequest)
		return
	}
	if !s.check(w, r, true) {
		return
	}
	key, err := os.ReadFile(filepath.Join(s.KeyDir, id+".key"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(key)
}
