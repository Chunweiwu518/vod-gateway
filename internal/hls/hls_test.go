package hls

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSigner_VerifyExpiryTamperAndPathBinding(t *testing.T) {
	s := NewSigner("secret")
	now := time.Unix(1_700_000_000, 0)
	s.now = func() time.Time { return now }

	u := s.SignedURL("/hls/1/seg_000.ts", now.Add(time.Minute))
	var exp, sig string
	for _, kv := range strings.Split(strings.SplitN(u, "?", 2)[1], "&") {
		k, v, _ := strings.Cut(kv, "=")
		if k == "exp" {
			exp = v
		} else {
			sig = v
		}
	}
	if err := s.Verify("/hls/1/seg_000.ts", exp, sig); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	if err := s.Verify("/hls/2/seg_000.ts", exp, sig); !errors.Is(err, ErrBadSig) {
		t.Fatalf("signature must bind to path, got %v", err)
	}
	if err := s.Verify("/hls/1/seg_000.ts", "1999999999", sig); !errors.Is(err, ErrBadSig) {
		t.Fatalf("changed exp must invalidate signature, got %v", err)
	}
	if err := s.Verify("/hls/1/seg_000.ts", exp, "AAAA"+sig[4:]); !errors.Is(err, ErrBadSig) {
		t.Fatalf("tampered sig accepted: %v", err)
	}
	if err := NewSigner("other").Verify("/hls/1/seg_000.ts", exp, sig); !errors.Is(err, ErrBadSig) {
		t.Fatalf("different secret accepted: %v", err)
	}
	now = now.Add(2 * time.Minute)
	if err := s.Verify("/hls/1/seg_000.ts", exp, sig); !errors.Is(err, ErrExpired) {
		t.Fatalf("want ErrExpired, got %v", err)
	}
}

func TestHotlink(t *testing.T) {
	h := Hotlink{Hosts: []string{"example.com", "localhost:8090", "*.cdn.example.org"}}
	cases := []struct {
		hdr, val string
		want     bool
	}{
		{"Referer", "https://example.com/watch?v=1", true},
		{"Referer", "https://example.com:8443/x", true},
		{"Referer", "https://evil.com/", false},
		{"Referer", "https://example.com.evil.com/", false},
		{"Origin", "http://localhost:8090", true},
		{"Origin", "http://localhost:9999", false},
		{"Referer", "https://a.cdn.example.org/", true},
		{"Referer", "https://cdn.example.org/", false},
		{"Origin", "null", false},
		{"", "", false},
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "/", nil)
		if c.hdr != "" {
			r.Header.Set(c.hdr, c.val)
		}
		if got := h.Allowed(r); got != c.want {
			t.Errorf("%s=%q: got %v want %v", c.hdr, c.val, got, c.want)
		}
	}
	h.AllowEmpty = true
	if !h.Allowed(httptest.NewRequest("GET", "/", nil)) {
		t.Error("AllowEmpty should allow missing headers")
	}
	// Origin 優先於 Referer：Origin 不在白名單就算 Referer 在也拒絕
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Origin", "https://evil.com")
	r.Header.Set("Referer", "https://example.com/")
	if h.Allowed(r) {
		t.Error("Origin must take precedence over Referer")
	}
}

func TestRewritePlaylist(t *testing.T) {
	raw := []byte("#EXTM3U\n#EXT-X-KEY:METHOD=AES-128,URI=\"enc.key\",IV=0xAB\n#EXTINF:6.0,\nseg_000.ts\n#EXTINF:6.0,\n../evil/seg_001.ts\n#EXT-X-ENDLIST\n")
	s := NewSigner("k")
	out := string(RewritePlaylist(raw, "7", s, time.Unix(2_000_000_000, 0)))
	for _, want := range []string{`URI="/key/7?exp=2000000000&sig=`, ",IV=0xAB", "\n/hls/7/seg_000.ts?exp=2000000000&sig=", "\n/hls/7/seg_001.ts?exp=2000000000&sig=", "#EXT-X-ENDLIST"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "..") || strings.Contains(out, "enc.key") {
		t.Errorf("unsafe content left in playlist:\n%s", out)
	}
}

func TestServer_DeniesWithoutValidSignature(t *testing.T) {
	srv := NewServer(&Server{Signer: NewSigner("k"), Hotlink: Hotlink{Hosts: []string{"localhost"}}, HLSDir: t.TempDir(), KeyDir: t.TempDir(), TTL: time.Minute}, nil)
	mux := http.NewServeMux()
	srv.Mount(mux)
	for _, path := range []string{"/hls/1/index.m3u8", "/hls/1/seg_000.ts", "/key/1", "/hls/1/index.m3u8?exp=1&sig=x"} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Referer", "http://localhost/")
		mux.ServeHTTP(w, r)
		if w.Code != http.StatusForbidden {
			t.Errorf("%s: got %d want 403", path, w.Code)
		}
	}
}
