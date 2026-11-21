// 邊緣鑑權的純函式（不依賴 Worker runtime，node:test 可直接跑）。
// 規格與 Go 端 internal/hls/hls.go 一致：
//   sig = base64url 無 padding（HMAC-SHA256(secret, path + "\n" + exp)），exp 為 Unix 秒。

const enc = new TextEncoder();

// /hls/{數字id}/{檔名}.(m3u8|ts) 與 /key/{數字id}；檔名只允許 [A-Za-z0-9_-]+
const HLS_RE = /^\/hls\/([0-9]+)\/([A-Za-z0-9_-]+)\.(m3u8|ts)$/;
const KEY_RE = /^\/key\/([0-9]+)$/;
const MAX_UINT64 = 18446744073709551615n;

function validId(id) {
  const n = BigInt(id);
  return n > 0n && n <= MAX_UINT64; // 同 Go 的 ParseUint(id) 且 > 0
}

/** 回傳 { kind: "ts"|"m3u8"|"key", id } 或 null（路徑不合法）。 */
export function parsePath(pathname) {
  let m = HLS_RE.exec(pathname);
  if (m && validId(m[1])) return { kind: m[3], id: m[1] };
  m = KEY_RE.exec(pathname);
  if (m && validId(m[1])) return { kind: "key", id: m[1] };
  return null;
}

function b64urlToBytes(s) {
  if (!/^[A-Za-z0-9_-]+$/.test(s)) return null; // RawURLEncoding：不允許 padding 與其他字元
  const b64 = s.replace(/-/g, "+").replace(/_/g, "/");
  try {
    const bin = atob(b64 + "=".repeat((4 - (b64.length % 4)) % 4));
    const bytes = Uint8Array.from(bin, (c) => c.charCodeAt(0));
    // 拒絕非標準寫法（末字元多餘的 bit 不同但解出相同位元組），與 Go 的字串比對一致
    return bytesToB64url(bytes) === s ? bytes : null;
  } catch {
    return null;
  }
}

function bytesToB64url(bytes) {
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function hmacKey(secret, usage) {
  return crypto.subtle.importKey("raw", enc.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, [usage]);
}

/** 產生簽章（測試與本地工具用；Worker 只驗不簽）。 */
export async function sign(secret, path, exp) {
  const mac = await crypto.subtle.sign("HMAC", await hmacKey(secret, "sign"), enc.encode(`${path}\n${exp}`));
  return bytesToB64url(new Uint8Array(mac));
}

/**
 * 驗簽章與過期。回傳 null 表示通過，否則回傳原因字串："bad_sig" | "expired"。
 * 比對用 crypto.subtle.verify（HMAC verify 為常數時間），不自己 === 比字串。
 * nowSec 可注入以便測試。
 */
export async function verifySig(secret, path, expStr, sig, nowSec = Math.floor(Date.now() / 1000)) {
  if (typeof expStr !== "string" || !/^[0-9]{1,15}$/.test(expStr) || typeof sig !== "string") return "bad_sig";
  const sigBytes = b64urlToBytes(sig);
  if (!sigBytes) return "bad_sig";
  const ok = await crypto.subtle.verify("HMAC", await hmacKey(secret, "verify"), sigBytes, enc.encode(`${path}\n${Number(expStr)}`));
  if (!ok) return "bad_sig";
  if (nowSec > Number(expStr)) return "expired";
  return null;
}

/** "a.com, *.b.com ,localhost:8090" -> ["a.com","*.b.com","localhost:8090"] */
export function parseHosts(csv) {
  return String(csv ?? "").split(",").map((s) => s.trim()).filter(Boolean);
}

/**
 * 防盜鏈：Origin 優先、沒有就看 Referer；兩者都沒有時看 allowEmpty。
 *   "example.com"   比對 hostname（任何 port）
 *   "localhost:8090" 精確比對 host:port
 *   "*.example.com"  比對 hostname 尾綴（不含 example.com 本身，與 Go 版一致）
 * 與 Go 版的差異：URL 解析會把 host 轉小寫（瀏覽器送出的 Origin 本來就是小寫）。
 */
export function hotlinkAllowed(headers, hosts, allowEmpty) {
  const raw = headers.get("Origin") || headers.get("Referer") || "";
  if (raw === "") return allowEmpty;
  let u;
  try {
    u = new URL(raw);
  } catch {
    return false;
  }
  if (u.host === "") return false;
  for (const p of hosts) {
    const pl = p.toLowerCase();
    if (pl.startsWith("*.")) {
      if (u.hostname.endsWith(pl.slice(1))) return true;
    } else if (pl.includes(":")) {
      if (u.host === pl) return true;
    } else if (u.hostname === pl) {
      return true;
    }
  }
  return false;
}
