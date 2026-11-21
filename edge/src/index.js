// vod-edge：Cloudflare Worker，掛在 /hls/* 與 /key/*。
//   1. 路徑檢查（400）→ 簽章與過期（403）→ 防盜鏈（403），失敗不到源站
//   2. TS：Cache API 快取，cache key = 去掉 exp／sig 的 URL，所以不同使用者（不同簽章）共用同一份邊緣快取
//   3. m3u8 與 /key/*：不快取，轉源站（源站自己也會再驗一次）
import { hotlinkAllowed, parseHosts, parsePath, verifySig } from "./auth.js";

const CLIENT_CACHE_CONTROL = "public, max-age=300"; // 與源站 TS 回應一致

function deny(status, reason) {
  // 只回原因代碼，不回傳預期簽章或任何可用來推算簽章的內容
  return new Response(`${reason}\n`, {
    status,
    headers: { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store", "X-Edge-Auth": reason },
  });
}

/** 去掉 exp／sig 的 URL，當作快取 key（其他 query 保留）。 */
export function cacheKeyUrl(url) {
  const u = new URL(url);
  u.searchParams.delete("exp");
  u.searchParams.delete("sig");
  return u.toString();
}

async function proxyToOrigin(request, env, url, cacheState) {
  const origin = new URL(env.ORIGIN_URL);
  const target = new URL(url.pathname + url.search, origin);
  // 沿用原請求的 Origin／Referer 等標頭，源站的防盜鏈才看得到真正的來源
  const res = await fetch(new Request(target, request));
  const out = new Response(res.body, res);
  out.headers.set("X-Edge-Cache", cacheState);
  return out;
}

async function serveTs(request, env, ctx, url) {
  // Range 請求不進快取（Cache API 不收 206），直接轉源站
  if (request.headers.has("Range")) return proxyToOrigin(request, env, url, "BYPASS");

  const cache = caches.default;
  const key = new Request(cacheKeyUrl(request.url), { method: "GET" });
  const hit = await cache.match(key);
  if (hit) {
    const out = new Response(hit.body, hit);
    out.headers.set("Cache-Control", CLIENT_CACHE_CONTROL);
    out.headers.set("X-Edge-Cache", "HIT");
    return out;
  }

  const res = await proxyToOrigin(request, env, url, "MISS");
  if (res.status === 200) {
    const ttl = Number(env.CACHE_TTL_SECONDS) || 86400;
    const stored = new Response(res.clone().body, res);
    stored.headers.set("Cache-Control", `public, max-age=${ttl}`);
    stored.headers.delete("X-Edge-Cache");
    ctx.waitUntil(cache.put(key, stored));
  }
  return res;
}

export default {
  async fetch(request, env, ctx) {
    if (request.method !== "GET" && request.method !== "HEAD") return deny(405, "method_not_allowed");

    const url = new URL(request.url);
    const parsed = parsePath(url.pathname);
    if (!parsed) return deny(400, "bad_path");

    const reason = await verifySig(env.HLS_SECRET, url.pathname, url.searchParams.get("exp"), url.searchParams.get("sig"));
    if (reason) return deny(403, reason);

    // /key/* 一律要帶 Origin 或 Referer（與源站 strictReferer 相同）；TS／m3u8 看 ALLOW_EMPTY_REFERER
    const allowEmpty = parsed.kind !== "key" && String(env.ALLOW_EMPTY_REFERER).toLowerCase() === "true";
    if (!hotlinkAllowed(request.headers, parseHosts(env.ALLOWED_HOSTS), allowEmpty)) return deny(403, "hotlink");

    if (parsed.kind === "ts" && request.method === "GET") return serveTs(request, env, ctx, url);
    return proxyToOrigin(request, env, url, "BYPASS");
  },
};
