import { test } from "node:test";
import assert from "node:assert/strict";
import { hotlinkAllowed, parseHosts, parsePath, sign, verifySig } from "../src/auth.js";
import worker, { cacheKeyUrl } from "../src/index.js";

const SECRET = "test-secret";
const PATH = "/hls/1/seg_000.ts";
const NOW = 1_800_000_000;

// 這組向量由 Go 端 `go run ./cmd/signurl -secret test-secret -path /hls/1/seg_000.ts -exp 4102444800` 產生
const GO_VECTOR = { exp: "4102444800", sig: "4CDVGtzJ2oAKv_GGZb7vazbWuiFYm9ERboddtRjk084" };

test("簽章：與 Go 產生的向量一致（跨語言）", async () => {
  assert.equal(await sign(SECRET, PATH, GO_VECTOR.exp), GO_VECTOR.sig);
  assert.equal(await verifySig(SECRET, PATH, GO_VECTOR.exp, GO_VECTOR.sig, NOW), null);
});

test("簽章：有效、剛好到期秒仍有效、過一秒過期", async () => {
  const exp = NOW + 60;
  const sig = await sign(SECRET, PATH, exp);
  assert.equal(await verifySig(SECRET, PATH, String(exp), sig, NOW), null);
  assert.equal(await verifySig(SECRET, PATH, String(exp), sig, exp), null);
  assert.equal(await verifySig(SECRET, PATH, String(exp), sig, exp + 1), "expired");
});

test("簽章：竄改 sig／path／exp／secret 都 bad_sig", async () => {
  const exp = String(NOW + 60);
  const sig = await sign(SECRET, PATH, exp);
  const flipped = (sig[0] === "A" ? "B" : "A") + sig.slice(1);
  assert.equal(await verifySig(SECRET, PATH, exp, flipped, NOW), "bad_sig");
  assert.equal(await verifySig(SECRET, "/hls/2/seg_000.ts", exp, sig, NOW), "bad_sig");
  assert.equal(await verifySig(SECRET, PATH, String(NOW + 9999), sig, NOW), "bad_sig");
  assert.equal(await verifySig("other-secret", PATH, exp, sig, NOW), "bad_sig");
});

test("簽章：過期的簽章即使正確也回 expired；竄改過的過期簽章回 bad_sig（先比簽章）", async () => {
  const exp = NOW - 10;
  const sig = await sign(SECRET, PATH, exp);
  assert.equal(await verifySig(SECRET, PATH, String(exp), sig, NOW), "expired");
  assert.equal(await verifySig(SECRET, PATH, String(exp), "AAAA" + sig.slice(4), NOW), "bad_sig");
});

test("簽章：缺參數、格式錯誤、padding、非 base64url 都 bad_sig", async () => {
  const exp = String(NOW + 60);
  const sig = await sign(SECRET, PATH, exp);
  for (const [e, s] of [
    [null, sig], [exp, null], ["", sig], [exp, ""], ["abc", sig], ["-1", sig], ["+60", sig], ["1.5", sig],
    [exp, sig + "="], [exp, sig + "!"], [exp, "@@@"], [exp, sig.slice(0, -1)], [exp, sig + "A"],
  ]) {
    assert.equal(await verifySig(SECRET, PATH, e, s, NOW), "bad_sig", `exp=${e} sig=${s}`);
  }
});

test("簽章：末字元非標準寫法（解出相同位元組）也拒絕", async () => {
  const exp = String(NOW + 60);
  const sig = await sign(SECRET, PATH, exp);
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  let rejected = 0;
  for (const c of alphabet) {
    if (c === sig.at(-1)) continue;
    const r = await verifySig(SECRET, PATH, exp, sig.slice(0, -1) + c, NOW);
    assert.equal(r, "bad_sig");
    rejected++;
  }
  assert.equal(rejected, 63);
});

test("路徑：合法的 ts／m3u8／key", () => {
  assert.deepEqual(parsePath("/hls/1/seg_000.ts"), { kind: "ts", id: "1" });
  assert.deepEqual(parsePath("/hls/42/index.m3u8"), { kind: "m3u8", id: "42" });
  assert.deepEqual(parsePath("/hls/007/a-B_9.ts"), { kind: "ts", id: "007" });
  assert.deepEqual(parsePath("/key/12"), { kind: "key", id: "12" });
});

test("路徑：不合法一律 null", () => {
  for (const p of [
    "/hls/0/a.ts", "/hls/abc/a.ts", "/hls/-1/a.ts", "/hls/1/a.mp4", "/hls/1/a.ts/", "/hls/1/../2/a.ts",
    "/hls/1/a b.ts", "/hls/1/a.TS", "/hls/1/.ts", "/hls/1/a.b.ts", "/hls/1/%2e%2e.ts", "/hls//a.ts", "/hls/1",
    "/hls/18446744073709551616/a.ts", "/key/0", "/key/x", "/key/1/", "/key/1.key", "/other/1/a.ts", "/",
  ]) {
    assert.equal(parsePath(p), null, p);
  }
});

const H = (o) => new Headers(o);
const HOSTS = parseHosts("example.com, *.cdn.example.org ,localhost:8090");

test("防盜鏈：hostname 比對任何 port，精確 host:port，萬用字元尾綴", () => {
  assert.equal(hotlinkAllowed(H({ Origin: "https://example.com" }), HOSTS, false), true);
  assert.equal(hotlinkAllowed(H({ Origin: "http://example.com:8443" }), HOSTS, false), true);
  assert.equal(hotlinkAllowed(H({ Origin: "http://localhost:8090" }), HOSTS, false), true);
  assert.equal(hotlinkAllowed(H({ Origin: "http://localhost:9999" }), HOSTS, false), false);
  assert.equal(hotlinkAllowed(H({ Origin: "http://localhost" }), HOSTS, false), false);
  assert.equal(hotlinkAllowed(H({ Origin: "https://a.cdn.example.org" }), HOSTS, false), true);
  assert.equal(hotlinkAllowed(H({ Origin: "https://cdn.example.org" }), HOSTS, false), false);
});

test("防盜鏈：像白名單但不是的 host 擋下", () => {
  for (const o of ["https://evil.com", "https://example.com.evil.com", "https://notexample.com", "https://evil.com/example.com", "null", "not a url", "https://"]) {
    assert.equal(hotlinkAllowed(H({ Origin: o }), HOSTS, false), false, o);
  }
});

test("防盜鏈：Origin 優先於 Referer；沒有 Origin 才看 Referer", () => {
  assert.equal(hotlinkAllowed(H({ Origin: "https://evil.com", Referer: "https://example.com/p" }), HOSTS, false), false);
  assert.equal(hotlinkAllowed(H({ Origin: "https://example.com", Referer: "https://evil.com/p" }), HOSTS, false), true);
  assert.equal(hotlinkAllowed(H({ Referer: "https://example.com/watch?v=1" }), HOSTS, false), true);
  assert.equal(hotlinkAllowed(H({ Referer: "https://evil.com/watch" }), HOSTS, false), false);
});

test("防盜鏈：兩者都沒有時看 allowEmpty", () => {
  assert.equal(hotlinkAllowed(H({}), HOSTS, true), true);
  assert.equal(hotlinkAllowed(H({}), HOSTS, false), false);
});

test("cacheKeyUrl：去掉 exp／sig、保留其他 query", () => {
  assert.equal(cacheKeyUrl("https://e.com/hls/1/a.ts?exp=1&sig=x"), "https://e.com/hls/1/a.ts");
  assert.equal(cacheKeyUrl("https://e.com/hls/1/a.ts?sig=y&exp=2&v=3"), "https://e.com/hls/1/a.ts?v=3");
});

// ---- Worker 整合（不經快取的分支：400／403／405，不會碰 fetch 與 caches）----
const env = { HLS_SECRET: SECRET, ORIGIN_URL: "http://origin.invalid", ALLOWED_HOSTS: "example.com", ALLOW_EMPTY_REFERER: "true" };
const call = (url, headers = {}, method = "GET") => worker.fetch(new Request(url, { method, headers }), env, { waitUntil() {} });
const live = async (path) => {
  const exp = Math.floor(Date.now() / 1000) + 60;
  return `https://edge.test${path}?exp=${exp}&sig=${await sign(SECRET, path, exp)}`;
};

test("Worker：路徑不合法 400、方法不允許 405", async () => {
  assert.equal((await call("https://edge.test/hls/abc/a.ts?exp=1&sig=x")).status, 400);
  assert.equal((await call("https://edge.test/other")).status, 400);
  assert.equal((await call(await live(PATH), {}, "POST")).status, 405);
});

test("Worker：缺簽章／竄改／過期／盜鏈 都 403 且不洩漏正確簽章", async () => {
  const good = await live(PATH);
  const goodSig = new URL(good).searchParams.get("sig");
  const bodies = [];
  for (const [url, hdr, want] of [
    ["https://edge.test" + PATH, {}, "bad_sig"],
    [good.replace(/sig=./, "sig=Z"), { Origin: "https://example.com" }, "bad_sig"],
    [`https://edge.test${PATH}?exp=1&sig=${await sign(SECRET, PATH, 1)}`, { Origin: "https://example.com" }, "expired"],
    [good, { Origin: "https://evil.com" }, "hotlink"],
  ]) {
    const r = await call(url, hdr);
    assert.equal(r.status, 403, url);
    assert.equal(r.headers.get("X-Edge-Auth"), want);
    bodies.push(await r.text(), JSON.stringify([...r.headers]));
  }
  assert.ok(!bodies.join("").includes(goodSig), "回應不得含正確簽章");
});

test("Worker：/key/* 即使 ALLOW_EMPTY_REFERER=true 也要帶 Origin／Referer", async () => {
  const r = await call(await live("/key/1"));
  assert.equal(r.status, 403);
  assert.equal(r.headers.get("X-Edge-Auth"), "hotlink");
});
