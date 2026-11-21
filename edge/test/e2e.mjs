// 跨語言端到端：Go 簽網址（cmd/signurl）→ 本機 `wrangler dev` 的 Worker 驗證與快取 → 假源站（記錄被打次數）。
// 不需要 Cloudflare 登入（wrangler dev 本機模式）。結束時一定殺掉 wrangler／workerd 與假源站。
// 用法：cd edge && node test/e2e.mjs      （或 bash test/e2e.sh）
import http from "node:http";
import { spawn, execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import assert from "node:assert/strict";

const EDGE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const ROOT = path.resolve(EDGE, "..");
const ORIGIN_PORT = Number(process.env.E2E_ORIGIN_PORT || 18791);
const EDGE_PORT = Number(process.env.E2E_EDGE_PORT || 18792);
const SECRET = "e2e-secret-" + Math.random().toString(36).slice(2);
const GOOD = "https://example.com";
const BASE = `http://127.0.0.1:${EDGE_PORT}`;

// --- 假源站：記錄每個請求 ---
const hits = [];
const origin = http.createServer((req, res) => {
  hits.push({ url: req.url, origin: req.headers.origin, referer: req.headers.referer });
  const p = new URL(req.url, "http://x").pathname;
  if (p.endsWith(".ts")) {
    res.writeHead(200, { "Content-Type": "video/mp2t", "Cache-Control": "public, max-age=300" });
    res.end(Buffer.from("TS-BYTES:" + p));
  } else if (p.endsWith(".m3u8")) {
    res.writeHead(200, { "Content-Type": "application/vnd.apple.mpegurl", "Cache-Control": "no-store" });
    res.end("#EXTM3U\n");
  } else if (p.startsWith("/key/")) {
    res.writeHead(200, { "Content-Type": "application/octet-stream", "Cache-Control": "no-store" });
    res.end(Buffer.alloc(16, 7));
  } else {
    res.writeHead(404).end();
  }
});
await new Promise((r) => origin.listen(ORIGIN_PORT, "127.0.0.1", r));

// --- Go 簽章 ---
const sign = (p, extra = ["-ttl", "60"]) =>
  execFileSync(path.join(ROOT, "scripts/gorun"), ["go", "run", "./cmd/signurl", "-secret", SECRET, "-path", p, ...extra], { cwd: ROOT, encoding: "utf8" }).trim();

// 埠被舊的行程占著就直接報錯，避免測到上一輪殘留的 Worker
try {
  await fetch(BASE + "/");
  console.error(`port ${EDGE_PORT} 已有服務在回應，請先清掉殘留的 wrangler／workerd`);
  process.exit(2);
} catch {}

// 本機 Cache API 會寫進 persist 目錄；每輪先清空，否則上一輪的快取會讓第一個請求變成 HIT
import fs from "node:fs";
const STATE = path.join(EDGE, ".wrangler/e2e-state");
fs.rmSync(STATE, { recursive: true, force: true });

// --- 起 wrangler dev（獨立 process group，方便整組殺）---
const wr = spawn(
  path.join(EDGE, "node_modules/.bin/wrangler"),
  ["dev", "--local", "--port", String(EDGE_PORT), "--ip", "127.0.0.1",
   "--var", `HLS_SECRET:${SECRET}`, "--var", `ORIGIN_URL:http://127.0.0.1:${ORIGIN_PORT}`,
   "--var", "ALLOWED_HOSTS:example.com,*.cdn.example.org,localhost:8090", "--var", "ALLOW_EMPTY_REFERER:true",
   "--persist-to", STATE],
  { cwd: EDGE, detached: true, stdio: ["ignore", "pipe", "pipe"], env: { ...process.env, WRANGLER_SEND_METRICS: "false", CI: "1" } },
);
let wrLog = "";
wr.stdout.on("data", (d) => (wrLog += d));
wr.stderr.on("data", (d) => (wrLog += d));
let cleaned = false;
function cleanup() {
  if (cleaned) return;
  cleaned = true;
  // 先 SIGTERM 讓 wrangler 自己收掉 workerd，等一下再整組 SIGKILL，確保不留常駐程序
  try { process.kill(-wr.pid, "SIGTERM"); } catch {}
  execFileSync("sleep", ["2"]);
  try { process.kill(-wr.pid, "SIGKILL"); } catch {}
  origin.close();
}
process.on("exit", cleanup);
for (const s of ["SIGINT", "SIGTERM"]) process.on(s, () => process.exit(130));

let pass = 0, fail = 0;
const check = async (name, fn) => {
  try { await fn(); pass++; console.log(`ok   ${name}`); }
  catch (e) { fail++; console.log(`FAIL ${name}\n     ${e.message.split("\n").join("\n     ")}`); }
};
const get = (u, headers = {}) => fetch(BASE + u, { headers, redirect: "manual" });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

try {
  for (let i = 0; i < 120; i++) {
    try { await fetch(BASE + "/"); break; } catch { await sleep(500); }
    if (i === 119) throw new Error("wrangler dev 起不來:\n" + wrLog);
  }

  const seg0 = "/hls/1/seg_000.ts";
  const url60 = sign(seg0, ["-ttl", "60"]);
  const url120 = sign(seg0, ["-ttl", "120"]);
  assert.notEqual(url60, url120, "兩個簽章網址必須不同");

  await check("有效簽章 TS -> 200、MISS、源站 1 次、內容正確、源站收到 Origin 與原 query", async () => {
    const r = await get(url60, { Origin: GOOD });
    assert.equal(r.status, 200);
    assert.equal(r.headers.get("x-edge-cache"), "MISS");
    assert.equal(await r.text(), "TS-BYTES:" + seg0);
    assert.equal(hits.length, 1);
    assert.equal(hits[0].origin, GOOD);
    assert.equal(hits[0].url, url60);
  });

  // Cache.put 走 waitUntil，給它一點時間
  let second;
  await check("不同 exp／sig 的同一個 TS -> 200、HIT、源站次數仍是 1（cache key 共用）", async () => {
    for (let i = 0; i < 20; i++) {
      second = await get(url120, { Origin: GOOD });
      if (second.headers.get("x-edge-cache") === "HIT") break;
      await sleep(250);
    }
    assert.equal(second.status, 200);
    assert.equal(second.headers.get("x-edge-cache"), "HIT");
    assert.equal(await second.text(), "TS-BYTES:" + seg0);
    assert.equal(hits.length, 1, `源站被打 ${hits.length} 次`);
  });

  await check("沒有 Origin／Referer 的 TS（ALLOW_EMPTY_REFERER=true）-> 200 HIT", async () => {
    const r = await get(url60);
    assert.equal(r.status, 200);
    assert.equal(r.headers.get("x-edge-cache"), "HIT");
  });

  await check("Referer 在白名單（無 Origin）-> 200 HIT", async () => {
    const r = await get(url60, { Referer: "https://sub.cdn.example.org/watch?v=1" });
    assert.equal(r.status, 200);
    assert.equal(r.headers.get("x-edge-cache"), "HIT");
  });

  const before = hits.length;
  const [p, q] = url60.split("?");
  const params = new URLSearchParams(q);

  await check("竄改 sig -> 403 bad_sig", async () => {
    const sig = params.get("sig");
    const bad = (sig[0] === "A" ? "B" : "A") + sig.slice(1);
    const r = await get(`${p}?exp=${params.get("exp")}&sig=${bad}`, { Origin: GOOD });
    assert.equal(r.status, 403);
    assert.equal(r.headers.get("x-edge-auth"), "bad_sig");
    assert.ok(!(await r.text()).includes(sig), "回應不得含正確簽章");
  });

  await check("竄改 exp（沿用舊 sig）-> 403 bad_sig", async () => {
    const r = await get(`${p}?exp=${Number(params.get("exp")) + 3600}&sig=${params.get("sig")}`, { Origin: GOOD });
    assert.equal(r.status, 403);
    assert.equal(r.headers.get("x-edge-auth"), "bad_sig");
  });

  await check("缺 exp／sig -> 403", async () => {
    assert.equal((await get(p, { Origin: GOOD })).status, 403);
  });

  await check("過期（Go 用 -exp 簽出已過期但簽章正確的網址）-> 403 expired，即使快取裡已有這個 TS", async () => {
    const r = await get(sign(seg0, ["-exp", "1000000000"]), { Origin: GOOD });
    assert.equal(r.status, 403);
    assert.equal(r.headers.get("x-edge-auth"), "expired");
  });

  await check("簽的是別的路徑（拿 seg_000 的簽章打 seg_001）-> 403", async () => {
    const r = await get(`/hls/1/seg_001.ts?${q}`, { Origin: GOOD });
    assert.equal(r.status, 403);
  });

  await check("Origin 不在白名單 -> 403 hotlink（即使已快取）", async () => {
    const r = await get(url60, { Origin: "https://evil.com" });
    assert.equal(r.status, 403);
    assert.equal(r.headers.get("x-edge-auth"), "hotlink");
  });

  await check("Referer 不在白名單 -> 403 hotlink", async () => {
    const r = await get(url60, { Referer: "https://example.com.evil.com/x" });
    assert.equal(r.status, 403);
    assert.equal(r.headers.get("x-edge-auth"), "hotlink");
  });

  await check("Origin 優先：Origin 壞、Referer 好 -> 403", async () => {
    const r = await get(url60, { Origin: "https://evil.com", Referer: "https://example.com/" });
    assert.equal(r.status, 403);
  });

  for (const bad of ["/hls/abc/seg_000.ts", "/hls/0/seg_000.ts", "/hls/1/seg_000.mp4", "/hls/1/a.b.ts", "/hls/1/seg%2F000.ts", "/key/x", "/other/1/seg_000.ts"]) {
    await check(`路徑不合法 ${bad} -> 400（連簽章都不看）`, async () => {
      const r = await get(`${bad}?${q}`, { Origin: GOOD });
      assert.equal(r.status, 400);
    });
  }

  await check("上面所有被擋的請求都沒到源站", async () => {
    assert.equal(hits.length, before, `源站多被打 ${hits.length - before} 次`);
  });

  await check("另一個 TS（seg_001）-> MISS，源站 +1", async () => {
    const r = await get(sign("/hls/1/seg_001.ts"), { Origin: GOOD });
    assert.equal(r.status, 200);
    assert.equal(r.headers.get("x-edge-cache"), "MISS");
    assert.equal(hits.length, before + 1);
  });

  await check("m3u8：驗簽後直接轉源站，不快取（兩次都打源站、BYPASS）", async () => {
    const n = hits.length;
    const u = sign("/hls/1/index.m3u8");
    for (let i = 0; i < 2; i++) {
      const r = await get(u, { Origin: GOOD });
      assert.equal(r.status, 200);
      assert.equal(r.headers.get("x-edge-cache"), "BYPASS");
      assert.equal(await r.text(), "#EXTM3U\n");
    }
    assert.equal(hits.length, n + 2);
  });

  await check("m3u8 簽章錯 -> 403，不到源站", async () => {
    const n = hits.length;
    const r = await get("/hls/1/index.m3u8?exp=4102444800&sig=AAAA", { Origin: GOOD });
    assert.equal(r.status, 403);
    assert.equal(hits.length, n);
  });

  await check("/key/1：有 Origin -> 200、不快取（兩次都打源站）", async () => {
    const n = hits.length;
    const u = sign("/key/1");
    for (let i = 0; i < 2; i++) {
      const r = await get(u, { Origin: GOOD });
      assert.equal(r.status, 200);
      assert.equal(r.headers.get("x-edge-cache"), "BYPASS");
      assert.equal((await r.arrayBuffer()).byteLength, 16);
    }
    assert.equal(hits.length, n + 2);
  });

  await check("/key/1：沒帶 Origin／Referer -> 403（key 不允許空來源）", async () => {
    const n = hits.length;
    const r = await get(sign("/key/1"));
    assert.equal(r.status, 403);
    assert.equal(hits.length, n);
  });

  await check("POST -> 405", async () => {
    const r = await fetch(BASE + url60, { method: "POST", headers: { Origin: GOOD } });
    assert.equal(r.status, 405);
  });
} catch (e) {
  fail++;
  console.log("FATAL " + e.message);
} finally {
  cleanup();
}
console.log(`\ne2e: ${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
