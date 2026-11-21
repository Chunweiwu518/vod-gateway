# 認證走環境變數 CLOUDFLARE_API_TOKEN（權限：Zone WAF、Zone Rulesets（Cache Rules）、Workers Routes 的編輯），
# 不進程式碼。
provider "cloudflare" {}

locals {
  hls_or_key = "(starts_with(http.request.uri.path, \"/hls/\") or starts_with(http.request.uri.path, \"/key/\"))"

  # Origin 長得像 https://host 或 https://host:port；Referer 還會多一段 /path。
  # 每個樣式都以 "https://<白名單host>" 字面開頭，所以不會被 "https://evil.com/?u=https://example.com" 這種字串騙過。
  origin_patterns = flatten([
    for h in var.allowed_hosts : strcontains(h, ":")
    ? ["https://${h}", "http://${h}"]
    : ["https://${h}", "http://${h}", "https://${h}:*", "http://${h}:*"]
  ])
  referer_patterns = concat(local.origin_patterns, [for p in local.origin_patterns : "${p}/*"])

  origin_ok  = join(" or ", [for p in local.origin_patterns : format("any(http.request.headers[\"origin\"][*] wildcard %q)", p)])
  referer_ok = join(" or ", [for p in local.referer_patterns : format("http.referer wildcard %q", p)])

  # 與 Go／Worker 一致：Origin 優先，沒有才看 Referer。
  # 兩者都沒有的情況這裡放行（TS／m3u8 的 AllowEmpty=true），/key 另有一條規則擋。
  hotlink_expr = "${local.hls_or_key} and ((len(http.request.headers[\"origin\"]) gt 0 and not (${local.origin_ok})) or (len(http.request.headers[\"origin\"]) eq 0 and http.referer ne \"\" and not (${local.referer_ok})))"

  # 免費方案的 rate limiting 只能 10 秒的 period、10 秒的 mitigation timeout；
  # 挑戰類動作在 Free／Pro／Business 必須把 mitigation_timeout 設 0。
  key_mitigation_timeout = var.key_rate_limit_action == "block" ? 10 : 0
}

# (i) Cache Rules：TS 邊緣快取 1 天且 cache key 忽略 query（exp／sig 不同也共用同一份）；m3u8 與 /key 不快取。
# 注意：cache key 忽略 query 之後，快取命中不會再經過源站驗簽章，所以前面一定要有 Worker（見 route）先驗。
resource "cloudflare_ruleset" "cache" {
  zone_id     = var.zone_id
  name        = "vod-hls-cache"
  description = "HLS：TS 邊緣快取、m3u8 與 key 不快取"
  kind        = "zone"
  phase       = "http_request_cache_settings"

  rules = [
    {
      description = "HLS TS: edge cache, ignore query string in cache key"
      expression  = "http.host eq \"${var.hostname}\" and starts_with(http.request.uri.path, \"/hls/\") and http.request.uri.path.extension eq \"ts\""
      action      = "set_cache_settings"
      enabled     = true
      action_parameters = {
        cache = true
        edge_ttl = {
          mode    = "override_origin"
          default = var.ts_edge_ttl_seconds
        }
        cache_key = {
          custom_key = {
            query_string = {
              exclude = { all = true }
            }
          }
        }
      }
    },
    {
      description = "HLS m3u8 and /key: bypass cache"
      expression  = "http.host eq \"${var.hostname}\" and ((starts_with(http.request.uri.path, \"/hls/\") and http.request.uri.path.extension eq \"m3u8\") or starts_with(http.request.uri.path, \"/key/\"))"
      action      = "set_cache_settings"
      enabled     = true
      action_parameters = {
        cache = false
      }
    },
  ]
}

# (ii) WAF custom rules：防盜鏈。粗的一層，精確比對（含子網域、簽章）在 Worker 與源站。
resource "cloudflare_ruleset" "waf" {
  zone_id     = var.zone_id
  name        = "vod-hls-waf"
  description = "HLS／key 防盜鏈"
  kind        = "zone"
  phase       = "http_request_firewall_custom"

  rules = [
    {
      description = "Block /hls and /key when Origin/Referer host is not allowlisted"
      expression  = "http.host eq \"${var.hostname}\" and ${local.hotlink_expr}"
      action      = "block"
      enabled     = true
    },
    {
      description = "Block /key without any Origin or Referer"
      expression  = "http.host eq \"${var.hostname}\" and starts_with(http.request.uri.path, \"/key/\") and len(http.request.headers[\"origin\"]) eq 0 and http.referer eq \"\""
      action      = "block"
      enabled     = true
    },
  ]
}

# (iii) Rate limiting：/key/* 每 IP 每 10 秒超過門檻就處置。
# 免費方案限制（官方 Availability 表）：1 條規則、counting period 10 秒、mitigation timeout 10 秒、
# 只能用 IP 計數、規則運算式只能用 Path（和 Verified Bot）。
resource "cloudflare_ruleset" "ratelimit" {
  zone_id     = var.zone_id
  name        = "vod-key-ratelimit"
  description = "/key/* 限速"
  kind        = "zone"
  phase       = "http_ratelimit"

  rules = [
    {
      description = "Rate limit /key per IP"
      expression  = "starts_with(http.request.uri.path, \"/key/\")"
      action      = var.key_rate_limit_action
      enabled     = true
      ratelimit = {
        characteristics     = ["ip.src"]
        period              = 10
        requests_per_period = var.key_rate_limit_requests
        mitigation_timeout  = local.key_mitigation_timeout
      }
    },
  ]
}

# (iv) Worker route：把 /hls/* 與 /key/* 交給已部署的 Worker（先用 wrangler deploy 部署 edge/）。
resource "cloudflare_workers_route" "vod_edge" {
  for_each = toset(["/hls/*", "/key/*"])

  zone_id = var.zone_id
  pattern = "${var.hostname}${each.value}"
  script  = var.worker_script_name
}
