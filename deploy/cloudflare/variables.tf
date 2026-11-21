variable "zone_id" {
  description = "Cloudflare zone ID（網域所在的 zone）。不寫死，用 TF_VAR_zone_id 或 -var 帶入。"
  type        = string
}

variable "hostname" {
  description = "對外的影片網域，例如 video.example.com；Worker route 與規則都只作用在這個 host。"
  type        = string
}

variable "worker_script_name" {
  description = "已用 wrangler deploy 部署的 Worker 名稱（edge/wrangler.toml 的 name）。"
  type        = string
  default     = "vod-edge"
}

variable "allowed_hosts" {
  description = <<-EOT
    防盜鏈白名單（Origin／Referer 的 host）。"example.com" 比對任何 port，"localhost:8090" 精確比對 host:port。
    WAF 層只支援這兩種字面比對，不支援 "*.example.com"：wildcard 的 * 會吃掉 "/"，子網域要逐一列出
    （精確的子網域比對在 Worker／源站層做；正規表示式 matches 要 Business 以上）。
  EOT
  type        = list(string)

  validation {
    condition     = length(var.allowed_hosts) > 0 && alltrue([for h in var.allowed_hosts : !strcontains(h, "*") && !strcontains(h, "\"") && !strcontains(h, "\\")])
    error_message = "allowed_hosts 不可為空，且不可含 * 、雙引號或反斜線。"
  }
}

variable "ts_edge_ttl_seconds" {
  description = "TS 的邊緣快取 TTL（秒），預設 1 天。"
  type        = number
  default     = 86400
}

variable "key_rate_limit_requests" {
  description = "/key/* 每個 IP 在 10 秒內允許的請求數，超過就處置。"
  type        = number
  default     = 10
}

variable "key_rate_limit_action" {
  description = "超過門檻的處置：block 或 managed_challenge。"
  type        = string
  default     = "block"

  validation {
    condition     = contains(["block", "managed_challenge"], var.key_rate_limit_action)
    error_message = "key_rate_limit_action 只能是 block 或 managed_challenge。"
  }
}
