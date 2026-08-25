# InterGate · AI API 智能中转网关(Python 版)

> **开发者**: 恣桐
> 
> 对标 [RelayGo](https://github.com/RelayGo) / Android 版 InterGate 的轻量级 AI API 中转网关,
> 在 Termux / 任何 Linux 上运行,统一管理多家 AI 服务商 API Key,提供负载均衡、失败自动切换、
> 限流、响应缓存、额度监控、模型同步,并附带一个 Web 控制台。

第三方 AI 应用(ChatBox、NextChat、Open WebUI、自定义脚本...)只需把 `base_url` 指向本网关,
即可共享这一批受管理的高可用 Key。

## 核心特性

- **多提供商接入** — openai / anthropic / google / azure / opencode / custom,统一适配
- **6 种负载均衡策略** — 轮询、加权轮询、优先级、最少连接、响应时间、智能
- **失败自动切换** — 连续失败自动冷却与恢复,单请求最多切换 3 个 Key
- **模型列表同步** — 自动拉取各提供商模型,统一格式、能力推断
- **响应缓存** — 复用幂等 2xx 响应,降低重复请求成本(响应头 `x-relay-cache`)
- **多维限流** — IP / 全局 / Key RPM 限流,自适应 TPM(AIMD 动态调整)
- **额度与告警** — 每日配额、错误率监控、额度耗尽自动禁用
- **Key 安全管理** — AES-256-GCM 本地加密,明文不落盘、不写日志
- **Web 控制台** — 仪表盘、Key 管理、模型、日志、设置,一键启停

## 安装

```bash
# 1. 进入项目
cd ~/工作目录/InterGate

# 2. 安装依赖(Python 3.10+)
pip install -r requirements.txt
# 或: pip install fastapi uvicorn httpx cryptography

# 3. 启动 / 停止
./run.sh              # 后台启动
./run.sh status       # 查看运行状态
./run.sh restart      # 重启(改配置/更新后)
./run.sh stop         # 停止
```
> 重复执行 `./run.sh` 不会报端口占用(自动检测已在运行)。若出现
> `Errno 98 address already in use`,说明端口被旧进程或其它程序占用,
> 先 `./run.sh stop`,确认端口释放后再启动。

## 使用

启动后:

| 服务 | 地址 | 说明 |
|---|---|---|
| 代理网关 | `http://0.0.0.0:51234` | OpenAI 兼容 API(`/v1/chat/completions` 等) |
| Web 控制台 | `http://0.0.0.0:51235` | 浏览器打开,管理 Key / 模型 / 日志 / 设置 |

1. 打开 Web 控制台 → 「Key 管理」→ 添加各提供商 API Key。
2. (可选)「模型」页点击「同步模型」拉取可用模型。
3. 第三方应用设置:
   ```
   base_url = http://<手机IP>:51234/v1
   api_key  = <网关 Key>(设置里配置了网关 Key 时才需要)
   ```

高级用法(请求头):

| 请求头 | 作用 |
|---|---|
| `x-relay-provider: openai` | 强制指定提供商 |
| `x-relay-key: 名称或ID` | 强制指定某个 Key |
| `x-relay-cache` | 缓存命中时网关返回该头 |

## 目录结构

```
InterGate/
├── main.py              # 入口(同时启动网关 + Web)
├── run.sh               # 一键启动脚本
├── gateway.py           # 代理网关(路由/鉴权/缓存/限流/失败切换/SSE/用量)
├── webapp.py            # Web 管理 API
├── web/index.html       # 控制台单页
├── config/              # 常量与用户设置
├── crypto/              # AES-256-GCM 密钥加密
├── db/                  # SQLite 存储(keys/settings/logs/stats)
├── models/              # 数据模型
├── providers/           # 提供商识别与上游转发
├── services/            # key_manager/load_balancer/rate_limiter/cache/quota/sync
├── data/                # 运行时数据(自动创建:.master_key + intergate.db)
└── tests/               # 冒烟测试
```

## 安全说明

- API Key 使用 AES-256-GCM 加密存储,主密钥在 `data/.master_key`(权限 0600)。
- 网关默认不鉴权(局域网内均可调用);建议在「设置」中配置主网关 Key 后再暴露到局域网。
  配置后,调用网关需携带 `Authorization: Bearer <网关 Key>`,并支持按权限拆分的附加网关密钥。
- 网关管理接口(`/relay/stats`、`/relay/report`、`/relay/version`、`/relay/cache`)同样需要网关 Key
  鉴权;健康检查 `/health`、`/healthz`、`/ping` 保持公开。
- Web 控制台:未设置管理台密码时仅本机(`127.0.0.1`)可访问;设置密码后局域网可访问。
  登录 token 24 小时有效,登录接口有失败限速(60 秒内 5 次失败即临时锁定)。
- 生产环境请做好网络访问控制,遵守各 AI 服务商使用条款。

## 测试

```bash
python3 tests/unit_smoke.py     # 纯逻辑冒烟(不依赖网络)
python3 tests/gateway_smoke.py  # 网关+Web 集成冒烟(需先安装依赖)
```


## 版本历史

### v1.1.0 (2026-08-25)

> 相对原版的大版本更新：新增多 Worker 并发、模型服务商前缀、自适应用量控制，
> 并完成大规模性能优化与若干 Bug 修复。

#### 新功能

- **多 Worker 并发模式**：网关可多进程并行服务，显著提升并发吞吐；Web 控制台、模型同步在主进程统一管理。
- **模型对外暴露 ID 加服务商前缀**：`/v1/models` 返回的模型 ID 形如 `日日新-deepseek-v4-flash`、`DeepSeek-deepseek-v4-flash`，一眼可知来源服务商；不同服务商 / 不同 `base_url` 的模型彼此隔离，重名模型自动唯一化，互不干扰。
- **调用自动路由到对应服务商**：AI 客户端用带前缀模型名调用时，网关自动反解析并精确路由到提供该模型的那个 Key（相当于点对点隔离）；上游请求自动回写真实模型名，日志/统计/费用均按真实模型记价，不受前缀影响。
- **自适应 TPM 用量控制（AIMD）**：以"乘性减、加性增"动态调整每个 Key 的每分钟 token 上限，冷启动宽松、触发限流后自动降速并逐步收敛。
- **负载均衡轮询升级**：轮询改为按"服务商 + 端点 + 分组"维护独立游标，同名 / 同端点的多个 Key 真正轮流使用，避免一直固定选中第一个；候选 Key 增删或状态变化时自动重建游标。
- **上游连接池可配置**：连接数 / 空闲保持可调节，适配不同上游与并发规模。
- **告警持久化**：额度耗尽、错误率过高等历史告警重启后不再丢失。
- **Web 控制台一键重启**：修改端口 / 并发 / 连接池等需重启生效的设置后，页面提示并可直接一键重启服务，无需手动到终端操作。

#### 性能优化

- **后台批量异步写库**：请求日志、统计、用量写回不阻塞事件循环，攒批一次性落库，高并发下吞吐大幅提升。
- **SQLite 开启 WAL 模式**：读写并发显著改善，多进程下写锁不再相互阻塞，避免繁忙报错。
- **请求体单次解析**：模型名 / 流式标记 / token 估算复用同一份解析结果，降低大请求的 CPU 开销。
- **概览聚合走缓存**：仪表盘、健康检查的今日统计带短时缓存，不再每次全表扫描日志。
- **鉴权热路径零读库**：网关密钥校验走内存缓存，控制台改动即时生效，每请求省去一次数据库读取。
- **限流按 token 加权记账**：高 token 请求一次生效，替代原先逐条叠加。
- **负载均衡延迟记录裁剪优化**：历史样本裁剪避免无效拷贝。

#### Bug 修复

- **Key 管理页"今日用量"跨天显示错误**：昨天用过、今天还没使用时会错误显示昨日残留用量，现按天正确归零。
- **统计落库后读取不到（WAL 快照不可见）**：改用独立写连接，保证读到的始终是已落库的最新数据。
- **Key 自动恢复偶发死锁**：冷却过期的 Key 在自动恢复时不再因重复加锁而卡死。

#### 测试

- 新增完整自动化测试套件：覆盖网关请求流程、缓存、限流、负载均衡、密钥管理等，替代原先仅有的冒烟脚本。
- 适配异步批量写语义，保证行为断言可靠。
- 全量回归：`pytest` 71 项 + 冒烟 62 项全部通过。

---


### v1.0.0 (初始版本)

首次发布，基础功能：多提供商代理、6 种负载均衡策略、失败自动切换、模型同步、响应缓存、多维限流、额度告警、Key 加密管理、Web 控制台。



---

## 给开发者

### 代码结构

```
InterGate/
├── main.py              # 入口(同时启动网关 + Web, 支持多 Worker)
├── run.sh               # 一键启动/停止/重启/状态
├── gateway.py           # 代理网关(路由/鉴权/缓存/限流/失败切换/SSE/用量)
├── webapp.py            # Web 管理 API(仪表盘/Key/模型/日志/设置)
├── web/                 # 控制台单页(PWA)
├── config/              # 常量、定价、用户设置
├── crypto/              # AES-256-GCM 密钥加密
├── db/                  # SQLite 存储(keys/settings/logs/stats/alerts)
├── models/              # 数据模型
├── providers/           # 提供商识别与上游转发
├── services/            # key_manager/load_balancer/rate_limiter/cache/quota/sync/...
└── tests/               # pytest + 冒烟测试
```

### 运行测试

```bash
# 安装依赖后, 运行完整测试套件(不依赖网络)
python3 -m pytest tests/ -q

# 纯逻辑冒烟(不依赖网络)
python3 tests/unit_smoke.py

# 网关 + Web 集成冒烟
python3 tests/gateway_smoke.py
```

### 第三方 AI 应用接入

```text
base_url = http://<服务器IP>:51234/v1
api_key  = <网关 Key>   # 设置里配置了网关 Key 时才需要

# 可选: 用带服务商前缀的模型名, 精确路由到指定服务商
model = 日日新-deepseek-v4-flash   # 等价 deepseek-v4-flash, 但固定走 日日新
```

### 环境变量

| 变量 | 作用 |
|---|---|
| `INTERGATE_DATA_DIR` | 数据目录(默认项目下 `data/`), 主密钥与数据库存于此 |
| `TERMUX` | 自动识别 Termux 环境(用于端口占用处理等) |

### LICENSE

本项目采用 **GNU Affero General Public License v3.0 (AGPL-3.0)**。

> **原作者**: 恣桐 (InterGate Contributors)
> **版权年份**: 2026

#### 核心版权条款（copyleft · 必须开源改版）

1. **允许使用与修改**：任何人均可自由使用、复制、修改、分发本软件。
2. **改版必须开源（强 Copyleft）**：**非本项目作者发布、自行修改后的代码，必须以
   AGPL-3.0（或兼容的开源许可证）公开其改版后的完整源代码**，包括通过网络/服务端
   形式对外提供的场景（详见 AGPL-3.0 第 13 条「远程网络交互」条款）。
3. **保留署名**：修改或分发时必须保留原作者（恣桐）的版权声明与署名，不得删除。
4. **禁止闭源衍生**：不得将本软件（或其修改版）用于闭源商业分发，违反者将自动
   丧失本协议授予的权利。

完整许可证全文见 [LICENSE](LICENSE)（GNU AGPL v3.0）。
