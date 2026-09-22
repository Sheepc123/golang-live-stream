# 路线图（2026-09 起）

基于 docs/newplan.md 和当前代码状态重新排期。原则不变：先单机、再并发、再分布式、最后部署。

## 当前状态快照

| 阶段 | 状态 | 备注 |
|---|---|---|
| I 可观测性 | ✅ 完成 | zap / trace_id / Prometheus / GORM logger 全部落地 |
| II Docker | ❌ 未开始 | compose 里只有 Kafka + Prometheus + Grafana，没有 MySQL / Redis / Dockerfile |
| III 性能改造 | 🟡 80% | III.1–III.5、III.7 完成；III.6 分区键、III.8、III.9、III.10 部分未做 |
| IV 压测 | ❌ 未开始 | 实验 B / C 的开关刚接上 |
| V 礼物系统 | ❌ 未开始 | |
| VI 多实例 | ❌ 未开始 | 仍是 PSubscribe("ws:room:*") 通配订阅 |
| VII K8s | ❌ 可选 | |

---

## 步骤 0：收口当前提交（半天）

工作区里有 14 个文件、约 500 行未提交改动，先让它编译、测试通过、提交。

- [ ] 新建 `ws/sink.go`：`MsgSink` 接口 + `KafkaSink` + `SyncDBSink`
- [ ] `ws/manager.go`：`producer` 字段换成 `sink MsgSink`，`PersistMsg` 委托给 sink
- [ ] `ws/message.go`：补 `NewJoinMessage` / `NewLeaveMessage`
- [ ] `ws/action_like.go`、`ws/handler.go`：加 `instant` 开关（实验 C 对照组）
- [ ] `router/router.go`：按 `cfg.Experiment` 选 sink、决定是否启动 Aggregator
- [ ] `cmd/server/main.go`：接上 `infra.StartPprof`、`cfg.MySQL.AutoMigrate`
- [ ] `service/room_service.go`：`RoomList` 不再吞错误
- [ ] `metrics/metrics.go`：加 `SyncDBWriteDuration`
- [ ] 清理 `ws/aggregator.go` `flushRoomEvents` 里那段讨论式注释，改成一句结论

验收：
```
go build ./... && go vet ./... && go test ./...
```
三种配置各冒烟一次：默认 / `EXP_LEGACY_SYNC_DB_WRITE=true` / `EXP_LEGACY_INSTANT_COUNTERS=true`。
`curl localhost:6060/debug/pprof/` 有响应。

提交信息：`feat(experiment): add MsgSink abstraction and legacy toggles for benchmark control groups`

---

## 步骤 1：阶段 III 收尾（2–3 天）

### 1.1 Kafka 分区键改 RoomID（10 分钟）

- [ ] `ws/sink.go` `KafkaSink.Persist`：`msg.UserID` → `msg.RoomID`
- [ ] `deployments/docker-compose.yml`：`KAFKA_NUM_PARTITIONS: 3` → `8`（已有的 danmu topic 分区数不会自动变，需要 `kafka-topics.sh --alter` 或删了重建）

为什么现在可以改：sent_at 排序已落地，历史消息顺序不再依赖分区内顺序；按房间分区后 consumer 每批数据的 room_id 集中，InnoDB 二级索引写入局部性更好。

验收：`kafka-ui` 里看 danmu topic，同一房间的消息落在同一分区。

### 1.2 限流与防护（1 天）⭐ 压测前必须有

没有这层保护，压测客户端一开就把服务打死，测出来的是「谁先崩」而不是容量。

- [ ] 弹幕令牌桶：`ws/client.go` 加 `limiter *rate.Limiter`（`golang.org/x/time/rate`，新依赖），`rate.NewLimiter(2, 5)` = 每秒 2 条、突发 5 条。`ChatAction.Execute` 开头 `if !c.limiter.Allow()` 就 `SendMsgOnlyOne(NewErrorMessage(...))` 并 return，同时 `metrics.WSDropped{reason="rate_limited"}` +1
- [ ] 全局连接上限：`ws/manager.go` 加 `maxConns int` 和 `atomic.Int64` 计数，`TrackConn` 返回 bool；`handler.go` 在 `Upgrade` **之前**检查，超了直接 `503`。上限从 config 读（`server.max_ws_conns`，默认 0 = 不限）
- [ ] 单用户连接上限：`Manager` 加 `userConns map[int64]int`（受 `mu` 保护），`Register` 时 +1、`Unregister` 时 -1，超过 `server.max_conns_per_user`（默认 5）拒绝。⚠️ 这是单实例内的限制，多实例下要挪到 Redis，阶段 VI 再说
- [ ] `config.go` / `config.yaml` 加上述三个字段
- [ ] 前端 `liveSocket.ts` / `RoomDetailView.vue`：收到 `type=error` 已经会显示，确认限流提示能看见即可

验收：
- 连发 10 条弹幕，第 6 条起收到 error 消息
- 同一 token 开 6 个标签页，第 6 个握手失败
- `/metrics` 里 `live_ws_dropped_total{reason="rate_limited"}` 在增长

学习点：令牌桶 vs 漏桶 / 为什么在 Upgrade 前拒绝（握手后再断代价高得多）/ `atomic` vs `mutex` 的选择

### 1.3 补关键测试（1 天）

- [ ] `live/online_counter_test.go`：用 `github.com/alicebob/miniredis/v2` 测 Lua 脚本 —— 同一用户两个连接 Join 后 HLEN 仍为 1；Leave 一次后仍为 1，再 Leave 为 0；peak 只升不降
- [ ] `ws/broadcast_pool_test.go`：同一 roomId 的 job 总是落到同一队列（`roomId % workers`）；队列满时 `Submit` 返回 false 且不阻塞
- [ ] `repo/msg_repo_integration_test.go` 追加：插入 sent_at 乱序的三条，`ListBySessionID` 返回升序；sent_at 相同时按 id 升序
- [ ] `go test -race ./...` 全绿

### 1.4 小修（半天，可穿插）

- [ ] `live/like_counter.go` `Incr`：INCR + EXPIRE 是两次往返，改成 pipeline 或 Lua 一次搞定（点赞是最高频写路径）
- [ ] `cmd/consumer/main.go`：`:9101` 硬编码 → `config.ServerConfig.MetricsPort`
- [ ] `model/room.go` `UpdateRoomRequest.Status` 字段没人用，删掉，避免误导
- [ ] `LoginView.vue` 「忘记密码」按钮无功能，先隐藏

### 1.5 暂缓：III.8 WritePump 合并写

等步骤 3 的 pprof 火焰图出来再决定。如果 CPU 大头不在 `syscall.Write`，做了也白做。

---

## 步骤 2：阶段 II Docker 一键启动（2–3 天）

目标：干净机器 `git clone && make up` 后浏览器能用。步骤 4 的多实例验证完全依赖它。

### 2.1 补全 docker-compose.yml

- [ ] `mysql:8.0`：挂 volume、`MYSQL_DATABASE=live_stream`、healthcheck 用 `mysqladmin ping`
- [ ] `redis:7-alpine`：healthcheck 用 `redis-cli ping`
- [ ] kafka 加 healthcheck（`kafka-broker-api-versions.sh --bootstrap-server localhost:9092`）
- [ ] `server` / `consumer` 服务：`depends_on` 三个基础设施都用 `condition: service_healthy`
- [ ] `server` 起 1 个实例即可，多实例留给阶段 VI
- [ ] `frontend` 服务：nginx 托管静态文件 + 反代
- [ ] `prometheus.yml` 里 `host.docker.internal:8080` → `server:8080`，`:9101` → `consumer:9101`

### 2.2 backend/Dockerfile（多阶段）

```dockerfile
FROM golang:1.26 AS builder
ARG CMD=server
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /app ./cmd/${CMD}

FROM gcr.io/distroless/static
WORKDIR /
COPY --from=builder /app /app
COPY configs/config.yaml /configs/config.yaml
ENTRYPOINT ["/app"]
```

注意 `config.Load("configs/config.yaml")` 是相对路径，WORKDIR 必须和它对得上。

### 2.3 frontend/Dockerfile + nginx.conf

- [ ] `pnpm build` → nginx 静态托管
- [ ] `/api` 反代到 `server:8080`，WebSocket 要加 `Upgrade` / `Connection` 头，`proxy_read_timeout` 放大到 3600s（默认 60s 会把空闲长连接掐断）

### 2.4 配置外置

容器里全靠环境变量：`DB_HOST=mysql`、`REDIS_HOST=redis`、`KAFKA_BROKERS=kafka:29092`、`DB_AUTO_MIGRATE=true`（只给 server，consumer 设 false）。`.env` 只在本地开发用。

### 2.5 Makefile

`make up / down / logs / build / test / bench`

### 2.6 根目录 README

架构图（ASCII 即可）/ 技术选型理由 / 启动步骤 / 接口列表 / WS 协议。每个阶段结束后追加。

验收：
- 干净机器 `make up` 后：注册 → 登录 → 开播 → 发弹幕 → 刷新看历史，全程通
- `docker images` backend 镜像 < 30MB

学习点：多阶段构建 / distroless / healthcheck 与 depends_on / 容器网络 DNS / 12-Factor 配置

---

## 步骤 3：阶段 IV 压测与调优（3–5 天）⭐ 整个项目的核心产出

### 3.1 cmd/benchmark/main.go

参数：`-target ws://host:8080` `-conns 1000` `-rooms 10` `-msg-rate 1.0`（每连接每秒）`-duration 60s` `-jwt-secret xxx` `-admin-user admin0 -admin-pass 123456`

流程：
1. 用 admin 登录，对 rooms 1..N 调 `POST /live/start`（弹幕需要活跃 session）
2. 用 `-jwt-secret` 本地签发 N 个不同 user_id 的 token（`token.GenerateAccessToken` 直接可用，不用真注册）
3. 阶梯建连，记录成功/失败数
4. 每个连接按 `-msg-rate` 发 chat，内容里带发送时间戳
5. 收到 chat 时算 `now - msg.timestamp` = 端到端延迟（同一台机器时钟一致）
6. 结束时输出：建连成功率、上行 QPS、下行接收总数、延迟 P50/P95/P99、丢包率（`期望接收 = 发送数 × 房间人数` 与实际接收对比）

### 3.2 环境准备

```
ulimit -n 200000
sysctl -w net.core.somaxconn=65535
sysctl -w net.ipv4.tcp_max_syn_backlog=65535
sysctl -w net.ipv4.ip_local_port_range="1024 65535"
```

⚠️ WSL2 跑不出真实数字。压测客户端和服务端必须是两台机器。云服务器按量付费两台 4C8G，两天不到 20 块。
⚠️ 单机对单个 IP:端口最多约 28000 个临时端口，服务端要多开几个端口或客户端绑多个 IP。

### 3.3 三组对照实验

| 实验 | 对照 | 怎么切换 |
|---|---|---|
| A | 逐条 WriteJSON vs 预序列化 | 旧版本用 `git worktree add ../old 48eb77a` 单独构建 |
| B | 同步写库 vs Kafka 异步 | `EXP_LEGACY_SYNC_DB_WRITE=true` |
| C | 每次点赞广播 vs 1 秒聚合 | `EXP_LEGACY_INSTANT_COUNTERS=true` |

每组：固定连接数阶梯 1k → 5k → 1w → 3w，找拐点。看 Grafana 里 `live_ws_dropped_total`、`live_broadcast_duration_seconds`、`live_consumer_lag_seconds`。

### 3.4 pprof

压测中抓 `curl localhost:6060/debug/pprof/profile?seconds=30 > cpu.prof`，`go tool pprof -http=:8081 cpu.prof` 看火焰图。同时抓 heap 验证「单连接 < 1 KB」，抓 goroutine 验证「≈ 2×连接数 + 常量」。

火焰图决定下一步：
- 大头在 `syscall.Write` → 做 III.8 合并写
- 在 `runtime.mapaccess` / 锁 → `Manager.rooms` 分片
- 在 `json.Marshal` → 换 sonic / 手写序列化

### 3.5 docs/benchmark.md

环境 / 方法 / 数据表 / 火焰图截图 / 瓶颈分析 / 结论。

诚实性要求：
- 「下行 QPS」必须注明是扇出计数
- 跑出自己的数，哪怕只有 3 万连接
- 每个数字都要能回答：卡在哪？为什么是这个数？再往上要改什么？

验收：一份能撑住 20 分钟追问的报告。

---

## 步骤 4：阶段 VI 多实例验证（2–3 天）

### 4.1 PSubscribe 通配 → 动态订阅

问题：`redis_broker.go` 每个实例都收全站消息，3 实例 = 2/3 流量纯浪费。

改法：
- `Manager.Register`：房间第一个人进来时 `m.pubsub.PSubscribe(ctx, "ws:room:{id}:*")`（频道名带 msgType，所以仍是 pattern，但范围缩到单房间）
- `Manager.Unregister`：房间最后一人离开时 `PUnsubscribe`
- go-redis 的 `PubSub` 支持在已有订阅上动态增删，不用重建连接
- 订阅是网络调用，不能在持 `mu` 的时候做；先改 map、释放锁、再订阅

### 4.2 三实例 + nginx

- [ ] compose 里 `server` 起 3 个副本（`deploy.replicas` 或写三个 service）
- [ ] nginx `upstream` 用 `ip_hash` 保证 WS 连接粘性
- [ ] 只有一个实例 `DB_AUTO_MIGRATE=true`

### 4.3 验证

- [ ] 用户 A 连实例 1、用户 B 连实例 2，A 发弹幕 B 能收到
- [ ] 在线人数跨实例正确（Redis Hash 是全局的，应天然正确）
- [ ] 单用户连接上限改到 Redis（步骤 1.2 留的坑）
- [ ] 对比动态订阅前后 `redis-cli INFO stats` 的 `total_net_output_bytes`，补一组数据进 benchmark.md

---

## 步骤 5：阶段 V 礼物系统 + 假支付（2–3 周）

从「最终一致的高吞吐」切换到「强一致的交易」。详细设计见 newplan.md 阶段 V，这里只列顺序：

1. **账户体系**（2 天）：`accounts` 表，金额 int64 存分，注册事务里开户
2. **假支付网关**（3–4 天）：`cmd/mockpay/`，HMAC 签名、延迟回调、故意重复通知 / 随机失败 / 随机延迟
3. **充值订单**（4–5 天）：状态机 PENDING → PAID → SUCCESS / CANCELLED / EXPIRED，回调幂等（`UPDATE ... WHERE status='PENDING'` 看 RowsAffected），ticker 扫表关单，主动查询兜底
4. **送礼物**（4–5 天）：CAS 扣减 + request_id 幂等 + 双向流水，一个事务；`GiftAction` 注册进 ActionRegistry 做特效广播；Redis ZSET 土豪榜
5. **抽奖**（1–2 天，附赠）

必写测试：100 goroutine 并发送礼余额分文不差；同一 request_id 重复 50 次只扣一次。

核心原则：送礼主链路不走 MQ。MQ 只用于送礼之后的衍生动作。

---

## 步骤 6：阶段 VII K8s（1 周，可选，优先级最低）

投后端岗价值不大，投 SRE / 云原生岗再做。如果做，只聚焦长连接部署难题：
- preStop 钩子 → 摘流量 → 广播「即将重启」→ 等 connWg 归零 → 退出（`main.go` 的优雅关闭已是基础）
- HPA 用 `live_ws_connections` 自定义指标而不是 CPU
- 前端指数退避重连 + 重连后拉历史补齐（`liveSocket.ts` 已有退避，缺补齐）

---

## 贯穿全程

- [ ] 每个步骤结束更新 README
- [ ] 每个阶段写一篇技术笔记：为什么这么改、数据对比、踩的坑 —— 这是面试弹药库
- [ ] 提交规范保持 `feat(scope): / fix(scope): / perf(scope):`
- [ ] 前端 `RoomDetailView` 和 `RoomConsoleView` 大量重复，等后端稳定后抽成 `useLiveRoom()` composable（不急）

## 推荐顺序

```
步骤 0 收口提交
  → 1.1 分区键 → 1.2 限流 → 1.3 测试 → 1.4 小修
  → 步骤 2 Docker
  → 步骤 3 压测（产出 benchmark.md）
  → 视火焰图决定 III.8
  → 步骤 4 多实例
  → 步骤 5 礼物系统
  → (步骤 6 K8s)
```

步骤 0 到步骤 3 大约 2 周，做完就有一份能拿出去讲的压测报告。
