阶段 I：可观测性地基（3-4 天）
目标：让系统能被观测。没有观测，后面所有优化都是盲改。

先定 4 个设计决策
决策	选项	我的建议	理由
全局 logger vs 依赖注入	全局单例 / 构造函数注入	全局单例 + 包级函数	60 处调用点，注入要改 30+ 个构造函数签名。日志是基础设施，不是业务依赖。等你有了测试需求再说
SugaredLogger vs Logger	好写 / 零分配	两者都要	冷路径（启动、错误）用 Sugared 好写；热路径（deliver/ReadPump/consumer 循环）必须用 Logger —— Sugared 的 interface{} 参数会逃逸到堆上
上下文字段怎么带	全局 / context 传递	两层：房间级用参数显式传；请求级用 context + 中间件	别为了 TraceID 把所有函数签名改成带 ctx
指标：Prometheus vs 手写	标准 / 简单	直接上 Prometheus	就多一个依赖，但能白嫖 Grafana 面板；手写 JSON 后面还得换
任务清单
 I.1 新建 internal/logger/logger.go：Zap 初始化 + 包级 Info/Warn/Error/Debug + L() 返回裸 Logger
开发环境：zap.NewDevelopment()（彩色、人类可读）
生产环境：zap.NewProduction()（JSON、采样）
由 config 的 log.level / log.format 控制
 I.2 替换全部 60 处 log.Printf，同时分级：
位置	现状	改成
broadcast_pool.go:53 worker 逐条日志	Printf	Debug（默认关闭）
manager.go:83 慢客户端丢消息	Printf	Debug + 计数器
handler.go:107 进出房间	Printf	Debug（6 万连接时这是灾难）
启动/关闭/配置加载	Printf	Info
Redis/Kafka/DB 失败	Printf	Error
 I.3 internal/middleware/logger.go：替换 gin.Logger()，输出结构化字段（method/path/status/latency/trace_id），并跳过 /metrics 和健康检查
 I.4 internal/middleware/trace.go：生成 TraceID 塞进 context + 响应头 X-Trace-Id
 I.5 internal/metrics/metrics.go：Prometheus 指标

live_ws_connections            Gauge    当前连接数
live_ws_messages_total         Counter  {direction=up|down, type=chat|like|...}
live_ws_dropped_total          Counter  {reason=queue_full|client_slow}
live_broadcast_duration_seconds Histogram
live_kafka_produce_total       Counter  {result=ok|error}
live_consumer_lag              Gauge
live_consumer_batch_size       Histogram
 I.6 暴露 GET /metrics（不放在 JWT 保护组里）
 I.7 mysql.go:17 GORM Logger 改由配置控制，默认 logger.Warn
学习点
zap.Logger vs SugaredLogger 的分配差异 / LevelEnabler 短路机制 / 结构化日志为什么优于字符串拼接 / Prometheus 四种指标类型（Counter/Gauge/Histogram/Summary）的适用场景 / 为什么标准库 log 不能用在热路径

验收
日志级别设为 Info 时，deliver 热路径零日志、零分配（用 go test -bench -benchmem 验证）
curl localhost:8080/metrics 能看到连接数随浏览器开关变化
每条 HTTP 日志带 trace_id，能和错误日志串起来
阶段 II：Docker 一键启动（2-3 天）
目标：git clone → docker compose up → 浏览器能用。

任务清单
 II.1 补全 deployments/docker-compose.yml：现在只有 Kafka + kafka-ui，加上
mysql:8.0（挂 volume + 初始化 SQL）
redis:7-alpine
healthcheck + depends_on: condition: service_healthy（否则 server 会在 MySQL 就绪前启动然后崩）
 II.2 backend/Dockerfile：多阶段构建

FROM golang:1.26 AS builder → go build -ldflags="-s -w"
FROM gcr.io/distroless/static → 最终镜像 ~15MB
用同一个 Dockerfile 通过 --build-arg CMD=server|consumer 构建两个二进制
 II.3 frontend/Dockerfile：pnpm build → nginx 静态托管 + /api /ws 反代
 II.4 配置体系整理：config.yaml（默认值）→ 环境变量覆盖 → 容器里全靠环境变量
现在 .env + config.yaml 混用，容器化时要理清优先级
 II.5 Makefile：make up / make down / make logs / make bench
 II.6 写根目录 README（架构图 + 技术选型理由 + 启动步骤 + 接口列表）
关键坑
容器内的 localhost 不是宿主机 —— config.yaml 里的 127.0.0.1 全要换成服务名（mysql / redis / kafka:29092）
Kafka 的 ADVERTISED_LISTENERS 你已经配好了双监听器（HOST + INTERNAL），容器内的 server 要连 kafka:29092 而不是 localhost:9092
学习点
多阶段构建 / distroless 镜像 / depends_on 的 healthcheck 条件 / 容器网络与服务发现 / 12-Factor 的配置外置原则

验收
干净机器上 docker compose up -d 后能完整走通：注册 → 登录 → 开播 → 发弹幕 → 刷新看历史
docker images 里 backend 镜像 < 30MB
阶段 III：性能改造（1-1.5 周）
目标：把架构改成能承载 6 万连接的形状。这是整个计划的核心。

任务清单（按 ROI 排序）
III.1 P0 修复（半天）
 client.go 加 c.Conn.SetReadLimit(4096) —— 现在没有上限，一条 500MB 的 JSON 就能 OOM 整台机器
 msg_handler.go:44 缺 return，会二次写响应
 删除 msg_service.go:42 SaveMsg 死代码
 完成之前讨论的 sent_at 排序改造（entity / consumer / repo / model 四处）+ DROP TABLE messages 重建
III.2 预序列化重构 ⭐ 最重要（2-3 天）
问题：client.go:88 每个 client 各自 WriteJSON(msg) —— 一条弹幕广播给 6 万人，同一份数据被序列化 6 万次。

 Client.Send 类型：chan Message → chan []byte
 Manager.deliver：进循环前 json.Marshal 一次，把 []byte 分发给所有 client
 BroadcastPool 的 job 载荷同步改成 []byte
 WritePump：WriteJSON(msg) → WriteMessage(TextMessage, data)
 处理副作用：handler.go:115 的 client.Send <- likeCountMsg 等直接投递点要先序列化
收益：下行 CPU 降低约 6 万倍的重复工作；chan 元素从 80 字节降到 24 字节，60k 连接省 ~700MB

III.3 内存瘦身（半天）
 Send 缓冲 256 → 32
理由：弹幕是实时数据，积压 256 条（可能是几十秒前的）再推给用户没有意义，不如丢掉
收益：结合 III.2，单连接从 20KB 降到 768 字节，6 万连接省 1.2GB
III.4 定时聚合广播 ⭐（1-2 天）
问题：action_like.go:41 每次点赞都广播。6 万人房间 × 5000 点赞/秒 = 3 亿次/秒下行，必崩。

 新建 internal/ws/aggregator.go：一个 ticker（1 秒）遍历活跃房间，广播当前点赞数 / 在线人数
 LikeAction 只做 Redis INCR，不再广播
 handler.go:99/handler.go:123 进出房间的在线数广播同样去掉，交给 ticker
 加"脏标记"：只广播数值有变化的房间，避免静默房间的空转
收益：从 O(事件数 × 房间人数) 降到 O(1/秒 × 房间人数)，降低约 1000-5000 倍

这是整个改造里设计思想最值钱的一块。名字叫「写扩散聚合」，是所有高扇出系统的通用手法。

III.5 Consumer 攒批落库（1-2 天）
问题：consumer/main.go:57 逐条单行 INSERT，上限约 1000-3000 行/s。

 ConsumeClaim 改成攒批：满 500 条 或 满 100ms 触发一次 CreateInBatches
 MsgRepo 加 CreateBatchIfAbsent(ctx, msgs []entity.Message)
 批量写成功后才 MarkMessage（保证 at-least-once，宁可重复不可丢失）
 用 select + time.After 实现"攒够或超时"的双触发
 KAFKA_NUM_PARTITIONS: 3 → 8-12
收益：10-30 倍

III.6 Kafka Producer 调优（10 分钟）
 kafka.go:19 WaitForAll → WaitForLocal（单 broker + 副本因子 1 场景，弹幕可容忍丢失）
 sc.Producer.Compression = sarama.CompressionLZ4 —— 纯文本压缩率极高，网络流量降 70%
 sc.Producer.Flush.Frequency = 100 * time.Millisecond + Flush.Messages = 100
 分区键从 UserID 改成 RoomID（sent_at 排序落地后，顺序不再依赖分区；按房间分区落库有局部性，批量写更友好）
III.7 本地缓存 session_id（半天）
 SessionManager 加 sync.Map + 30s TTL，避免每条弹幕都 Redis GET
 Close() 时主动清除本地缓存
 收益：Redis 调用量减半
III.8 WritePump 合并写（1 天）
 收到第一条消息后不立即发，攒 5-10ms 内的所有消息合并成一个 WebSocket frame
 用 websocket.Conn.NextWriter 一次写入多条（换行分隔或 JSON 数组）
 前端 liveSocket.ts 配合解析
 收益：write syscall 次数降一个数量级
III.9 限流与防护（1 天）
 Client 加令牌桶（golang.org/x/time/rate），每人每秒 2 条弹幕
 全局连接数上限（超过拒绝 Upgrade）
 单用户连接数上限（防止一人建 1000 连接）
III.10 关键路径补测试（1-2 天）
改性能之前得有回归保障。不追求覆盖率，只测最容易改错的：

 BroadcastPool 的分片与丢弃逻辑
 Manager.Register/Unregister/deliver 的并发安全（go test -race）
 OnlineCounter 的 Lua 脚本（用 miniredis）
 msgRepo.ListBySessionID 的排序正确性
学习点
chan 底层内存分配 / 逃逸分析 / go test -race / 扇出放大 / 写扩散聚合 / 背压与主动丢弃 / 批量写摊薄事务成本 / 令牌桶限流 / pprof heap & goroutine

验收
指标	目标
单连接内存	< 1 KB（现在 ~40 KB）
日志级别 Info 时，deliver 分配	0 allocs/op
Consumer 落库	> 20000 行/s
go test -race ./...	全绿
阶段 IV：压测与调优（3-5 天）
目标：产出你自己的、可被追问的数字。

任务清单
 IV.1 cmd/benchmark/main.go
参数：-conns -rooms -msg-rate -duration -target
统计：建连成功率、上行 QPS、下行接收数、端到端延迟 P50/P95/P99、丢包率
用 -conns 阶梯递增（1k → 5k → 1w → 3w → 6w）找拐点
 IV.2 环境准备（必须两台机器）

ulimit -n 200000
sysctl -w net.core.somaxconn=65535
sysctl -w net.ipv4.tcp_max_syn_backlog=65535
sysctl -w net.ipv4.ip_local_port_range="1024 65535"
⚠️ WSL2 跑不出真实数字 —— 压测客户端和服务端抢同一份 CPU/内存/端口
⚠️ 单机对单个 IP:端口 最多约 28000 临时端口 → 服务端要开多个端口，或客户端绑多个 IP
建议：云服务器按量付费，两台 4核8G，压两天不到 20 块钱
 IV.3 三组对比实验（对比数据比绝对数字值钱 10 倍）
实验	对比项	想证明什么
A	逐条 WriteJSON vs 预序列化	III.2 的价值
B	同步写库 vs Kafka 异步	Kafka 到底有没有必要（这是必被追问的）
C	每次点赞广播 vs 1 秒聚合	III.4 的价值
 IV.4 pprof 分析：go tool pprof 看 CPU 火焰图、heap、goroutine 泄漏
 IV.5 写 docs/benchmark.md：环境、方法、数据表、火焰图截图、瓶颈分析与结论
诚实性要求 ⚠️
"下行 QPS" 必须注明是扇出计数，写成"6 万连接下，房间内 25 msg/s 可完成 150 万次/秒下行投递"，而不是含糊的"150 万 QPS"
跑出你自己的数。哪怕是 3 万连接、80 万下行，真实的 3 万远胜抄来的 6 万 —— 面试官一追问细节就会穿帮
每个数字你都要能回答："卡在哪？为什么是这个数？再往上要改什么？"
验收
一份能撑住 20 分钟追问的压测报告。

阶段 V：礼物系统 + 假支付（2-3 周）
目标：从"最终一致的高吞吐"切换到"强一致的交易"，两种截然不同的工程范式。

核心原则
送礼物主链路不走任何 MQ。 一次 HTTP 请求 → 一个 MySQL 事务 → 立即返回。
MQ 只用于送礼之后的衍生动作（归档、结算、通知）。

判断标准：MQ 适合"可以晚、可以丢、丢了能重算"的事；不适合"用户等结果且涉及钱"的事。

数量级对比（说明为什么送礼物不需要高并发架构）：

行为	QPS 量级	原因
弹幕	10000	免费
点赞	5000	免费
送礼物	10-100	要花钱
充值	1-10	要走支付
V.1 账户体系（2 天）
 新建 accounts 表（不放在 users 表里 —— 账户后续可能拆服务、可能一人多账户、users 表读多写少而余额写频繁）

accounts: id / user_id(uniq) / balance / frozen / version / created_at / updated_at
 金额一律 int64 存"分"，禁止 float（0.1+0.2 != 0.3）
 注册时自动开户（在注册事务里一起做）
 前端用户中心页 ProfileView.vue
V.2 假支付网关（3-4 天）⭐
独立服务 cmd/mockpay/，模拟第三方支付。这一步的技术密度比送礼物还高。

 POST /pay/create 接单 → 返回支付页 URL
 模拟支付页（点"确认支付"/"取消"）
 异步回调：延迟 2 秒 POST 你的 /api/v1/pay/notify
 签名机制：HMAC-SHA256 对参数排序拼接后签名
 故意制造真实世界的麻烦（这才是学习价值所在）：
重复通知同一笔订单 3 次 → 测你的幂等
随机 10% 概率通知失败后重试 → 测你的重试处理
随机延迟 0-30 秒 → 测你的超时逻辑
 GET /pay/query 主动查询接口（供你的兜底轮询用）
V.3 充值订单（4-5 天）
 recharge_orders 表：order_no(uniq) / user_id / amount / status / channel / trade_no / paid_at / expired_at
 订单状态机（先画图再写代码）

PENDING ──支付成功──> PAID ──入账──> SUCCESS
   │                    
   ├──用户取消──> CANCELLED
   └──30分钟超时──> EXPIRED

合法迁移只有以上 5 条,其余一律拒绝
 回调幂等：trade_no 唯一索引 + UPDATE ... WHERE status = 'PENDING' 判断 RowsAffected
 回调验签：验证失败直接拒绝，且不能泄漏错误细节
 超时关单：先用 Go ticker 扫表（每分钟扫一次 status=PENDING AND expired_at < now()）
为什么不用 RabbitMQ 延迟队列？因为扫表是唯一不会漏单的方案，DB 是真相源。真实支付系统里延迟队列是优化、扫表是保底，两者并存。想学延迟队列的话，等功能跑通后再加一层（见「可选扩展」）。

 主动查询兜底：超过 5 分钟仍 PENDING 的订单，主动调 /pay/query 确认
 入账用事务：改订单状态 + 加余额 + 写账户流水，三者原子
V.4 送礼物（4-5 天）⭐
 gifts 表（礼物配置：名称/图标/价格/特效等级）
 gift_records 表：request_id(uniq) / from_user / to_user / room_id / session_id / gift_id / count / amount / created_at
 account_flows 表（账户流水，所有余额变动都要有流水，这是对账的基础）
 核心事务（Service 层控制事务边界，Repo 层接受 *gorm.DB 参数以便复用）：

BEGIN
  1. UPDATE accounts SET balance = balance - ?
     WHERE user_id = ? AND balance >= ?     ← CAS 扣减
     RowsAffected == 0 → 余额不足,回滚
  2. UPDATE accounts SET balance = balance + ? WHERE user_id = 主播
  3. INSERT gift_records (request_id, ...)   ← 唯一索引拦重复
  4. INSERT account_flows × 2                ← 双向流水
COMMIT
 幂等：前端生成 request_id(UUID)，唯一键冲突时返回上次的成功结果而不是报错
 WS 特效广播：注册 GiftAction，复用现有 ActionRegistry（几乎零成本，这是它长在直播场景上的体现）
 土豪榜：Redis ZSET ZINCRBY（派生数据，丢了能从 gift_records 重算）
 并发正确性测试（必写）：
100 个 goroutine 同时送礼，验证余额精确、无超扣
同一 request_id 重复提交 50 次，验证只扣一次
go test -race
V.5 抽奖（1-2 天，附赠）
 主播开启抽奖 → 参与资格从 gift_records 取（本场送过礼的人）
 Redis SET 存参与者，SPOP 抽取
 WS 广播中奖结果
学习点
MySQL 事务与隔离级别 / 乐观锁 CAS vs 悲观锁 FOR UPDATE（你 seesion_repo.go:52 已经用过悲观锁，正好对比）/ 接口幂等的三种实现 / 状态机建模 / 异步回调与签名 / 复式记账与对账 / 金额精度 / 哪些数据能放 Redis、哪些必须落 MySQL

验收
并发压测下余额分文不差
拔网线模拟回调丢失，兜底轮询能补齐
能画出完整的订单状态机图
阶段 VI：多实例验证（2-3 天）
目标：证明你的分布式广播设计真的成立。

 VI.1 替换 PSubscribe("ws:room:*") 为动态订阅
问题：redis_broker.go:12 每个实例都收全站消息，3 实例 = 2/3 的 CPU 和网络纯浪费
改法：Register 时 Subscribe(ws:room:{id})，房间最后一人离开时 Unsubscribe
 VI.2 docker-compose 起 3 个 backend 实例 + nginx 负载均衡（ip_hash 保证 WS 连接粘性）
 VI.3 验证：用户 A 连实例 1，用户 B 连实例 2，A 发弹幕 B 能收到
 VI.4 验证在线人数在多实例下正确（Redis Hash 是全局的，应该天然正确）
 VI.5 补一组多实例压测数据，对比动态订阅前后的 Redis 流量
阶段 VII：K8s 部署（1 周，可选）
⚠️ 优先级最低。 投后端开发岗，"会写 deployment.yaml"远不如"能讲清扇出优化"值钱；投 SRE/云原生岗才是主菜。

如果要做，就聚焦在「长连接服务的部署难题」上 —— 这才值得花那一周。

基础部分
 Deployment（server / consumer / frontend）+ Service + Ingress
 ConfigMap（config.yaml）+ Secret（DB 密码、JWT secret）
 MySQL/Redis/Kafka 用 Helm 装或走云托管
 Liveness / Readiness 探针
有含金量的部分 ⭐
长连接特有难题	解法
滚动更新会断开所有连接	preStop 钩子 → 先从 Service 摘除 → 广播"即将重启"给客户端 → 等 connWg 归零 → 退出。你 main.go:92 的优雅关闭已经是这个的基础
HPA 按 CPU 扩容不准	10 万个空闲连接 CPU 接近 0 但内存已爆 → 用 custom metrics（live_ws_connections，正是阶段 I 埋的指标）驱动 HPA
Service 负载均衡对长连接是"一次性"的	新 Pod 起来后老连接不会迁移 → 需要主动断连再平衡策略
就绪探针的两难	"就绪"不等于能立刻承接，摘除时也不能立刻断
前端配合	指数退避重连 + 重连后拉历史补齐断连期间的弹幕
贯穿全程的横切任务
 README 持续更新（阶段 II 建立，每个阶段结束后补充）—— 架构图、技术选型理由、启动步骤、压测数据、接口文档
 Git 提交规范：你现在的 feat(live): / fix(ws): 已经很规范，保持
 每阶段结束写一篇技术笔记：为什么这么改、数据对比、踩过的坑
这些笔记就是你面试时的弹药库。面试问的从来不是"你做了什么"，是"你为什么这么做"。

可选扩展（有余力再说）
项	价值	备注
RabbitMQ 延迟队列关单	★★★★	唯一值得引入第二个 MQ 的场景。保留扫表兜底，然后就能说："延迟队列做实时关单，扫表兜底防漏单，因为钱不能漏，真相源必须是数据库"
秒杀	★★★	技术内核和送礼物重叠 80%，边际收益低
分库分表	★★	弹幕表按 live_session_id 分表
链路追踪 OpenTelemetry	★★★	阶段 I 的 TraceID 打好了基础
真实第三方支付	★	❌ 不建议。核心难点（回调/验签/对账）假网关已覆盖，剩下的是内网穿透折腾
带货 / 物流发货	☆	❌ 不建议。纯 CRUD，零技术含量
最终产出（简历视角）
做完之后，你的项目能写出这样的描述：

高并发直播弹幕系统（Go / Gin / WebSocket / Redis / Kafka / MySQL / Docker）

基于 Redis Pub/Sub 实现跨实例弹幕广播，采用动态订阅替代通配订阅，多实例下 Redis 流量降低 N%
广播链路预序列化改造，消除单条消息在 6 万连接上的重复 JSON 编码，下行 CPU 降低 X%
点赞/在线人数由事件驱动广播改为定时聚合广播，扇出量从 O(事件×人数) 降至 O(1秒×人数)，降低约 N 倍
Kafka 异步落库 + 消费端攒批写入，积压 500 万条弹幕 N 分钟内落盘，平均 X 行/s
礼物/充值系统采用 MySQL 事务 + CAS 扣减 + request_id 幂等，并发压测下账目零误差；自研模拟支付网关完整实现异步回调、签名校验、超时关单与主动查询兜底
单机实测支撑 X 万长连接，上行 X QPS，下行投递 X 次/s，极限负载丢包率 < X%
完整压测报告见 docs/benchmark.md

每一个 X 都是你自己跑出来的，每一句都能撑住 10 分钟的追问。

要不要现在就开始阶段 I？我们先把那 4 个设计决策敲定，然后我给你 internal/logger/logger.go 的完整代码（带详细中文注释），你照着写。