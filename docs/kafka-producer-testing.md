# Kafka Producer 自动化验证

在项目根目录运行：

```bash
bash backend/scripts/test-kafka-producer.sh
```

只依赖运行中的开发 Kafka，默认地址为 `localhost:9092`。不需要 MySQL、Redis、Server 或 Consumer 进程。工具直接调用 Go Producer，不经过前端/WebSocket。它创建随机名称的 `producer-check-*` topic（三分区、单副本），正常结束或测试失败时请求删除；不会写入 `danmu`、修改业务消费组 offset 或操作业务数据库。

其他地址和竞态检查：

```bash
KAFKA_TEST_BROKERS=localhost:9092 bash backend/scripts/test-kafka-producer.sh
bash backend/scripts/test-kafka-producer.sh -race
```

## 功能检查

`TestKafkaProducerDelivery` 使用实际的 `NewKafkaProducer()`：

| 场景 | 校验 |
| --- | --- |
| 仅发送一条消息，保持 Producer 打开 | 不足数量阈值也能发送，不能依靠 Close 才发出 |
| 连续发送 500 条 | JSON 字节、消息 key、总数量一致，无额外重复记录 |
| 发送 7 条后立即正常 Close | 关闭返回后，剩余消息可以完整读取 |

读取结果有超时；缺失、重复、内容变化、发送错误和清理失败会使测试失败。测试 topic 刚建立时可能短暂发生 leader 元数据变化，验证读取端会有限等待恢复。这不放宽消息完整性校验。

正常关闭测试不模拟 kill -9，也不验证 Broker 故障下的持久性或多副本容灾。

## 三组对照

`TestKafkaProducerComparison` 与业务 Producer 共用配置函数，仅调整压缩和主动攒批；`WaitForAll`、哈希分区保持一致：

| profile | 配置 |
| --- | --- |
| baseline | 不压缩，不配置主动攒批等待；Sarama 仍可自然形成批次 |
| lz4 | 仅 LZ4 |
| lz4_batch | 当前业务配置，初始值为 LZ4 + 100 条 / 100ms |

每组使用相同的合成 JSON 和 key 分布，测试两种负载：

- low：5 条，每隔 200ms 提交一条。
- steady：1000 条，按每隔 1ms 的目标时间提交一条；调度抖动可能造成短时聚集。

每组结果输出一行 `KAFKA_RESULT` JSON：

| 字段 | 含义 |
| --- | --- |
| messages / payload_bytes | 输入消息数量、JSON 总字节数（不含 key） |
| client_outgoing_bytes | 此 Producer 的 Sarama 发送字节，包含元数据及协议请求，不等于网卡全部字节 |
| client_network_requests | 此 Producer 的全部网络请求数，包含元数据请求 |
| encoded_produce_requests | Sarama `records-per-request` 的样本计数，即被编码的 Produce 请求数；重试等情况下不应解释为不同业务批次数 |
| ack_p50_ms / ack_p95_ms / ack_p99_ms | 从尝试提交到 Producer 输入队列，到收到 Kafka 成功确认的延迟，含队列背压、攒批、网络和重试 |
| elapsed_ms | 第一条尝试提交到全部收到确认的时间，含主动控制的输入间隔 |

延迟不含预先生成 JSON 的时间，不是前端端到端延迟，也不是 MySQL 落库延迟。实际 Consumer 进程不参与此测试。工具不测 CPU；固定输入速率也不能据此得出最大吞吐能力。低流量只有 5 个样本，P95/P99 只能做直观观察。

对照测试验证消息完整性，但不会用“压缩必须节省某个百分比”作为通过条件。主题与连接均为新建，冷启动、元数据请求、共享 Kafka 负载、运行顺序和合成数据的重复度都会影响结果。竞态检测会额外增加开销，性能观察使用不带 `-race` 的运行结果。

## 使用结果选择配置

1. 先确认功能测试全部 PASS。
2. 比较 baseline 与 lz4 的发送字节，判断压缩收益。
3. 比较 lz4 与 lz4_batch 的请求数和确认延迟，判断攒批代价。
4. 重要决策应多轮测试，并替换为实际消息分布；不要把一次合成负载结果写成生产性能承诺。

在 backend 目录，重复运行对照测试：

```bash
RUN_KAFKA_INTEGRATION=1 go test -count=3 -v -timeout=3m ./internal/infra -run '^TestKafkaProducerComparison$'
```

普通 `go test ./...` 默认跳过这些集成测试。没有显式设置 `RUN_KAFKA_INTEGRATION=1`，不应将 SKIP 解释成 Kafka 功能通过。

工具不启动或停止 Kafka。外部强制终止、进程崩溃可能留下测试 topic；日志记录了每个准确名称，需单独检查清理。十分钟消息保留时间不等于自动删除 topic。
