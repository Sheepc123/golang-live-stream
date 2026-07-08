# 总体技术栈

## 前端

```
Vue 3
TypeScript
Vite
Vue Router
pnpm
Axios
WebSocket API
```

## 后端

```
Go
Gin
Gorilla WebSocket
GORM
MySQL
Redis
Kafka
Zap
JWT
```

## 部署

```
Docker
Docker Compose
Kubernetes
```

------

# 项目目录

```
golang-live-stream/
├── backend/
│   ├── cmd/
│   │   ├── server/
│   │   ├── consumer/
│   │   └── benchmark/
│   │
│   ├── internal/
│   │   ├── router/
│   │   ├── handler/
│   │   ├── service/
│   │   ├── repository/
│   │   ├── model/
│   │   ├── response/
│   │   ├── middleware/
│   │   ├── ws/
│   │   ├── consumer/
│   │   ├── infra/
│   │   └── logger/
│   │
│   ├── go.mod
│   └── go.sum
│
├── frontend/
├── deployments/
├── docs/
├── README.md
└── .gitignore
```

前期不会把所有目录都建出来，用到哪个模块再创建哪个模块。

------

# 阶段 0：项目初始化

这个阶段已经基本完成。

完成内容：

```
创建前后端目录
初始化 Vue 3
初始化 Go Module
初始化 Git
完成登录页面样式
配置 Vue Router
```

目标：

> 确认前端和后端都能独立运行。

------

# 阶段 1：完成登录功能

这是我们现在所在的阶段。

暂时不使用数据库，使用固定测试账号：

```
用户名：admin
密码：123456
```

## 后端模块顺序

我们会按照依赖关系一个文件一个文件写：

```
1. response/response.go
2. model/auth.go
3. service/auth_service.go
4. handler/auth_handler.go
5. router/router.go
6. cmd/server/main.go
7. curl 测试接口
8. 修改 Vue 登录请求
```

## 登录调用链

```
Vue 登录页面
    ↓ POST /api/auth/login
Router
    ↓
AuthHandler
    ↓
AuthService
    ↓
返回统一 Response
```

## 这一阶段学习内容

```
Gin 路由
POST 请求
JSON 参数绑定
结构体 Tag
Handler 和 Service 分层
统一 Response
构造函数
依赖注入
HTTP 状态码
业务状态码
前后端接口联调
```

阶段结束后的效果：

```
输入 admin / 123456
    ↓
后端验证成功
    ↓
前端跳转到 /home
```

------

# 阶段 2：加入 JWT 身份认证

登录功能稳定后，再加入 JWT。

## 功能

```
登录成功生成 Token
前端保存 Token
后续请求携带 Token
后端中间件校验 Token
未登录不能进入受保护接口
```

## 新增模块

```
internal/
├── middleware/
│   └── auth_middleware.go
│
├── service/
│   └── token_service.go
│
└── model/
    └── claims.go
```

## 学习内容

```
JWT Header、Payload、Signature
Claims
Bearer Token
Gin Middleware
Context
c.Set()
c.Get()
路由守卫
前端 Axios 拦截器
```

注意：

```
JWT Payload 可以被读取
不能放密码
只能放 user_id、username、role、exp 等信息
```

------

# 阶段 3：WebSocket 最小通信

先不做直播间，只实现浏览器和后端建立 WebSocket 连接。

## 功能

```
浏览器建立 WebSocket
前端发送消息
后端收到消息
后端原样返回
前端显示消息
```

接口：

```
GET /ws
```

## 新增模块

```
internal/ws/
├── client.go
└── handler.go
```

## 学习内容

```
HTTP 升级为 WebSocket
长连接
ReadMessage
WriteMessage
连接关闭
心跳
并发读写问题
```

这一阶段的核心目标：

> 先理解 WebSocket 连接是怎么建立和保持的。

------

# 阶段 4：内存直播房间

开始实现真正的直播间和弹幕广播。

## 核心结构

```
Manager
├── Clients
├── Rooms
├── Register
├── Unregister
└── Broadcast
```

## 功能

```
用户加入直播间
用户离开直播间
发送弹幕
同一房间用户收到弹幕
不同房间互不影响
统计在线人数
```

## 新增模块

```
internal/ws/
├── client.go
├── manager.go
├── room.go
├── message.go
└── handler.go
```

## 学习内容

```
goroutine
channel
map
sync.RWMutex
并发安全
生产者与消费者
广播模型
连接生命周期
```

这是整个项目最重要的 Go 并发阶段。

------

# 阶段 5：前端直播间页面

后端 WebSocket 房间完成后，开始完善前端。

## 页面

```
/login
/home
/room/:roomId
```

## 功能

```
直播房间列表
进入房间
弹幕列表
弹幕输入框
在线人数
连接状态
自动滚动
断线提示
自动重连
```

## 前端目录

```
frontend/src/
├── api/
├── views/
├── components/
├── router/
├── types/
└── utils/
```

## 学习内容

```
Vue 生命周期
onMounted
onUnmounted
响应式状态
WebSocket 封装
自动重连
TypeScript 接口定义
组件拆分
```

------

# 阶段 6：统一 WebSocket 消息协议

不能一直直接发送普通字符串，需要定义统一协议。

例如：

```
{
  "type": "chat",
  "room_id": "1001",
  "user_id": 1,
  "content": "hello",
  "timestamp": 1780000000
}
```

消息类型：

```
join
leave
chat
heartbeat
online_count
system
error
```

## 学习内容

```
消息协议设计
JSON 序列化
消息路由
消息类型常量
前后端类型一致性
```

------

# 阶段 7：接入 MySQL

前面都使用内存数据，这一阶段开始保存真实数据。

## 数据表

```
users
rooms
messages
```

## 新增模块

```
internal/
├── repository/
│   ├── user_repository.go
│   ├── room_repository.go
│   └── message_repository.go
│
└── infra/
    └── mysql.go
```

调用链变成：

```
Handler
    ↓
Service
    ↓
Repository
    ↓
MySQL
```

## 功能

```
用户注册
数据库登录
密码哈希
查询房间
创建房间
保存历史弹幕
查询历史弹幕
```

## 学习内容

```
GORM
数据库连接池
Repository 模式
事务
数据库迁移
bcrypt 密码哈希
配置文件
环境变量
```

------

# 阶段 8：接入 Redis

单台 Gin 服务可以用内存广播，但多台服务之间无法共享房间消息。

Redis 用于解决：

```
Server A 上的用户
和
Server B 上的用户
互相收不到消息
```

## Redis 用途

```
Pub/Sub 跨实例广播
在线用户状态
房间在线人数
缓存房间信息
Token 黑名单
```

## 新增模块

```
internal/infra/redis.go
internal/ws/redis_broker.go
```

## 学习内容

```
Redis Pub/Sub
分布式状态
缓存
消息广播
多实例服务
```

------

# 阶段 9：接入 Kafka

WebSocket 收到每一条弹幕后，不直接同步写数据库。

流程：

```
用户发送弹幕
    ↓
Gin 收到消息
    ↓
推送 Kafka
    ↓
立即广播给用户
    ↓
Kafka Consumer 异步写 MySQL
```

## 新增入口

```
cmd/
├── server/
└── consumer/
```

## 学习内容

```
消息队列
生产者
消费者
Topic
Partition
Offset
异步处理
削峰
解耦
消息重复
消息丢失
幂等性
```

这一阶段开始接近真实高并发系统。

------

# 阶段 10：日志、配置和错误处理

完善工程质量。

## 增加内容

```
Zap 日志
统一错误码
配置文件
环境变量
请求日志
Recovery
链路追踪 ID
优雅关闭
参数校验
```

## 学习内容

```
结构化日志
错误包装
panic 恢复
context.Context
信号处理
Graceful Shutdown
```

------

# 阶段 11：测试与压力测试

## 单元测试

```
Service 测试
Repository 测试
Handler 测试
WebSocket 测试
```

## 压力测试

创建：

```
cmd/benchmark/
```

模拟：

```
100 个连接
1,000 个连接
10,000 个连接
持续发送弹幕
断线重连
房间广播
```

观察：

```
CPU
内存
goroutine 数量
消息延迟
吞吐量
连接失败数
```

## 学习内容

```
go test
httptest
Benchmark
pprof
race detector
性能分析
```

------

# 阶段 12：Docker 部署

先使用 Docker Compose 启动基础设施：

```
MySQL
Redis
Kafka
Zookeeper 或 KRaft
```

然后容器化：

```
Gin Server
Kafka Consumer
Vue 前端
Nginx
```

## 学习内容

```
Dockerfile
Docker Compose
容器网络
环境变量
数据卷
多阶段构建
Nginx 反向代理
```

------

# 阶段 13：Kubernetes 部署

最后再接触 Kubernetes。

## 部署对象

```
Frontend Deployment
Backend Deployment
Consumer Deployment
Service
Ingress
ConfigMap
Secret
HorizontalPodAutoscaler
```

## 学习内容

```
Pod
Deployment
Service
Ingress
ConfigMap
Secret
滚动更新
水平扩容
健康检查
```

------

# 推荐开发顺序

不要提前跳到 Redis、Kafka 或 Kubernetes。

正确顺序是：

```
HTTP 登录
→ JWT
→ WebSocket 单连接
→ 内存房间广播
→ Vue 直播间
→ 消息协议
→ MySQL
→ Redis
→ Kafka
→ 工程化
→ 压测
→ Docker
→ Kubernetes
```

这个顺序的原因是：

```
先理解单机逻辑
再理解并发
再理解数据库
再理解分布式
最后理解部署
```