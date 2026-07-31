<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import DanmakuInput from '@/components/DanmakuInput.vue'
import DanmakuList from '@/components/DanmakuList.vue'
import DanmakuScreen from '@/components/DanmakuScreen.vue'
import { LiveSocket, type LiveSocketStatus, type WSMessage } from '@/utils/liveSocket'

// 主播控制台页面。
// 它复用了 RoomDetailView 的「看画面 + 弹幕 + 点赞 + WebSocket」逻辑，
// 额外加了一块主播专属的「直播控制区」（开播 / 下播）。
// 目前先做开播/下播；踢人、拉黑、福袋等以后往控制区里加。
//
// 教学备注：这个文件和 RoomDetailView 有大量重复代码。
// 这是刻意为之的取舍——先拷贝跑通，等功能稳定后再把公共部分抽成组件重构。

const route = useRoute()
const router = useRouter()

interface ApiResponse<T> {
  code: number
  msg: string
  data: T
}

interface Room {
  id: number
  owner_id: number // 归属字段，用来判断当前登录用户是不是房主
  title: string
  anchor_name: string
  category: string
  cover_url: string
  stream_url: string
  description: string
  status: string
  viewer_count: number
  created_at: string
}

interface HistoryMessage {
  id: number
  room_id: number
  user_id: number
  username: string
  content: string
  type: string
  timestamp: number
}

interface HistoryListResponse {
  messages: HistoryMessage[]
  total: number
}

const room = ref<Room | null>(null)
const loading = ref(false)
const errorMsg = ref('')

const liveSocket = ref<LiveSocket | null>(null)
const wsStatus = ref<LiveSocketStatus>('disconnected')
const messages = ref<WSMessage[]>([])
const onlineCount = ref(0)
const likeCount = ref(0)

const danmakuScreenRef = ref<InstanceType<typeof DanmakuScreen> | null>(null)
const videoPaused = ref(false)

const danmakuFontSize = ref(20)
const fontSizeOptions = [
  { label: '小', value: 16 },
  { label: '中', value: 20 },
  { label: '大', value: 26 },
]

// ===== 主播控制区状态 =====
// 是否正在提交开播/下播请求，防止用户狂点。
const statusSubmitting = ref(false)
// 控制区自己的错误提示，和页面顶部的 errorMsg 分开，避免互相覆盖。
const consoleError = ref('')

const roomID = computed(() => {
  const value = route.params.room_id
  return Array.isArray(value) ? value[0] : value
})

// 当前房间是否处于直播中。
const isLive = computed(() => room.value?.status === 'live')

function getAuthorizationHeader() {
  const token = localStorage.getItem('token')
  const tokenType = localStorage.getItem('token_type') || 'Bearer'

  if (!token) return null

  return `${tokenType} ${token}`
}

function clearAuthStorage() {
  localStorage.removeItem('token')
  localStorage.removeItem('token_type')
  localStorage.removeItem('expires_in')
  localStorage.removeItem('login_at')
  localStorage.removeItem('user')
}

function goLogin() {
  clearAuthStorage()
  router.push('/login')
}

// 从 localStorage 里的 user 取出当前登录用户的 user_id。
// 登录时存的是 result.data（含 user 对象），这里读 user.user_id。
function getCurrentUserId(): number | null {
  try {
    const raw = localStorage.getItem('user')
    if (!raw) return null
    const parsed = JSON.parse(raw)
    // 兼容两种结构：{ user: { user_id } } 或直接 { user_id }
    const id = parsed?.user?.user_id ?? parsed?.user_id
    return typeof id === 'number' ? id : null
  } catch {
    return null
  }
}

async function fetchHistoryMessages() {
  const authorization = getAuthorizationHeader()
  if (!authorization) return
  if (!roomID.value) return

  try {
    const res = await fetch(`/api/v1/rooms/${roomID.value}/messages?limit=50`, {
      method: 'GET',
      headers: {
        Authorization: authorization,
      },
    })

    if (res.status === 401) {
      goLogin()
      return
    }

    const result = (await res.json()) as ApiResponse<HistoryListResponse>
    if (result.code !== 0) return

    const historyList: WSMessage[] = result.data.messages.map((item) => ({
      type: item.type as WSMessage['type'],
      room_id: item.room_id,
      user_id: item.user_id,
      username: item.username,
      content: item.content,
      timestamp: item.timestamp,
    }))

    messages.value = historyList
  } catch {
    // 拉历史失败不影响直播和发弹幕，静默处理。
  }
}

async function fetchRoomDetail() {
  const authorization = getAuthorizationHeader()

  if (!authorization) {
    goLogin()
    return
  }

  if (!roomID.value) {
    errorMsg.value = '直播间编号不存在'
    return
  }

  loading.value = true
  errorMsg.value = ''

  try {
    const res = await fetch(`/api/v1/rooms/${roomID.value}`, {
      method: 'GET',
      headers: {
        Authorization: authorization,
      },
    })

    if (res.status === 401) {
      goLogin()
      return
    }

    const result = (await res.json()) as ApiResponse<Room>

    if (result.code !== 0) {
      errorMsg.value = result.msg || '获取直播间详情失败'
      return
    }

    // 归属校验：控制台是主播专属页，如果当前用户不是房主，直接挡回观看页。
    // 后端 PUT 也有 403 兜底，这里前端先拦一道，体验更好。
    const currentUserId = getCurrentUserId()
    if (currentUserId === null || currentUserId !== result.data.owner_id) {
      errorMsg.value = '你不是该直播间的主播，无法进入管理台'
      // 跳到观众观看页，让非房主也能正常看播
      router.replace(`/rooms/${roomID.value}`)
      return
    }

    room.value = result.data
  } catch {
    errorMsg.value = '无法连接服务器，请确认后端是否已经启动'
  } finally {
    loading.value = false
  }
}

function createLocalMessage(type: 'system' | 'error', content: string): WSMessage {
  return {
    type,
    room_id: Number(roomID.value) || 0,
    user_id: 0,
    username: 'system',
    content,
    timestamp: Date.now(),
  }
}

function appendMessage(msg: WSMessage) {
  messages.value.push(msg)
}

function handleSocketMessage(msg: WSMessage) {
  if (msg.type === 'online_count') {
    onlineCount.value = Number(msg.content)
    return
  }

  if (msg.type === 'like_count') {
    const nextLikeCount = Number(msg.content)

    if (Number.isFinite(nextLikeCount)) {
      likeCount.value = nextLikeCount
    }

    return
  }

  if (msg.type === 'heartbeat') return

  if (msg.type === 'chat') {
    danmakuScreenRef.value?.push(msg.content)
  }

  appendMessage(msg)
}

function connectWebSocket() {
  if (!roomID.value) return

  liveSocket.value = new LiveSocket({
    roomID: roomID.value,
    onStatusChange: (status) => {
      wsStatus.value = status
    },
    onMessage: handleSocketMessage,
    onInvalidMessage: () => {
      appendMessage(createLocalMessage('error', '收到一条无法解析的 WebSocket 消息'))
    },
  })

  liveSocket.value.connect()
}

function sendMessage(content: string) {
  if (!content) return

  liveSocket.value?.sendChat(content)
}

function sendLike() {
  liveSocket.value?.sendLike()
}

function closeWebSocket() {
  liveSocket.value?.close()
  liveSocket.value = null
}

// ===== 主播控制区：开播 / 下播 =====
// 调用后端专门的开播/下播接口（不再走 PUT /rooms/:id 改 status）。
//   开播: POST /api/v1/rooms/:id/live/start → 建场次 + 重置点赞 + status=live
//   下播: POST /api/v1/rooms/:id/live/stop  → 快照点赞入库 + 结算场次 + status=offline
async function toggleLiveStatus() {
  if (!room.value) return

  const authorization = getAuthorizationHeader()
  if (!authorization) {
    goLogin()
    return
  }

  // 当前在播 → 调 stop；当前未播 → 调 start。
  const action = isLive.value ? 'stop' : 'start'

  statusSubmitting.value = true
  consoleError.value = ''

  try {
    const res = await fetch(`/api/v1/rooms/${room.value.id}/live/${action}`, {
      method: 'POST',
      headers: {
        Authorization: authorization,
      },
    })

    if (res.status === 401) {
      goLogin()
      return
    }
    if (res.status === 403) {
      consoleError.value = '你没有权限操作这个直播间'
      return
    }

    const result = (await res.json()) as ApiResponse<unknown>
    if (result.code !== 0) {
      consoleError.value = result.msg || '切换直播状态失败'
      return
    }

    // start 返回 { session_id }、stop 返回 { room_id }，都不是完整 Room，
    // 所以重新拉一次房间详情拿最新 status，isLive/按钮/徽章会跟着更新。
    await fetchRoomDetail()
  } catch {
    consoleError.value = '无法连接服务器'
  } finally {
    statusSubmitting.value = false
  }
}

function goMyRooms() {
  router.push('/my/rooms')
}

onMounted(async () => {
  await fetchRoomDetail()

  if (!room.value) return

  await fetchHistoryMessages()

  connectWebSocket()
})

onUnmounted(() => {
  closeWebSocket()
})
</script>

<template>
  <main class="console-page">
    <header class="page-header">
      <button class="back-button" type="button" @click="goMyRooms">
        返回我的直播间
      </button>

      <div>
        <p class="eyebrow">主播控制台</p>
        <h1>{{ room?.title || '直播间' }}</h1>
      </div>
    </header>

    <p v-if="errorMsg" class="error-message">
      {{ errorMsg }}
    </p>

    <p v-if="loading" class="state-text">
      直播间加载中...
    </p>

    <article v-else-if="room" class="console-content">
      <!-- 主播控制区：目前只有开播/下播，以后踢人、拉黑、福袋往这里加 -->
      <section class="control-bar">
        <div class="control-status">
          <span
            class="live-badge"
            :class="isLive ? 'is-live' : 'is-offline'"
          >
            {{ isLive ? '直播中' : '未开播' }}
          </span>
          <span class="online-hint">当前在线 {{ onlineCount }} 人 · {{ likeCount }} 个赞</span>
        </div>

        <div class="control-actions">
          <button
            class="live-toggle"
            :class="isLive ? 'is-stop' : 'is-start'"
            type="button"
            :disabled="statusSubmitting"
            @click="toggleLiveStatus"
          >
            {{ statusSubmitting ? '处理中...' : isLive ? '结束直播' : '开始直播' }}
          </button>
        </div>
      </section>

      <p v-if="consoleError" class="error-message">
        {{ consoleError }}
      </p>

      <section class="watch-layout">
        <section class="player-panel">
          <video
            class="video-player"
            :src="room.stream_url"
            :poster="room.cover_url"
            controls
            @play="videoPaused = false"
            @pause="videoPaused = true"
          ></video>

          <DanmakuScreen
            ref="danmakuScreenRef"
            :paused="videoPaused"
            :font-size="danmakuFontSize"
          />
        </section>

        <aside class="chat-panel">
          <div class="chat-header">
            <strong>弹幕</strong>
            <span>连接状态：{{ wsStatus }}</span>
            <span>在线人数：{{ onlineCount }}</span>

            <div class="like-control">
              <button
                class="like-button"
                type="button"
                :disabled="wsStatus !== 'connected'"
                aria-label="给当前直播间点赞"
                @click="sendLike"
              >
                ❤ 点赞
              </button>

              <span class="like-count">{{ likeCount }} 个赞</span>
            </div>

            <div class="font-size-switch">
              <span>字号</span>
              <button
                v-for="option in fontSizeOptions"
                :key="option.value"
                type="button"
                :class="{ active: danmakuFontSize === option.value }"
                @click="danmakuFontSize = option.value"
              >
                {{ option.label }}
              </button>
            </div>
          </div>

          <DanmakuList :messages="messages" />

          <DanmakuInput
            :disabled="wsStatus !== 'connected'"
            @send="sendMessage"
          />
        </aside>
      </section>

      <section class="info-area">
        <div class="title-row">
          <span
            class="live-badge"
            :class="isLive ? 'is-live' : 'is-offline'"
          >
            {{ isLive ? '直播中' : '未开播' }}
          </span>

          <span class="viewer-count">
            {{ room.viewer_count }} 人观看
          </span>
        </div>

        <h2>{{ room.title }}</h2>

        <p class="anchor">
          主播：{{ room.anchor_name }}
        </p>

        <p class="description">
          {{ room.description }}
        </p>

        <dl class="meta-list">
          <div>
            <dt>分类</dt>
            <dd>{{ room.category }}</dd>
          </div>

          <div>
            <dt>直播流地址</dt>
            <dd>{{ room.stream_url }}</dd>
          </div>

          <div>
            <dt>创建时间</dt>
            <dd>{{ room.created_at }}</dd>
          </div>
        </dl>
      </section>
    </article>

    <p v-else class="state-text">
      没有找到直播间
    </p>
  </main>
</template>

<style scoped>
.console-page {
  min-height: 100vh;
  padding: 24px 24px 48px;
  background: #f5f7fb;
}

.page-header,
.console-content,
.error-message,
.state-text {
  width: min(1760px, calc(100vw - 48px));
  margin-left: auto;
  margin-right: auto;
}

.page-header {
  margin-bottom: 24px;
  display: flex;
  align-items: center;
  gap: 18px;
}

.back-button {
  padding: 10px 16px;
  color: #ffffff;
  border: 0;
  border-radius: 10px;
  cursor: pointer;
  background: #6874e8;
}

.eyebrow {
  margin: 0 0 4px;
  color: #6874e8;
  font-size: 13px;
  font-weight: 700;
}

.page-header h1 {
  margin: 0;
  color: #20243a;
  font-size: 30px;
  font-weight: 800;
}

.error-message {
  margin-bottom: 18px;
  padding: 12px 14px;
  color: #d84646;
  border-radius: 10px;
  background: #fff1f1;
}

.state-text {
  color: #858a9f;
}

.console-content {
  display: block;
}

/* ===== 主播控制区 ===== */
.control-bar {
  margin-bottom: 16px;
  padding: 16px 20px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  border: 1px solid #e5e8f0;
  border-radius: 12px;
  background: #ffffff;
  box-shadow: 0 12px 32px rgba(35, 44, 85, 0.08);
}

.control-status {
  display: flex;
  align-items: center;
  gap: 14px;
}

.online-hint {
  color: #7d8498;
  font-size: 14px;
}

.control-actions {
  display: flex;
  gap: 12px;
}

.live-toggle {
  padding: 10px 24px;
  color: #ffffff;
  font: inherit;
  font-weight: 700;
  border: 0;
  border-radius: 10px;
  cursor: pointer;
  transition:
    transform 0.15s,
    opacity 0.15s;
}

.live-toggle:hover:not(:disabled) {
  transform: translateY(-1px);
}

.live-toggle:disabled {
  cursor: not-allowed;
  opacity: 0.6;
}

/* 开始直播：绿色。结束直播：红色 */
.live-toggle.is-start {
  background: #22c55e;
}

.live-toggle.is-stop {
  background: #ef4444;
}

.watch-layout {
  display: grid;
  grid-template-columns: minmax(0, 1fr) clamp(340px, 24vw, 430px);
  gap: 16px;
  align-items: stretch;
}

.player-panel,
.info-area,
.chat-panel {
  overflow: hidden;
  border: 1px solid #e5e8f0;
  border-radius: 12px;
  background: #ffffff;
  box-shadow: 0 18px 50px rgba(35, 44, 85, 0.1);
}

.player-panel {
  position: relative;
  background: #111827;
}

.video-player {
  width: 100%;
  aspect-ratio: 16 / 9;
  display: block;
  background: #111827;
}

.info-area {
  margin-top: 16px;
  padding: 22px 24px;
}

.title-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 16px;
  margin-bottom: 14px;
}

.live-badge {
  padding: 4px 10px;
  color: #ffffff;
  font-size: 12px;
  font-weight: 700;
  border-radius: 999px;
}

.live-badge.is-live {
  background: #ef4444;
}

.live-badge.is-offline {
  background: #8a90a3;
}

.viewer-count {
  color: #7d8498;
  font-size: 14px;
}

.info-area h2 {
  margin: 0 0 10px;
  color: #20243a;
  font-size: 28px;
  font-weight: 800;
}

.anchor {
  margin: 0 0 14px;
  color: #6874e8;
  font-weight: 700;
}

.description {
  margin: 0 0 22px;
  color: #6f7688;
  line-height: 1.8;
}

.meta-list {
  display: grid;
  gap: 14px;
  margin: 0;
}

dt {
  color: #8a90a3;
  font-size: 13px;
}

dd {
  margin: 4px 0 0;
  color: #252a42;
  word-break: break-all;
}

.chat-panel {
  padding: 16px;
  display: flex;
  flex-direction: column;
  min-height: 0;
}

.chat-header {
  margin-bottom: 12px;
  display: flex;
  align-items: flex-start;
  flex-direction: column;
  gap: 6px;
  color: #6874e8;
  font-size: 14px;
}

.like-control {
  display: flex;
  align-items: center;
  gap: 10px;
}

.like-button {
  padding: 6px 14px;
  color: #ffffff;
  font: inherit;
  font-weight: 700;
  border: 0;
  border-radius: 999px;
  cursor: pointer;
  background: #ef476f;
  transition:
    transform 0.15s,
    opacity 0.15s;
}

.like-button:hover:not(:disabled) {
  transform: translateY(-1px) scale(1.02);
}

.like-button:disabled {
  cursor: not-allowed;
  opacity: 0.55;
}

.like-count {
  color: #ef476f;
  font-weight: 700;
}

.font-size-switch {
  display: flex;
  align-items: center;
  gap: 6px;
}

.font-size-switch button {
  padding: 2px 10px;
  color: #6874e8;
  border: 1px solid #d9deea;
  border-radius: 8px;
  cursor: pointer;
  background: #ffffff;
}

.font-size-switch button.active {
  color: #ffffff;
  border-color: #6874e8;
  background: #6874e8;
}

@media (max-width: 900px) {
  .watch-layout {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 640px) {
  .console-page {
    padding: 22px;
  }

  .page-header,
  .console-content,
  .error-message,
  .state-text {
    width: calc(100vw - 44px);
  }

  .page-header {
    align-items: flex-start;
    flex-direction: column;
  }

  .back-button {
    width: 100%;
  }

  .control-bar {
    flex-direction: column;
    align-items: flex-start;
  }

  .info-area h2 {
    font-size: 24px;
  }
}
</style>
