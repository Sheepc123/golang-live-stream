<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import DanmakuInput from '@/components/DanmakuInput.vue'
import DanmakuList from '@/components/DanmakuList.vue'
import DanmakuScreen from '@/components/DanmakuScreen.vue'
import { LiveSocket, type LiveSocketStatus, type WSMessage } from '@/utils/liveSocket'

const route = useRoute()
const router = useRouter()

interface ApiResponse<T> {
  code: number
  msg: string
  data: T
}

interface Room {
  id: number
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

// 当前房间的权威点赞总数。
// 页面不在本地执行 likeCount++，而是等待后端的 like_count 消息，
// 避免断线、重连或漏消息后与后端计数不一致。
const likeCount = ref(0)

// 弹幕飘屏组件的实例引用。
// 通过它调用子组件 defineExpose 出来的 push 方法，让弹幕飞过视频。
const danmakuScreenRef = ref<InstanceType<typeof DanmakuScreen> | null>(null)

// 视频当前是否处于暂停状态，传给飘屏组件，暂停时弹幕一起冻结。
// 由 video 的 @play / @pause 事件在模板里直接切换。
const videoPaused = ref(false)

// 弹幕字号（像素）。提供「小 / 中 / 大」三档，默认中。
const danmakuFontSize = ref(20)
const fontSizeOptions = [
  { label: '小', value: 16 },
  { label: '中', value: 20 },
  { label: '大', value: 26 },
]

const roomID = computed(() => {
  const value = route.params.room_id
  return Array.isArray(value) ? value[0] : value
})

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

    // token 失效直接回登录页
    if (res.status === 401) {
      goLogin()
      return
    }

    const result = (await res.json()) as ApiResponse<HistoryListResponse>
    if (result.code !== 0) return

    // 把后端的 HistoryMessage 转成前端统一用的 WSMessage 结构。
    // 后端返回的 type 是 string，这里断言成 WSMessage['type']。
    const historyList: WSMessage[] = result.data.messages.map((item) => ({
      type: item.type as WSMessage['type'],
      room_id: item.room_id,
      user_id: item.user_id,
      username: item.username,
      content: item.content,
      timestamp: item.timestamp,
    }))

    // 教学要点：历史弹幕要放在最前面。
    // 因为后端已经按时间正序返回了（老的在前、新的在后），
    // 直接赋值给 messages，后续 WebSocket 收到的新弹幕会 push 到后面，
    // 时间顺序刚好正确。
    messages.value = historyList
  } catch {
    // 拉历史失败不影响看直播和发新弹幕，静默处理即可。
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

  // like_count 是房间状态，只更新点赞数字，不进入聊天消息列表。
  if (msg.type === 'like_count') {
    const nextLikeCount = Number(msg.content)

    if (Number.isFinite(nextLikeCount)) {
      likeCount.value = nextLikeCount
    }

    return
  }

  if (msg.type === 'heartbeat') return

  // 只有聊天弹幕才飘屏，join/leave/system 等只进侧边栏列表，不飞过视频。
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

// 发送一次点赞动作。
// 页面不直接修改 likeCount，后端计数成功并广播 like_count 后再更新。
function sendLike() {
  liveSocket.value?.sendLike()
}

function closeWebSocket() {
  liveSocket.value?.close()
  liveSocket.value = null
}

function goHome() {
  router.push('/rooms')
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
  <main class="detail-page">
    <header class="page-header">
      <button class="back-button" type="button" @click="goHome">
        返回首页
      </button>

      <div>
        <p class="eyebrow">直播间详情</p>
        <h1>{{ room?.title || '直播间' }}</h1>
      </div>
    </header>

    <p v-if="errorMsg" class="error-message">
      {{ errorMsg }}
    </p>

    <p v-if="loading" class="state-text">
      直播间加载中...
    </p>

    <article v-else-if="room" class="room-page-content">
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

          <!-- 弹幕飘屏层：绝对定位覆盖在视频之上，pointer-events:none 不挡视频操作 -->
          <!-- paused 传视频暂停状态；font-size 传当前字号，切换即时生效 -->
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

            <!-- 字号切换：点击按钮切换飘屏弹幕字号，选中项高亮 -->
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
          <!-- 根据 status 动态挂类名 + 显示中文文案 -->
          <!-- status === 'live' 说明在开播，否则一律视为未开播 -->
          <span
            class="live-badge"
            :class="room.status === 'live' ? 'is-live' : 'is-offline'"
          >
            {{ room.status === 'live' ? '直播中' : '未开播' }}
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
.detail-page {
  min-height: 100vh;
  padding: 24px 24px 48px;
  background: #f5f7fb;
}

.page-header,
.room-page-content,
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

.room-page-content {
  display: block;
}

.watch-layout {
  display: grid;
  /*
   * 左侧放直播画面，右侧放弹幕。
   * 右侧弹幕给固定的舒适宽度，左侧播放器吃掉剩余空间。
   * 这样在宽屏 Web 页面上，直播画面会真正变大。
   */
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
  /* 作为弹幕飘屏层(绝对定位 inset:0)的定位父容器 */
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
  /* 背景色不在这里写死，交给下面两个状态类决定 */
}

/* 开播：红色，醒目 */
.live-badge.is-live {
  background: #ef4444;
}

/* 未开播：灰色，弱化 */
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

/* 字号切换：一行排列的小按钮 */
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

/* 当前选中的字号按钮高亮 */
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
  .detail-page {
    padding: 22px;
  }

  .page-header,
  .room-page-content,
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

  .info-area h2 {
    font-size: 24px;
  }

}
</style>
