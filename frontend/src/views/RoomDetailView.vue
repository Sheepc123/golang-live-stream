<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import DanmakuInput from '@/components/DanmakuInput.vue'
import DanmakuList from '@/components/DanmakuList.vue'
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
  anchorName: string
  category: string
  coverURL: string
  streamURL: string
  description: string
  status: string
  viewerCount: number
  createdAt: string
}

interface BackendRoom {
  ID?: number
  Title?: string
  ChannelName?: string
  Category?: string
  CoverURL?: string
  StreamURL?: string
  Description?: string
  Status?: string
  ViewerCount?: number
  CreatedAt?: string

  id?: number
  title?: string
  anchor_name?: string
  category?: string
  cover_url?: string
  stream_url?: string
  description?: string
  status?: string
  viewer_count?: number
  created_at?: string
}

const room = ref<Room | null>(null)
const loading = ref(false)
const errorMsg = ref('')

const liveSocket = ref<LiveSocket | null>(null)
const wsStatus = ref<LiveSocketStatus>('disconnected')
const messages = ref<WSMessage[]>([])
const onlineCount = ref(0)

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

function normalizeRoom(data: BackendRoom): Room {
  return {
    id: data.id ?? data.ID ?? 0,
    title: data.title ?? data.Title ?? '',
    anchorName: data.anchor_name ?? data.ChannelName ?? '',
    category: data.category ?? data.Category ?? '',
    coverURL: data.cover_url ?? data.CoverURL ?? '',
    streamURL: data.stream_url ?? data.StreamURL ?? '',
    description: data.description ?? data.Description ?? '',
    status: data.status ?? data.Status ?? '',
    viewerCount: data.viewer_count ?? data.ViewerCount ?? 0,
    createdAt: data.created_at ?? data.CreatedAt ?? '',
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

    const result = (await res.json()) as ApiResponse<BackendRoom>

    if (result.code !== 0) {
      errorMsg.value = result.msg || '获取直播间详情失败'
      return
    }

    room.value = normalizeRoom(result.data)
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
    timestamp: Math.floor(Date.now() / 1000),
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

  if (msg.type === 'heartbeat') return

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

function closeWebSocket() {
  liveSocket.value?.close()
  liveSocket.value = null
}

function goHome() {
  router.push('/rooms')
}

onMounted(() => {
  fetchRoomDetail()
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
            :src="room.streamURL"
            :poster="room.coverURL"
            controls
          ></video>
        </section>

        <aside class="chat-panel">
          <div class="chat-header">
            <strong>弹幕</strong>
            <span>连接状态：{{ wsStatus }}</span>
            <span>在线人数：{{ onlineCount }}</span>
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
          <span class="live-badge">
            {{ room.status }}
          </span>

          <span class="viewer-count">
            {{ room.viewerCount }} 人观看
          </span>
        </div>

        <h2>{{ room.title }}</h2>

        <p class="anchor">
          主播：{{ room.anchorName }}
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
            <dd>{{ room.streamURL }}</dd>
          </div>

          <div>
            <dt>创建时间</dt>
            <dd>{{ room.createdAt }}</dd>
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
  background: #ef4444;
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
