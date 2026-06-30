<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'

const route = useRoute()
const router = useRouter()

/**
 * 后端统一响应格式。
 */
interface ApiResponse<T> {
  code: number
  msg: string
  data: T
}

/**
 * 前端页面最终使用的直播间数据结构。
 *
 * 这里统一使用前端更容易理解的字段名。
 */
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

/**
 * 后端单个直播间接口当前可能返回的是 Go 结构体默认字段：
 * ID、Title、ChannelName、CoverURL...
 *
 * 后面更推荐后端 GetRoomByID 也返回 RoomResponse，
 * 这样列表接口和详情接口字段就统一了。
 */
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

/**
 * 从路由参数中拿直播间 ID。
 *
 * 路由后面会写成：
 * /rooms/:room_id
 */
const roomID = computed(() => {
  const value = route.params.room_id

  if (Array.isArray(value)) {
    return value[0]
  }

  return value
})

/**
 * 生成后端 JWT 中间件需要的请求头内容。
 */
function getAuthorizationHeader() {
  const token = localStorage.getItem('token')
  const tokenType = localStorage.getItem('token_type') || 'Bearer'

  if (!token) {
    return null
  }

  return `${tokenType} ${token}`
}

/**
 * 清理登录状态。
 */
function clearAuthStorage() {
  localStorage.removeItem('token')
  localStorage.removeItem('token_type')
  localStorage.removeItem('expires_in')
  localStorage.removeItem('login_at')
  localStorage.removeItem('user')
}

/**
 * 回到登录页。
 */
function goLogin() {
  clearAuthStorage()
  router.push('/login')
}

/**
 * 把后端返回的数据转换成前端页面统一使用的数据。
 *
 * 这样做的原因：
 * 当前列表接口返回的是 cover_url 这种字段；
 * 但单个详情接口可能返回 CoverURL 这种字段。
 */
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

/**
 * 请求单个直播间详情。
 *
 * 对应后端接口：
 * GET /api/v1/rooms/:room_id
 */
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

/**
 * 返回直播间首页。
 */
function goHome() {
  router.push('/home')
}

onMounted(() => {
  fetchRoomDetail()
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

    <article v-else-if="room" class="room-detail">
      <section class="player-area">
        <video
          class="video-player"
          :src="room.streamURL"
          :poster="room.coverURL"
          controls
        ></video>
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
  padding: 32px;
  background: #f5f7fb;
}

.page-header,
.room-detail,
.error-message,
.state-text {
  max-width: 1180px;
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

.room-detail {
  overflow: hidden;
  border: 1px solid #e5e8f0;
  border-radius: 16px;
  background: #ffffff;
  box-shadow: 0 18px 50px rgba(35, 44, 85, 0.1);
}

.player-area {
  background: #111827;
}

.video-player {
  width: 100%;
  aspect-ratio: 16 / 9;
  display: block;
  background: #111827;
}

.info-area {
  padding: 24px;
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

@media (max-width: 640px) {
  .detail-page {
    padding: 22px;
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