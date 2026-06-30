<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'

const router = useRouter()

/**
 * 后端统一响应格式。
 *
 * 你的后端 response.Ok(c, data) 返回：
 * {
 *   code: 0,
 *   msg: "success",
 *   data: ...
 * }
 */
interface ApiResponse<T> {
  code: number
  msg: string
  data: T
}

/**
 * 对应后端 model.RoomResponse。
 *
 * 注意：
 * 这里的字段名要和后端 JSON 返回字段保持一致。
 * 比如后端返回的是 cover_url，前端就写 cover_url。
 */
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

/**
 * 对应后端 model.RoomListResponse。
 */
interface RoomListResponse {
  rooms: Room[]
  total: number
}

const rooms = ref<Room[]>([])
const total = ref(0)
const loading = ref(false)
const errorMsg = ref('')

/**
 * 生成 Authorization 请求头。
 *
 * 后端 JWT 中间件要求格式：
 * Authorization: Bearer xxxxx
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
 *
 * token 失效、用户退出登录时都需要清理。
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
 * 请求直播间列表。
 *
 * 对应后端接口：
 * GET /api/v1/rooms
 */
async function fetchRooms() {
  const authorization = getAuthorizationHeader()

  if (!authorization) {
    goLogin()
    return
  }

  loading.value = true
  errorMsg.value = ''

  try {
    const res = await fetch('/api/v1/rooms', {
      method: 'GET',
      headers: {
        Authorization: authorization,
      },
    })

    /**
     * 如果 token 无效或过期，后端会返回 401。
     * 前端收到 401 后，直接回到登录页。
     */
    if (res.status === 401) {
      goLogin()
      return
    }

    const result = (await res.json()) as ApiResponse<RoomListResponse>

    if (result.code !== 0) {
      errorMsg.value = result.msg || '获取直播间失败'
      return
    }

    rooms.value = result.data.rooms
    total.value = result.data.total
  } catch {
    errorMsg.value = '无法连接服务器，请确认后端是否已经启动'
  } finally {
    loading.value = false
  }
}

/**
 * 退出登录。
 */
function logout() {
  goLogin()
}

/**
 * 点击直播间卡片后，进入对应的直播间详情页。
 *
 * 例如：
 * roomID = 1
 * 跳转地址就是 /rooms/1
 */
function openRoom(roomID: number) {
  router.push(`/rooms/${roomID}`)
}

onMounted(() => {
  fetchRooms()
})
</script>

<template>
  <main class="home-page">
    <header class="page-header">
      <div>
        <p class="eyebrow">Live Stream</p>
        <h1>实时直播</h1>
        <p class="subtitle">选择一个直播间，进入正在发生的精彩现场。</p>
      </div>

      <button class="logout-button" type="button" @click="logout">
        退出登录
      </button>
    </header>

    <p v-if="errorMsg" class="error-message">
      {{ errorMsg }}
    </p>

    <section class="room-section">
      <div class="section-title">
        <h2>直播间</h2>
        <span>{{ total }} 个直播间</span>
      </div>

      <p v-if="loading" class="state-text">直播间加载中...</p>

      <p v-else-if="rooms.length === 0" class="state-text">
        暂无直播间
      </p>

      <div v-else class="room-grid">
        <button
          v-for="room in rooms"
          :key="room.id"
          class="room-card"
          type="button"
          @click="openRoom(room.id)"
        >
          <div class="cover-wrap">
            <img :src="room.cover_url" :alt="room.title" />

            <span class="live-badge">
              {{ room.status }}
            </span>
          </div>

          <div class="room-body">
            <h3>{{ room.title }}</h3>

            <p class="anchor">
              {{ room.anchor_name }}
            </p>

            <p class="description">
              {{ room.description }}
            </p>

            <div class="room-meta">
              <span>{{ room.category }}</span>
              <span>{{ room.viewer_count }} 人观看</span>
            </div>
          </div>
        </button>
      </div>
    </section>
  </main>
</template>

<style scoped>
.home-page {
  min-height: 100vh;
  padding: 32px;
  background: #f5f7fb;
}

.page-header {
  max-width: 1180px;
  margin: 0 auto 28px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 20px;
}

.eyebrow {
  margin: 0 0 6px;
  color: #6874e8;
  font-size: 30px;
  font-weight: 700;
}

.page-header h1 {
  margin: 0;
  color: #20243a;
  font-size: 34px;
  font-weight: 800;
}

.subtitle {
  margin: 8px 0 0;
  color: #7d8498;
}

.logout-button {
  padding: 10px 18px;
  color: #ffffff;
  border: 0;
  border-radius: 10px;
  cursor: pointer;
  background: #6874e8;
}

.error-message,
.room-section {
  max-width: 1180px;
  margin-left: auto;
  margin-right: auto;
}

.error-message {
  margin-bottom: 18px;
  padding: 12px 14px;
  color: #d84646;
  border-radius: 10px;
  background: #fff1f1;
}

.section-title {
  margin-bottom: 18px;
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 16px;
}

.section-title h2 {
  margin: 0;
  color: #252a42;
  font-size: 22px;
  font-weight: 800;
}

.section-title span {
  color: #8a90a3;
  font-size: 14px;
}

.room-grid {
  display: grid;

  /*
   * auto-fit：浏览器自动判断一行放几个卡片。
   * minmax(260px, 1fr)：卡片最小 260px，空间足够时平均分配宽度。
   */
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
  gap: 20px;
}

.room-card {
  min-width: 0;
  overflow: hidden;
  padding: 0;
  text-align: left;
  border: 1px solid #e5e8f0;
  border-radius: 14px;
  cursor: pointer;
  font: inherit;
  background: #ffffff;
  box-shadow: 0 12px 32px rgba(35, 44, 85, 0.08);
  transition:
    transform 0.2s,
    border-color 0.2s,
    box-shadow 0.2s;
}

.room-card:hover {
  transform: translateY(-3px);
  border-color: #6874e8;
  box-shadow: 0 18px 42px rgba(35, 44, 85, 0.14);
}

.room-card:focus-visible {
  outline: 3px solid rgba(104, 116, 232, 0.35);
  outline-offset: 3px;
}

.cover-wrap {
  position: relative;
  aspect-ratio: 16 / 9;
  background: #e8ebf3;
}

.cover-wrap img {
  width: 100%;
  height: 100%;
  display: block;
  object-fit: cover;
}

.live-badge {
  position: absolute;
  top: 12px;
  left: 12px;
  padding: 4px 10px;
  color: #ffffff;
  font-size: 12px;
  font-weight: 700;
  border-radius: 999px;
  background: #ef4444;
}

.room-body {
  padding: 16px;
}

.room-body h3 {
  margin: 0 0 8px;
  color: #20243a;
  font-size: 17px;
  font-weight: 800;
}

.anchor {
  margin: 0 0 10px;
  color: #6874e8;
  font-size: 14px;
  font-weight: 700;
}

.description {
  min-height: 44px;
  margin: 0 0 14px;
  color: #6f7688;
  font-size: 14px;
  line-height: 1.6;
}

.room-meta {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  color: #7d8498;
  font-size: 13px;
}

.state-text {
  color: #858a9f;
}

@media (max-width: 640px) {
  .home-page {
    padding: 22px;
  }

  .page-header {
    align-items: flex-start;
    flex-direction: column;
  }

  .page-header h1 {
    font-size: 28px;
  }

  .logout-button {
    width: 100%;
  }
}
</style>
