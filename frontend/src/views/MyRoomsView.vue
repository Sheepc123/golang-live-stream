<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { useRouter } from 'vue-router'

const router = useRouter()

// ===== 类型定义：与后端 RoomResponse 对齐（字段名要和后端 JSON 一致）=====
interface ApiResponse<T> {
  code: number
  msg: string
  data: T
}

interface Room {
  id: number
  owner_id: number // 后端新加的归属字段
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

interface RoomListResponse {
  rooms: Room[]
  total: number
}

// ===== 列表相关状态 =====
const rooms = ref<Room[]>([])
const loading = ref(false)
const errorMsg = ref('')

// ===== 表单（弹窗）相关状态 =====
// showForm 控制弹窗是否显示。
// editingId 为 null 表示“新建”，为某个数字表示“编辑那个 id 的房间”。
// 用同一个弹窗、同一份 form 承担“新建”和“编辑”两种场景，避免重复代码。
const showForm = ref(false)
const editingId = ref<number | null>(null)
const submitting = ref(false)
const formError = ref('')

// 表单字段，新建/编辑共用这一份。
const form = reactive({
  title: '',
  anchor_name: '',
  category: '',
  cover_url: '',
  stream_url: '',
  description: '',
  status: 'offline', // 仅编辑时用；新建时后端固定 offline
})

// ===== 鉴权工具（与 HomeView / RoomDetailView 保持一致）=====
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

// ===== 拉取“我的直播间”列表 =====
async function fetchMyRooms() {
  const authorization = getAuthorizationHeader()
  if (!authorization) {
    goLogin()
    return
  }

  loading.value = true
  errorMsg.value = ''

  try {
    // 对应后端 GET /api/v1/rooms/mine，只返回当前登录用户拥有的房间
    const res = await fetch('/api/v1/rooms/mine', {
      method: 'GET',
      headers: { Authorization: authorization },
    })

    if (res.status === 401) {
      goLogin()
      return
    }

    const result = (await res.json()) as ApiResponse<RoomListResponse>
    if (result.code !== 0) {
      errorMsg.value = result.msg || '获取我的直播间失败'
      return
    }

    rooms.value = result.data.rooms
  } catch {
    errorMsg.value = '无法连接服务器，请确认后端是否已经启动'
  } finally {
    loading.value = false
  }
}

// ===== 打开/关闭弹窗 =====

// 打开“新建”：清空表单，editingId 置空
function openCreate() {
  editingId.value = null
  form.title = ''
  form.anchor_name = ''
  form.category = ''
  form.cover_url = ''
  form.stream_url = ''
  form.description = ''
  form.status = 'offline'
  formError.value = ''
  showForm.value = true
}

// 打开“编辑”：把选中房间的当前值预填进表单，记住 id
function openEdit(room: Room) {
  editingId.value = room.id
  form.title = room.title
  form.anchor_name = room.anchor_name
  form.category = room.category
  form.cover_url = room.cover_url
  form.stream_url = room.stream_url
  form.description = room.description
  form.status = room.status
  formError.value = ''
  showForm.value = true
}

function closeForm() {
  showForm.value = false
}

// ===== 提交表单：根据 editingId 自动区分“新建 / 编辑” =====
async function submitForm() {
  const authorization = getAuthorizationHeader()
  if (!authorization) {
    goLogin()
    return
  }

  // 前端先做必填校验，减少无效请求（后端 binding:"required" 也会再兜一层）
  if (!form.title.trim()) {
    formError.value = '标题不能为空'
    return
  }

  submitting.value = true
  formError.value = ''

  // editingId 为 null → 新建(POST)，否则 → 编辑(PUT /:id)
  const isEdit = editingId.value !== null
  const url = isEdit ? `/api/v1/rooms/${editingId.value}` : '/api/v1/rooms'
  const method = isEdit ? 'PUT' : 'POST'

  // 请求体：新建不带 status（后端固定 offline），只有编辑才提交 status
  const body: Record<string, unknown> = {
    title: form.title,
    anchor_name: form.anchor_name,
    category: form.category,
    cover_url: form.cover_url,
    stream_url: form.stream_url,
    description: form.description,
  }
  if (isEdit) {
    body.status = form.status
  }

  try {
    const res = await fetch(url, {
      method,
      headers: {
        'Content-Type': 'application/json',
        Authorization: authorization,
      },
      body: JSON.stringify(body),
    })

    if (res.status === 401) {
      goLogin()
      return
    }
    // 403 = 后端判定“不是房主”。正常操作自己的房间不会触发，这里做防御性提示
    if (res.status === 403) {
      formError.value = '你没有权限操作这个直播间'
      return
    }

    const result = (await res.json()) as ApiResponse<Room>
    if (result.code !== 0) {
      formError.value = result.msg || '保存失败'
      return
    }

    // 成功：关闭弹窗并刷新列表
    showForm.value = false
    await fetchMyRooms()
  } catch {
    formError.value = '无法连接服务器'
  } finally {
    submitting.value = false
  }
}

// ===== 删除房间 =====
async function removeRoom(room: Room) {
  // 学习项目用原生 confirm 足够；生产环境通常换成自定义确认弹窗
  if (!window.confirm(`确定删除直播间「${room.title}」吗？`)) return

  const authorization = getAuthorizationHeader()
  if (!authorization) {
    goLogin()
    return
  }

  try {
    const res = await fetch(`/api/v1/rooms/${room.id}`, {
      method: 'DELETE',
      headers: { Authorization: authorization },
    })

    if (res.status === 401) {
      goLogin()
      return
    }
    if (res.status === 403) {
      errorMsg.value = '你没有权限删除这个直播间'
      return
    }

    const result = (await res.json()) as ApiResponse<unknown>
    if (result.code !== 0) {
      errorMsg.value = result.msg || '删除失败'
      return
    }

    await fetchMyRooms()
  } catch {
    errorMsg.value = '无法连接服务器'
  }
}

function goHome() {
  router.push('/rooms')
}

// 点封面进入观看页（复用已有的 /rooms/:room_id）
function openRoom(roomID: number) {
  router.push(`/rooms/${roomID}`)
}

onMounted(() => {
  fetchMyRooms()
})
</script>

<template>
  <main class="my-rooms-page">
    <header class="page-header">
      <div class="header-left">
        <button class="ghost-button" type="button" @click="goHome">返回首页</button>
        <div>
          <p class="eyebrow">创作者中心</p>
          <h1>我的直播间</h1>
        </div>
      </div>

      <button class="primary-button" type="button" @click="openCreate">
        + 新建直播间
      </button>
    </header>

    <p v-if="errorMsg" class="error-message">{{ errorMsg }}</p>

    <p v-if="loading" class="state-text">加载中...</p>

    <p v-else-if="rooms.length === 0" class="state-text">
      你还没有创建任何直播间，点击右上角「新建直播间」开始吧。
    </p>

    <div v-else class="room-grid">
      <article v-for="room in rooms" :key="room.id" class="room-card">
        <!-- 点封面进入观看页 -->
        <div class="cover-wrap" @click="openRoom(room.id)">
          <img :src="room.cover_url" :alt="room.title" />
          <span
            class="status-badge"
            :class="room.status === 'live' ? 'is-live' : 'is-offline'"
          >
            {{ room.status === 'live' ? '直播中' : '未开播' }}
          </span>
        </div>

        <div class="room-body">
          <h3>{{ room.title }}</h3>
          <p class="anchor">{{ room.anchor_name || '未设置频道名' }}</p>
          <p class="description">{{ room.description || '暂无简介' }}</p>

          <div class="card-actions">
            <button class="edit-button" type="button" @click="openEdit(room)">
              编辑
            </button>
            <button class="delete-button" type="button" @click="removeRoom(room)">
              删除
            </button>
          </div>
        </div>
      </article>
    </div>

    <!-- 新建 / 编辑 弹窗：点遮罩空白处(self)关闭 -->
    <div v-if="showForm" class="modal-mask" @click.self="closeForm">
      <div class="modal">
        <h2>{{ editingId === null ? '新建直播间' : '编辑直播间' }}</h2>

        <form class="room-form" @submit.prevent="submitForm">
          <label class="field">
            <span>标题 *</span>
            <input v-model="form.title" type="text" placeholder="直播间标题" :disabled="submitting" />
          </label>

          <label class="field">
            <span>频道 / 主播名</span>
            <input v-model="form.anchor_name" type="text" placeholder="例如 Time Music" :disabled="submitting" />
          </label>

          <label class="field">
            <span>分类</span>
            <input v-model="form.category" type="text" placeholder="例如 Music / Gaming" :disabled="submitting" />
          </label>

          <label class="field">
            <span>封面图 URL</span>
            <input v-model="form.cover_url" type="text" placeholder="https://..." :disabled="submitting" />
          </label>

          <label class="field">
            <span>直播流 URL</span>
            <input v-model="form.stream_url" type="text" placeholder="https://xxx.m3u8" :disabled="submitting" />
          </label>

          <label class="field">
            <span>简介</span>
            <textarea v-model="form.description" rows="3" placeholder="直播间简介" :disabled="submitting"></textarea>
          </label>

          <!-- 状态只在“编辑”时出现：新建时后端固定 offline -->
          <label v-if="editingId !== null" class="field">
            <span>状态</span>
            <select v-model="form.status" :disabled="submitting">
              <option value="offline">未开播 (offline)</option>
              <option value="live">直播中 (live)</option>
            </select>
          </label>

          <p v-if="formError" class="error-message">{{ formError }}</p>

          <div class="modal-actions">
            <button class="ghost-button" type="button" :disabled="submitting" @click="closeForm">
              取消
            </button>
            <button class="primary-button" type="submit" :disabled="submitting">
              {{ submitting ? '保存中...' : '保存' }}
            </button>
          </div>
        </form>
      </div>
    </div>
  </main>
</template>

<style scoped>
.my-rooms-page {
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

.header-left {
  display: flex;
  align-items: center;
  gap: 16px;
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
  font-size: 28px;
  font-weight: 800;
}

/* 通用按钮 */
.primary-button {
  padding: 10px 18px;
  color: #fff;
  font: inherit;
  font-weight: 700;
  border: 0;
  border-radius: 10px;
  cursor: pointer;
  background: #6874e8;
}
.primary-button:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.ghost-button {
  padding: 10px 16px;
  color: #6874e8;
  font: inherit;
  border: 1px solid #d9deea;
  border-radius: 10px;
  cursor: pointer;
  background: #fff;
}

.error-message,
.state-text {
  max-width: 1180px;
  margin: 0 auto 18px;
}
.error-message {
  padding: 12px 14px;
  color: #d84646;
  border-radius: 10px;
  background: #fff1f1;
}
.state-text {
  color: #858a9f;
}

/* 卡片网格，复用首页风格 */
.room-grid {
  max-width: 1180px;
  margin: 0 auto;
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
  gap: 20px;
}

.room-card {
  overflow: hidden;
  border: 1px solid #e5e8f0;
  border-radius: 14px;
  background: #fff;
  box-shadow: 0 12px 32px rgba(35, 44, 85, 0.08);
}

.cover-wrap {
  position: relative;
  aspect-ratio: 16 / 9;
  cursor: pointer;
  background: #e8ebf3;
}
.cover-wrap img {
  width: 100%;
  height: 100%;
  display: block;
  object-fit: cover;
}

.status-badge {
  position: absolute;
  top: 12px;
  left: 12px;
  padding: 4px 10px;
  color: #fff;
  font-size: 12px;
  font-weight: 700;
  border-radius: 999px;
}
.status-badge.is-live {
  background: #ef4444;
}
.status-badge.is-offline {
  background: #8a90a3;
}

.room-body {
  padding: 16px;
}
.room-body h3 {
  margin: 0 0 6px;
  color: #20243a;
  font-size: 17px;
  font-weight: 800;
}
.anchor {
  margin: 0 0 8px;
  color: #6874e8;
  font-size: 14px;
  font-weight: 700;
}
.description {
  min-height: 40px;
  margin: 0 0 14px;
  color: #6f7688;
  font-size: 14px;
  line-height: 1.6;
}

.card-actions {
  display: flex;
  gap: 10px;
}
.edit-button,
.delete-button {
  flex: 1;
  padding: 8px 0;
  font: inherit;
  font-weight: 700;
  border: 0;
  border-radius: 8px;
  cursor: pointer;
}
.edit-button {
  color: #fff;
  background: #6874e8;
}
.delete-button {
  color: #d84646;
  background: #fff1f1;
}

/* ===== 弹窗 ===== */
.modal-mask {
  position: fixed;
  inset: 0;
  z-index: 100;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 20px;
  background: rgba(20, 24, 40, 0.5);
}
.modal {
  width: 100%;
  max-width: 480px;
  max-height: 90vh;
  overflow-y: auto;
  padding: 24px;
  border-radius: 14px;
  background: #fff;
}
.modal h2 {
  margin: 0 0 18px;
  color: #20243a;
  font-size: 20px;
}

.room-form {
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.field {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.field span {
  color: #3e435a;
  font-size: 13px;
  font-weight: 600;
}
.field input,
.field textarea,
.field select {
  padding: 10px 12px;
  color: #25293c;
  font: inherit;
  border: 1px solid #e1e4ed;
  border-radius: 10px;
  outline: none;
  background: #fafbfc;
}
.field textarea {
  resize: vertical;
}

.modal-actions {
  margin-top: 6px;
  display: flex;
  justify-content: flex-end;
  gap: 12px;
}
</style>
