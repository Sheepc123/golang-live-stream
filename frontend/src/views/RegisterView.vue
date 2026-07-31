<script setup lang="ts">
import { reactive, ref } from 'vue'
import { useRouter } from 'vue-router'

const router = useRouter()

const form = reactive({
  username: '',
  password: '',
  confirmPassword: '',
})

const showPassword = ref(false)
const loading = ref(false)
const errorMsg = ref('')

// 与后端 usernamePattern + binding tag 保持一致的规则。
// 前端校验只为了即时提示，真正的把关在后端 —— 绕过前端不会绕过服务器。
const USERNAME_RE = /^[a-zA-Z0-9_]{3,32}$/

function validate(): string {
  const username = form.username.trim()

  if (!username) return '请输入用户名'
  if (!USERNAME_RE.test(username)) return '用户名需为 3-32 位字母、数字或下划线'
  if (!form.password) return '请输入密码'
  if (form.password.length < 6 || form.password.length > 32) return '密码长度需为 6-32 位'
  if (form.password !== form.confirmPassword) return '两次输入的密码不一致'

  return ''
}

async function handleRegister() {
  errorMsg.value = ''

  const msg = validate()
  if (msg) {
    errorMsg.value = msg
    return
  }

  loading.value = true

  try {
    // 确认密码不发给后端：那是纯前端的一致性检查
    const res = await fetch('/api/v1/auth/register', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        username: form.username.trim(),
        password: form.password,
      }),
    })

    const result = await res.json()

    if (result.code !== 0) {
      errorMsg.value = result.msg || '注册失败'
      return
    }

    // 后端「注册即登录」，返回结构与 /auth/login 完全一致，
    // 所以这里的落盘逻辑和 LoginView 保持同一套 key。
    localStorage.setItem('user', JSON.stringify(result.data))
    localStorage.setItem('token', result.data.access_token)
    localStorage.setItem('token_type', result.data.token_type)
    localStorage.setItem('login_at', String(Date.now()))
    localStorage.setItem('expires_in', String(result.data.expires_in))

    // 注册当作一次未勾选「记住我」的登录：关闭浏览器后需重新登录。
    localStorage.setItem('remember', '0')
    sessionStorage.setItem('session_alive', '1')

    router.push('/rooms')
  } catch {
    errorMsg.value = '无法连接服务器'
  } finally {
    loading.value = false
  }
}
</script>

<template>
  <main class="register-page">
    <section class="brand-panel">
      <div class="brand-content">
        <div class="logo">
          <span class="logo-icon">▶</span>
          <span>Time-Live Stream</span>
        </div>

        <div class="brand-introduction">
          <p class="eyebrow">LIVE STREAMING PLATFORM</p>
          <h1>开启你的直播</h1>
          <p class="description">
            创建账号，开设属于自己的直播间，
            <br />
            让每一刻精彩都被看见。
          </p>
        </div>

        <div class="danmaku-preview" aria-hidden="true">
          <span class="danmaku danmaku-one">欢迎新主播！</span>
          <span class="danmaku danmaku-two">冲鸭</span>
          <span class="danmaku danmaku-three">已关注</span>
          <span class="danmaku danmaku-four">第一次来</span>
        </div>

        <p class="brand-footer">实时 · 互动 · 分享</p>
      </div>
    </section>

    <section class="form-panel">
      <div class="register-container">
        <div class="mobile-logo">
          <span class="logo-icon">▶</span>
          <span>Time-Live stream</span>
        </div>

        <header class="form-header">
          <h2>创建账号</h2>
          <p>注册成功后将自动登录</p>
        </header>

        <form class="register-form" @submit.prevent="handleRegister">
          <label class="form-field">
            <span class="field-label">用户名</span>
            <input
              v-model="form.username"
              type="text"
              autocomplete="username"
              placeholder="3-32 位字母、数字或下划线"
              :disabled="loading"
            />
          </label>

          <label class="form-field">
            <span class="field-label">密码</span>

            <div class="password-field">
              <input
                v-model="form.password"
                :type="showPassword ? 'text' : 'password'"
                autocomplete="new-password"
                placeholder="6-32 位"
                :disabled="loading"
              />

              <button
                class="password-toggle"
                type="button"
                :aria-label="showPassword ? '隐藏密码' : '显示密码'"
                @click="showPassword = !showPassword"
              >
                {{ showPassword ? '隐藏' : '显示' }}
              </button>
            </div>
          </label>

          <label class="form-field">
            <span class="field-label">确认密码</span>
            <input
              v-model="form.confirmPassword"
              :type="showPassword ? 'text' : 'password'"
              autocomplete="new-password"
              placeholder="请再次输入密码"
              :disabled="loading"
            />
          </label>

          <p v-if="errorMsg" class="error-message">
            {{ errorMsg }}
          </p>

          <button class="register-button" type="submit" :disabled="loading">
            <span v-if="loading" class="loading-spinner"></span>
            {{ loading ? '注册中...' : '注册' }}
          </button>
        </form>

        <p class="login-entry">
          已有账号？
          <button class="text-button" type="button" @click="router.push('/login')">
            返回登录
          </button>
        </p>
      </div>
    </section>
  </main>
</template>

<style scoped>
* {
  box-sizing: border-box;
}

button,
input {
  font: inherit;
}

button {
  border: 0;
}

.register-page {
  width: 100%;
  min-height: 100vh;
  display: grid;
  grid-template-columns: 56% 44%;
  background: #ffffff;
}

.brand-panel {
  position: relative;
  overflow: hidden;
  color: #ffffff;
  background:
    radial-gradient(circle at 20% 20%, rgba(94, 129, 255, 0.5), transparent 34%),
    radial-gradient(circle at 80% 75%, rgba(191, 89, 255, 0.36), transparent 34%),
    linear-gradient(145deg, #121733 0%, #20285f 48%, #38236b 100%);
}

.brand-panel::before,
.brand-panel::after {
  position: absolute;
  content: '';
  border-radius: 50%;
  filter: blur(2px);
}

.brand-panel::before {
  width: 360px;
  height: 360px;
  top: -150px;
  right: -100px;
  background: rgba(113, 143, 255, 0.18);
}

.brand-panel::after {
  width: 280px;
  height: 280px;
  left: -100px;
  bottom: -100px;
  background: rgba(205, 99, 255, 0.14);
}

.brand-content {
  position: relative;
  z-index: 1;
  width: min(680px, 100%);
  min-height: 100vh;
  margin: 0 auto;
  padding: 54px 70px;
  display: flex;
  flex-direction: column;
}

.logo,
.mobile-logo {
  display: flex;
  align-items: center;
  gap: 12px;
  font-size: 22px;
  font-weight: 700;
}

.logo-icon {
  width: 38px;
  height: 38px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding-left: 2px;
  color: #ffffff;
  font-size: 15px;
  border-radius: 12px;
  background: linear-gradient(135deg, #6d8cff, #a855f7);
  box-shadow: 0 10px 30px rgba(98, 97, 255, 0.35);
}

.brand-introduction {
  margin: auto 0;
}

.eyebrow {
  margin: 0 0 22px;
  color: rgba(255, 255, 255, 0.56);
  font-size: 13px;
  font-weight: 700;
  letter-spacing: 3px;
}

.brand-introduction h1 {
  margin: 0;
  font-size: clamp(46px, 5vw, 72px);
  line-height: 1.13;
  letter-spacing: -3px;
}

.description {
  margin: 30px 0 0;
  color: rgba(255, 255, 255, 0.7);
  font-size: 18px;
  line-height: 1.9;
}

.brand-footer {
  margin: 0;
  color: rgba(255, 255, 255, 0.48);
  font-size: 14px;
  letter-spacing: 5px;
}

.danmaku-preview {
  position: absolute;
  inset: 0;
  pointer-events: none;
}

.danmaku {
  position: absolute;
  padding: 10px 17px;
  color: rgba(255, 255, 255, 0.88);
  font-size: 13px;
  border: 1px solid rgba(255, 255, 255, 0.13);
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.08);
  backdrop-filter: blur(10px);
}

.danmaku-one {
  top: 22%;
  right: 8%;
}

.danmaku-two {
  top: 38%;
  left: 8%;
}

.danmaku-three {
  right: 15%;
  bottom: 27%;
}

.danmaku-four {
  left: 15%;
  bottom: 18%;
}

.form-panel {
  width: 100%;
  min-height: 100vh;
  padding: 48px 64px;

  display: flex;
  align-items: center;
  justify-content: center;

  background: #ffffff;
}

.register-container {
  width: 100%;
  max-width: 420px;
}

.mobile-logo {
  display: none;
  margin-bottom: 44px;
  color: #252b45;
}

.form-header {
  margin-bottom: 38px;
}

.form-header h2 {
  margin: 0 0 12px;
  color: #20243a;
  font-size: 36px;
  letter-spacing: -1px;
}

.form-header p {
  margin: 0;
  color: #8a8fa5;
  font-size: 15px;
}

.register-form {
  display: flex;
  flex-direction: column;
  gap: 22px;
}

.form-field {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.field-label {
  color: #3e435a;
  font-size: 14px;
  font-weight: 600;
}

.form-field input {
  width: 100%;
  height: 50px;
  padding: 0 16px;
  color: #25293c;
  border: 1px solid #e1e4ed;
  border-radius: 12px;
  outline: none;
  background: #fafbfc;
  transition:
    border-color 0.2s,
    box-shadow 0.2s,
    background 0.2s;
}

.form-field input::placeholder {
  color: #b2b5c3;
}

.form-field input:focus {
  border-color: #6f78eb;
  background: #ffffff;
  box-shadow: 0 0 0 4px rgba(111, 120, 235, 0.11);
}

.form-field input:disabled {
  cursor: not-allowed;
  opacity: 0.65;
}

.password-field {
  position: relative;
}

.password-field input {
  padding-right: 64px;
}

.password-toggle {
  position: absolute;
  top: 50%;
  right: 14px;
  transform: translateY(-50%);
  padding: 5px;
  color: #777d95;
  font-size: 13px;
  cursor: pointer;
  background: transparent;
}

.text-button {
  padding: 0;
  color: #646ee4;
  font-size: 13px;
  cursor: pointer;
  background: transparent;
}

.text-button:hover {
  color: #4d56ca;
}

.error-message {
  margin: -7px 0 0;
  padding: 11px 13px;
  color: #d84646;
  font-size: 13px;
  border-radius: 9px;
  background: #fff1f1;
}

.register-button {
  height: 52px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  color: #ffffff;
  font-weight: 700;
  border-radius: 12px;
  cursor: pointer;
  background: linear-gradient(135deg, #6676e8, #8c5de7);
  box-shadow: 0 14px 28px rgba(104, 98, 221, 0.23);
  transition:
    transform 0.2s,
    box-shadow 0.2s;
}

.register-button:hover:not(:disabled) {
  transform: translateY(-1px);
  box-shadow: 0 18px 34px rgba(104, 98, 221, 0.3);
}

.register-button:disabled {
  cursor: not-allowed;
  opacity: 0.7;
}

.loading-spinner {
  width: 17px;
  height: 17px;
  border: 2px solid rgba(255, 255, 255, 0.45);
  border-top-color: #ffffff;
  border-radius: 50%;
  animation: spin 0.7s linear infinite;
}

.login-entry {
  margin: 27px 0 0;
  color: #8a8fa3;
  font-size: 14px;
  text-align: center;
}

@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

@media (max-width: 900px) {
  .register-page {
    grid-template-columns: 1fr;
  }

  .brand-panel {
    display: none;
  }

  .form-panel {
    padding: 32px 24px;
    background:
      radial-gradient(circle at top left, rgba(114, 128, 238, 0.1), transparent 35%),
      #ffffff;
  }

  .mobile-logo {
    display: flex;
  }
}

@media (max-width: 480px) {
  .form-panel {
    align-items: flex-start;
    padding-top: 48px;
  }

  .form-header h2 {
    font-size: 31px;
  }
}
</style>