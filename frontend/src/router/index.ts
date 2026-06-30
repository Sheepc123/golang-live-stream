import { createRouter, createWebHistory } from 'vue-router'

import HomeView from '../views/HomeView.vue'
import LoginView from '../views/LoginView.vue'
import RoomDetailView from '@/views/RoomDetailView.vue'

const router = createRouter({
  // 使用浏览器正常的路由地址，例如 /login、/home
  history: createWebHistory(import.meta.env.BASE_URL),

  routes: [
    {
      // 访问根路径时，默认进入登录页
      path: '/',
      redirect: '/login',
    },
    {
      // 登录页面
      path: '/login',
      name: 'login',
      component: LoginView,
    },
    {
      // 兼容旧地址：以后主要使用 /rooms
      path: '/home',
      redirect: '/rooms',
    },

    {
      // 直播间宫格列表页
      // 这里暂时继续复用 HomeView.vue，不改文件名
      path: '/rooms',
      name: 'rooms',
      component: HomeView,
      meta: {
        requiresAuth: true,
      },
    },

    {
      // 单个直播间详情页。
      // room_id 会传给 RoomDetailView.vue。
      path: '/rooms/:room_id',
      name: 'room-detail',
      component: RoomDetailView,
      meta: {
        requiresAuth: true,
      },
    },

    {
      // 用户访问不存在的页面时，返回登录页
      path: '/:pathMatch(.*)*',
      redirect: '/login',
    },
  ],
})

function clearAuthStorage() {
  localStorage.removeItem('token')
  localStorage.removeItem('token_type')
  localStorage.removeItem('expires_in')
  localStorage.removeItem('login_at')
  localStorage.removeItem('user')
}

function hasValidToken() {
  const token = localStorage.getItem('token')
  const expiresIn = Number(localStorage.getItem("expires_in"))
  const loginAt = Number(localStorage.getItem("login_at"))


  if (!token || !expiresIn || !loginAt) return false

  const expiresAt = loginAt + expiresIn * 1000

  return Date.now() < expiresAt
}


router.beforeEach((to)=> {

  const ok = hasValidToken()

  if(!ok) {
    clearAuthStorage()
  }

  if(to.meta.requiresAuth && !ok) {
    return '/login'
  }

  if(to.path == '/login' && ok) {
    return '/rooms'
  }
  return true
})


export default router
