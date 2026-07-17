import { createRouter, createWebHistory } from 'vue-router'

import HomeView from '../views/HomeView.vue'
import LoginView from '../views/LoginView.vue'
import RoomDetailView from '@/views/RoomDetailView.vue'
import MyRoomsView from '@/views/MyRoomsView.vue'
import RoomConsoleView from '@/views/RoomConsoleView.vue'


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
      // 我的直播间管理页
      path: '/my/rooms',
      name: 'my-rooms',
      component: MyRoomsView,
      meta: {
        requiresAuth: true, // 需要登录，未登录会被守卫重定向到 /login
      },
    },

    {
      // 主播直播控制台：只有房主本人能操作，进来可看画面/弹幕 + 开播下播等控制
      // 归属校验放在页面内部（比对当前 user_id 与 room.owner_id），非房主会被挡回
      path: '/my/rooms/:room_id/console',
      name: 'room-console',
      component: RoomConsoleView,
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
  localStorage.removeItem('remember')
  sessionStorage.removeItem('session_alive')
}

function hasValidToken() {
  const token = localStorage.getItem('token')
  const expiresIn = Number(localStorage.getItem("expires_in"))
  const loginAt = Number(localStorage.getItem("login_at"))


  if (!token || !expiresIn || !loginAt) return false

  // 「记住我」未勾选时，登录只在当前浏览器会话内有效。
  // 依据 sessionStorage 的哨兵：浏览器关闭后 sessionStorage 被清空，
  // 哨兵消失即视为会话结束，需要重新登录。
  const remember = localStorage.getItem('remember') === '1'
  if (!remember && !sessionStorage.getItem('session_alive')) {
    return false
  }

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
