export type LiveSocketStatus = 'disconnected' | 'connecting' | 'connected'

export interface WSMessage {
  type: 'join' | 'leave' | 'chat' | 'heartbeat' | 'online_count' | 'system' | 'error'
  room_id: number
  user_id: number
  username: string
  content: string
  timestamp: number
}

interface LiveSocketOptions {
  roomID: string
  onStatusChange: (status: LiveSocketStatus) => void
  onMessage: (message: WSMessage) => void
  onInvalidMessage: () => void
}

function getAccessToken() {
  return localStorage.getItem('token')
}

// LiveSocket 只负责 WebSocket 连接本身。
// 页面组件负责：保存消息、展示 UI、决定什么时候连接和关闭。
export class LiveSocket {
  private socket: WebSocket | null = null

  constructor(private readonly options: LiveSocketOptions) {}

  connect() {
    if (this.socket) return

    const url = this.buildURL()
    if (!url) {
      this.options.onStatusChange('disconnected')
      return
    }

    this.options.onStatusChange('connecting')

    const socket = new WebSocket(url)
    this.socket = socket

    socket.onopen = () => {
      this.options.onStatusChange('connected')
    }

    socket.onmessage = (event) => {
      let message: WSMessage

      try {
        message = JSON.parse(event.data) as WSMessage
      } catch {
        this.options.onInvalidMessage()
        return
      }

      this.options.onMessage(message)
    }

    socket.onerror = () => {
      this.options.onStatusChange('disconnected')
    }

    socket.onclose = () => {
      this.socket = null
      this.options.onStatusChange('disconnected')
    }
  }

  sendChat(content: string) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return

    // 当前阶段后端只需要 content。
    // room_id、user_id、username 由后端连接信息统一补充。
    this.socket.send(
      JSON.stringify({
        content,
      }),
    )
  }

  close() {
    if (!this.socket) return

    this.socket.close()
    this.socket = null
  }

  private buildURL() {
    const token = getAccessToken()

    if (!token) return ''

    // WebSocket 不能像 fetch 一样方便地设置 Authorization header。
    // 当前阶段采用方案一：把 JWT token 放到 URL query 中。
    const params = new URLSearchParams({
      token,
    })

    return `ws://localhost:8080/api/v1/ws/rooms/${this.options.roomID}?${params.toString()}`
  }
}
