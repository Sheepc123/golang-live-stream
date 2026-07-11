export type LiveSocketStatus = 'disconnected' | 'connecting' | 'connected'

export interface WSMessage {
  type:
    | 'join'
    | 'leave'
    | 'chat'
    | 'like'
    | 'like_count'
    | 'heartbeat'
    | 'online_count'
    | 'system'
    | 'error'
  room_id: number
  user_id: number
  username: string
  content: string
  timestamp: number
}

// 前端只允许主动发送聊天和点赞消息。
// join、leave、online_count、like_count 等状态消息由后端生成。
type ClientActionMessage =
  | {
      type: 'chat'
      content: string
    }
  | {
      type: 'like'
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

export class LiveSocket {
  private socket: WebSocket | null = null

  // ===== 重连相关状态 =====
  // 区分「用户主动关闭」和「意外断开」：主动关不重连，意外断才重连。
  private manualClose = false
  // 重连定时器句柄，方便取消。
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  // 当前重连等待时间（毫秒），会指数增长。
  private reconnectDelay = 1000
  // 重连间隔上限，封顶 30 秒。
  private readonly maxReconnectDelay = 30000

  constructor(private readonly options: LiveSocketOptions) {}

  connect() {
    // 每次连接前清掉可能存在的重连定时器，避免重复连。
    this.clearReconnectTimer()

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
      // 连接成功后把重连延迟重置回 1 秒。
      this.reconnectDelay = 1000
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
      // onerror 后通常紧接着 onclose，重连逻辑统一放 onclose。
      this.options.onStatusChange('disconnected')
    }

    socket.onclose = () => {
      this.socket = null
      this.options.onStatusChange('disconnected')
      // 只有「不是主动关的」才自动重连。
      if (!this.manualClose) {
        this.scheduleReconnect()
      }
    }
  }

  // 安排一次「延迟后重连」，并把延迟指数增大。
  private scheduleReconnect() {
    this.clearReconnectTimer()
    this.reconnectTimer = setTimeout(() => {
      this.options.onStatusChange('connecting')
      this.connect()
    }, this.reconnectDelay)
    // 指数退避：下次翻倍，但不超过上限。
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.maxReconnectDelay)
  }

  private clearReconnectTimer() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
  }

  sendChat(content: string) {
    const normalizedContent = content.trim()
    if (!normalizedContent) return false

    return this.send({
      type: 'chat',
      content: normalizedContent,
    })
  }

  // 前端只发送点赞动作，用户身份、房间和总点赞数由后端处理。
  sendLike() {
    return this.send({ type: 'like' })
  }

  private send(message: ClientActionMessage) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      return false
    }

    this.socket.send(JSON.stringify(message))
    return true
  }

  close() {
    // 主动关闭时打标记，onclose 里就不会重连。
    this.manualClose = true
    this.clearReconnectTimer()
    if (!this.socket) return
    this.socket.close()
    this.socket = null
  }

  private buildURL() {
    const token = getAccessToken()
    if (!token) return ''
    const params = new URLSearchParams({ token })
    return `ws://localhost:8080/api/v1/ws/rooms/${this.options.roomID}?${params.toString()}`
  }
}
