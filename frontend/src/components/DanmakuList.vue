<script setup lang="ts">
import { nextTick, ref, watch } from 'vue'
import type { WSMessage } from '@/utils/liveSocket'

const props = defineProps<{
  messages: WSMessage[]
}>()

const messageListRef = ref<HTMLElement | null>(null)

function scrollMessageToBottom() {
  nextTick(() => {
    if (!messageListRef.value) return

    messageListRef.value.scrollTop = messageListRef.value.scrollHeight
  })
}

function formatMessageContent(msg: WSMessage) {
  switch (msg.type) {
    case 'join':
      return `${msg.username || '匿名用户'} 进入了直播间`
    case 'leave':
      return `${msg.username || '匿名用户'} 离开了直播间`
    case 'like':
      // 点赞用户身份来自后端验证后的 WebSocket Client。
      // 不直接依赖后端的英文 Content，前端统一展示中文文案。
      return `${msg.username || '匿名用户'} 点了一个赞`
    case 'like_count':
      return `当前点赞数：${msg.content}`
    case 'system':
      return msg.content || '系统消息'
    case 'error':
      return msg.content || '连接出现异常'
    default:
      return msg.content
  }
}

function formatMessageType(type: WSMessage['type']) {
  const typeTextMap: Record<WSMessage['type'], string> = {
    join: '进入',
    leave: '离开',
    chat: '弹幕',
    like: '点赞',
    like_count: '赞数',
    heartbeat: '心跳',
    online_count: '人数',
    system: '系统',
    error: '错误',
  }

  return typeTextMap[type]
}

// messages 增加时，自动滚动到最新弹幕。
// 父组件只负责维护消息数组，滚动这种展示细节交给列表组件。
watch(
  () => props.messages.length,
  () => {
    scrollMessageToBottom()
  },
)
</script>

<template>
  <div ref="messageListRef" class="message-list">
    <p v-if="messages.length === 0" class="empty-message">
      暂无弹幕
    </p>

    <div
      v-for="(msg, index) in messages"
      :key="`${msg.timestamp}-${msg.user_id}-${msg.type}-${index}`"
      :class="['message-item', `message-${msg.type}`]"
    >
      <span class="message-type">{{ formatMessageType(msg.type) }}</span>

      <template v-if="msg.type === 'chat'">
        <span class="message-user">{{ msg.username || '匿名用户' }}：</span>
        <span class="message-content">{{ msg.content }}</span>
      </template>

      <span v-else class="message-content">
        {{ formatMessageContent(msg) }}
      </span>
    </div>
  </div>
</template>

<style scoped>
.message-list {
  flex: 1;
  min-height: 0;
  overflow-y: auto;
  padding: 12px;
  border: 1px solid #e5e8f0;
  border-radius: 10px;
  background: #f8fafc;
}

.empty-message {
  margin: 0;
  color: #8a90a3;
}

.message-item {
  margin-bottom: 10px;
  color: #252a42;
  font-size: 14px;
  line-height: 1.6;
}

.message-type {
  margin-right: 8px;
  padding: 2px 6px;
  color: #6874e8;
  font-size: 12px;
  border-radius: 999px;
  background: rgba(104, 116, 232, 0.1);
}

.message-user {
  color: #6874e8;
  font-weight: 700;
}

.message-content {
  word-break: break-word;
}

.message-join,
.message-leave,
.message-system {
  color: #7d8498;
}

.message-join .message-type {
  color: #16a34a;
  background: rgba(22, 163, 74, 0.1);
}

.message-leave .message-type {
  color: #8a90a3;
  background: rgba(138, 144, 163, 0.14);
}

.message-system .message-type {
  color: #2563eb;
  background: rgba(37, 99, 235, 0.1);
}

.message-like {
  color: #d6336c;
}

.message-like .message-type {
  color: #d6336c;
  background: rgba(214, 51, 108, 0.1);
}

.message-error {
  color: #d84646;
}

.message-error .message-type {
  color: #d84646;
  background: #fff1f1;
}

@media (max-width: 900px) {
  .message-list {
    height: 320px;
    flex: none;
  }
}

@media (max-width: 640px) {
  .message-list {
    height: 260px;
  }
}
</style>
