<script setup lang="ts">
import { ref } from 'vue'

// disabled 由父组件传入。
// 当 WebSocket 没有连接成功时，输入框和按钮都应该禁用。
defineProps<{
  disabled: boolean
}>()

// 子组件不直接操作 WebSocket。
// 它只把用户输入的弹幕内容交给父组件，由父组件决定怎么发送。
const emit = defineEmits<{
  send: [content: string]
}>()

const input = ref('')

function submitMessage() {
  const content = input.value.trim()

  if (!content) return

  emit('send', content)
  input.value = ''
}
</script>

<template>
  <form class="message-form" @submit.prevent="submitMessage">
    <input
      v-model="input"
      type="text"
      placeholder="发送一条弹幕"
      :disabled="disabled"
    />

    <button type="submit" :disabled="disabled">
      发送
    </button>
  </form>
</template>

<style scoped>
.message-form {
  margin-top: 12px;
  display: flex;
  gap: 10px;
  flex-shrink: 0;
}

.message-form input {
  flex: 1;
  height: 42px;
  padding: 0 12px;
  border: 1px solid #d9deea;
  border-radius: 10px;
  outline: none;
}

.message-form button {
  width: 86px;
  color: #ffffff;
  border: 0;
  border-radius: 10px;
  cursor: pointer;
  background: #6874e8;
}

.message-form button:disabled,
.message-form input:disabled {
  cursor: not-allowed;
  opacity: 0.6;
}
</style>
