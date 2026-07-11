<script setup lang="ts">
import { computed, onUnmounted, ref } from 'vue'

// 一条「飘屏弹幕」的数据结构。
// 注意：它和侧边栏列表用的 WSMessage 不一样，
// 这里只关心「飘屏动画」需要的最少字段。
interface FlyingDanmaku {
  id: number // 唯一 key，给 v-for 用，保证每条弹幕独立
  text: string // 弹幕文字内容
  track: number // 轨道编号（第几行），避免所有弹幕挤在同一行
  color: string // 文字颜色，随机挑一个，画面更活泼
  duration: number // 飘过整个屏幕用的秒数，越大飘得越慢
}

// 父组件传进来的两个开关：
// - paused：视频是否暂停。暂停时让弹幕动画一起冻结。
// - fontSize：弹幕字号（像素）。父组件用「小/中/大」按钮切换。
const props = withDefaults(
  defineProps<{
    paused?: boolean
    fontSize?: number
  }>(),
  {
    paused: false,
    fontSize: 20,
  },
)

// 屏幕从上往下分成几条轨道（几行）。
// 只用上半部分，底部留给视频自带的控制条。
const TRACK_COUNT = 5
// 每条轨道距顶部的间距（像素），track * 这个值 = 该行的 top。
const TRACK_HEIGHT = 44
// 弹幕飘过整屏的基础时长（秒）。
const BASE_DURATION = 8

// 当前正在屏幕上飞的弹幕列表。
// 飘完的弹幕会被移除，所以这里只保留「正在飞的」。
const items = ref<FlyingDanmaku[]>([])

// 自增 id，保证每条弹幕的 key 唯一（即使文字相同）。
let seed = 0

// 记录每条轨道「最后一次放弹幕的时间戳」（performance.now() 毫秒）。
// 选轨道时优先挑最久没用的那条，从而错开、避免追尾。
// 初始为 0，表示所有轨道一开始都空闲。
const trackLastTime: number[] = new Array(TRACK_COUNT).fill(0)

// 一组明快的颜色，随机取一个。视频背景通常偏暗，用亮色更清楚。
const COLORS = ['#ffffff', '#ffd93d', '#6bcb77', '#4d96ff', '#ff6b6b']
function pickColor(): string {
  // ?? 兜底同 pickTrack：noUncheckedIndexedAccess 下数组下标类型是
  // string | undefined，用 ?? 收敛成 string。索引一定在范围内，不会真取到 undefined。
  return COLORS[Math.floor(Math.random() * COLORS.length)] ?? '#ffffff'
}

// 选择一条最合适的轨道，用来放下一条弹幕。
// 策略：挑「最久没用过」的轨道（trackLastTime 最小的那条）。
// 这样弹幕会尽量均匀分散到各行，天然错开、减少追尾。
function pickTrack(now: number): number {
  let bestTrack = 0
  // ?? 0 是为了兼容 TS 的 noUncheckedIndexedAccess：
  // 数组下标访问的类型是 number | undefined，用 ?? 兜底成 number。
  // 实际上 i 一定在 0..TRACK_COUNT-1 范围内，不会真的取到 undefined。
  let oldest = trackLastTime[0] ?? 0

  for (let i = 1; i < TRACK_COUNT; i++) {
    const last = trackLastTime[i] ?? 0
    if (last < oldest) {
      oldest = last
      bestTrack = i
    }
  }

  // 记下这条轨道最新的使用时间，供下次比较。
  trackLastTime[bestTrack] = now
  return bestTrack
}

// push 是这个组件对外暴露的唯一方法。
// 父组件每收到一条聊天弹幕，就调用一次 push，让它飘起来。
function push(text: string) {
  const content = text.trim()
  if (!content) return

  const now = performance.now()
  const track = pickTrack(now)

  items.value.push({
    id: seed++,
    text: content,
    track,
    color: pickColor(),
    duration: BASE_DURATION,
  })
}

// 动画结束回调：弹幕飘出屏幕左侧后，CSS 动画结束会触发 animationend。
// 这时把它从数组里删掉，否则 DOM 元素会无限堆积、拖慢页面。
function handleAnimationEnd(id: number) {
  items.value = items.value.filter((item) => item.id !== id)
}

// 组件卸载（离开直播间）时清空，避免残留。
onUnmounted(() => {
  items.value = []
})

// 弹幕层根节点的动态样式：
// - --danmaku-font-size：CSS 变量，控制所有弹幕字号，字号切换即时生效
const screenStyle = computed(() => ({
  '--danmaku-font-size': props.fontSize + 'px',
}))

// defineExpose：把 push 方法暴露出去，
// 这样父组件可以通过 ref 拿到这个组件实例并调用 push。
defineExpose({ push })
</script>

<template>
  <!--
    弹幕层：绝对定位铺满整个视频容器。
    - overflow: hidden 让飘出去的弹幕被裁掉，不影响布局
    - pointer-events: none 关键！让鼠标点击「穿透」到下面的视频，
      否则弹幕会挡住视频的播放/暂停操作
    - :class 里的 is-paused 用来冻结动画（见下方样式）
  -->
  <div
    class="danmaku-screen"
    :class="{ 'is-paused': paused }"
    :style="screenStyle"
  >
    <span
      v-for="item in items"
      :key="item.id"
      class="danmaku-item"
      :style="{
        // 根据轨道算出这一行的垂直位置
        top: item.track * TRACK_HEIGHT + 8 + 'px',
        color: item.color,
        // 用内联样式控制每条弹幕的动画时长
        animationDuration: item.duration + 's',
      }"
      @animationend="handleAnimationEnd(item.id)"
    >
      {{ item.text }}
    </span>
  </div>
</template>

<style scoped>
.danmaku-screen {
  position: absolute;
  inset: 0; /* top/right/bottom/left 全为 0，铺满父容器 */
  overflow: hidden;
  pointer-events: none;
  /*
   * container-type: inline-size 是关键。
   * 它让子元素可以用 cqw 单位感知「这个容器有多宽」，
   * 下面的动画用 100cqw 表示「容器的整个宽度」。
   */
  container-type: inline-size;
}

/*
 * 视频暂停时：让弹幕层里所有正在跑的动画一起冻结。
 * animation-play-state: paused 会「原地停住」CSS 动画，
 * 恢复 running 后从停住的位置继续，不会跳帧。
 */
.danmaku-screen.is-paused .danmaku-item {
  animation-play-state: paused;
}

.danmaku-item {
  position: absolute;
  left: 100%; /* 初始位置：紧贴容器右边缘之外，一开始是看不见的 */
  white-space: nowrap; /* 弹幕文字不换行 */
  /* 字号读取父层的 CSS 变量，父组件切换字号时即时生效 */
  font-size: var(--danmaku-font-size, 20px);
  font-weight: 700;
  /* 黑色描边阴影，让浅色文字在明亮视频画面上也看得清 */
  text-shadow:
    0 1px 2px rgba(0, 0, 0, 0.9),
    0 0 3px rgba(0, 0, 0, 0.7);
  will-change: transform; /* 提示浏览器用 GPU 加速这个 transform 动画 */
  animation-name: danmaku-fly;
  animation-timing-function: linear; /* 匀速飘过 */
  animation-fill-mode: forwards; /* 动画结束后停在最后一帧 */
}

/*
 * 飘屏动画：从右往左匀速移动。
 * - 起点 translateX(0)：配合 left:100%，元素在容器右边缘外
 * - 终点 translateX(calc(-100cqw - 100%))：
 *     -100cqw 表示向左移动「一个容器宽度」，
 *     -100%   表示再向左移动「弹幕自身宽度」，
 *   两者相加，保证弹幕完整地从右边飘到完全离开左边。
 */
@keyframes danmaku-fly {
  from {
    transform: translateX(0);
  }
  to {
    transform: translateX(calc(-100cqw - 100%));
  }
}
</style>
