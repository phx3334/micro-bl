<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  current: number
  total: number
  pageSize?: number
}>()

const emit = defineEmits<{
  (e: 'change', page: number): void
}>()

const pageSize = computed(() => props.pageSize || 20)
// 必须是 computed：total 由接口异步返回，写成普通常量会停留在挂载时的旧值，
// 导致分页总数不更新（页码不出现 / 翻页边界错误）
const totalPages = computed(() => Math.ceil(props.total / pageSize.value))

function goPage(page: number) {
  if (page < 1 || page > totalPages.value || page === props.current) return
  emit('change', page)
}
</script>

<template>
  <div v-if="totalPages > 1" class="flex items-center justify-center gap-2 py-4">
    <button
      class="px-3 h-8 rounded text-sm hover:bg-surface-subtle transition disabled:opacity-40 disabled:cursor-not-allowed"
      :disabled="current <= 1"
      @click="goPage(current - 1)"
    >
      上一页
    </button>
    <template v-for="p in totalPages" :key="p">
      <button
        v-if="p === 1 || p === totalPages || (p >= current - 2 && p <= current + 2)"
        class="w-8 h-8 rounded text-sm transition"
        :class="p === current ? 'bg-primary text-white' : 'hover:bg-surface-subtle'"
        @click="goPage(p)"
      >
        {{ p }}
      </button>
      <span
        v-else-if="p === current - 3 || p === current + 3"
        class="text-ink-muted"
      >...</span>
    </template>
    <button
      class="px-3 h-8 rounded text-sm hover:bg-surface-subtle transition disabled:opacity-40 disabled:cursor-not-allowed"
      :disabled="current >= totalPages"
      @click="goPage(current + 1)"
    >
      下一页
    </button>
  </div>
</template>
