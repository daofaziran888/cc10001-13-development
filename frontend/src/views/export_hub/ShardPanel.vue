<template>
  <section class="shard-panel" data-testid="shard-panel">
    <header class="page-head shard-head">
      <div>
        <h3>分片分发进度</h3>
        <p class="page-desc">
          按批次与通道查看每个分片的完成位置；中断后只补传失败分片，所有通道完成前不会暴露半成品。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="shard-export" type="button" @click="exportShards">导出分片清单</button>
      </div>
    </header>

    <form class="filter-bar shard-toolbar" @submit.prevent="loadShards">
      <label class="filter-item">
        <span>结果批次ID</span>
        <input v-model="batchId" data-testid="shard-batch-id" placeholder="如 3" />
      </label>
      <label class="filter-item">
        <span>分发通道</span>
        <select v-model="channelId" data-testid="shard-channel">
          <option value="">全部通道</option>
          <option v-for="channel in channels" :key="String(channel.id)" :value="String(channel.id)">
            {{ channel['通道编号'] }}
          </option>
        </select>
      </label>
      <label class="filter-item">
        <span>分片状态</span>
        <select v-model="statusFilter" data-testid="shard-status">
          <option value="">全部状态</option>
          <option v-for="status in shardStatuses" :key="status" :value="status">{{ status }}</option>
        </select>
      </label>
      <button class="btn" data-testid="shard-load" type="submit">读取分片</button>
      <button class="btn primary" data-testid="shard-prepare" type="button" :disabled="busy" @click="prepare">
        生成分片
      </button>
    </form>

    <div class="shard-summary">
      <article class="stat-card">
        <span class="stat-label">批次分发状态</span>
        <strong class="stat-value" data-testid="shard-batch-status">{{ batchState?.status || '未生成分片' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">分片完成进度</span>
        <strong class="stat-value" data-testid="shard-progress">
          {{ batchState ? `${batchState.completed_shards}/${batchState.total_shards}` : '—' }}
        </strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">续传起点</span>
        <strong class="stat-value" data-testid="shard-resume-from">
          {{ batchState?.resume_from ? `第 ${batchState.resume_from} 片` : '无需续传' }}
        </strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">部分成功冲突</span>
        <strong class="stat-value" data-testid="shard-partial">
          {{ batchState?.partial_success ? '是' : '否' }}
        </strong>
      </article>
    </div>

    <div v-if="channelStates.length" class="shard-channels">
      <span
        v-for="info in channelStates"
        :key="String(info.channel_id)"
        class="shard-chip"
        :data-testid="`shard-channel-${info.channel_id}`"
      >
        {{ info['通道编号'] }}：{{ info.status }}（{{ info.completed }}/{{ info.total }}）
        <template v-if="info.resume_from"> · 续传自第 {{ info.resume_from }} 片</template>
      </span>
    </div>

    <div class="shard-toolbar">
      <label class="filter-item">
        <span>部分成功策略</span>
        <select v-model="strategy" data-testid="shard-strategy">
          <option value="publish_success">立即发布成功通道</option>
          <option value="stage_until_all">暂存至全部通道完成</option>
        </select>
      </label>
      <button class="btn" data-testid="shard-resume" type="button" :disabled="busy" @click="resume">
        续传待传分片
      </button>
      <button class="btn primary" data-testid="shard-package" type="button" :disabled="busy" @click="packageFiles">
        打包完整文件
      </button>
      <span class="shard-hint">已完成分片不会被重新发送</span>
      <span v-if="errorMessage" class="error-text" data-testid="shard-error">{{ errorMessage }}</span>
    </div>

    <table class="data-table">
      <thead>
        <tr>
          <th>批次编号</th>
          <th>通道编号</th>
          <th>分片编号</th>
          <th>目标地址</th>
          <th>分片状态</th>
          <th>尝试次数</th>
          <th>完成时间</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="row in shardRows"
          :key="String(row.id)"
          :data-testid="`shard-row-${row.id}`"
          :data-status="String(row['分片状态'])"
        >
          <td>{{ row['批次编号'] ?? '—' }}</td>
          <td>{{ row['通道编号'] ?? '—' }}</td>
          <td>{{ row['分片编号'] ?? '—' }}</td>
          <td>{{ row['目标地址'] ?? '—' }}</td>
          <td>{{ row['分片状态'] ?? '—' }}</td>
          <td>{{ row['尝试次数'] ?? 0 }}</td>
          <td>{{ row['完成时间'] || '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in shardActions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`shard-action-${row.id}-${action}`"
              @click="runShardAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!shardRows.length">
          <td colspan="8" class="empty-state">暂无分片分发记录，输入已校验批次后生成分片</td>
        </tr>
      </tbody>
    </table>

    <div class="shard-files">
      <h4>分发文件</h4>
      <ul>
        <li v-for="file in files" :key="String(file.id)" :data-testid="`shard-file-${file.id}`">
          <span>{{ file['批次编号'] }} · {{ file['通道编号'] }} · {{ file.filename }}</span>
          <span class="shard-file-status" :data-status="String(file.status)">{{ file.status }}</span>
          <a
            v-if="file.downloadable"
            :href="String(file.download_url ?? '#')"
            class="link"
            :data-testid="`shard-download-${file.id}`"
            download
          >下载</a>
          <span v-else class="shard-hint">通道未完成，暂不提供下载</span>
        </li>
        <li v-if="!files.length" class="empty-state">暂无分发文件，通道全部分片完成后可打包</li>
      </ul>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

const emit = defineEmits<{ (event: 'error', message: string): void }>()

type Row = Record<string, string | number | boolean | null>
type BatchState = {
  status?: string
  completed_shards?: number
  total_shards?: number
  resume_from?: number | null
  partial_success?: boolean
  channels?: Row[]
}

const ENDPOINT = '/api/export_hub/shards'
const shardStatuses = ['待续传', '传输中', '已完成', '传输失败']
const shardActions = ['完成分片', '标记失败', '续传']

const batchId = ref('3')
const channelId = ref('')
const statusFilter = ref('')
const strategy = ref('publish_success')
const busy = ref(false)
const errorMessage = ref('')
const shardRows = ref<Row[]>([])
const channels = ref<Row[]>([])
const batchState = ref<BatchState | null>(null)
const channelStates = ref<Row[]>([])
const files = ref<Row[]>([])

function showError(message: string) {
  errorMessage.value = message
  emit('error', message)
}

async function loadChannels() {
  try {
    const response = await request('/api/export_hub?size=200')
    if (!response.ok) return
    const payload = await response.json()
    channels.value = payload.items ?? []
  } catch {
    channels.value = []
  }
}

async function loadBatchState() {
  const id = Number(batchId.value)
  if (!id) {
    batchState.value = null
    channelStates.value = []
    return
  }
  try {
    const response = await request(`${ENDPOINT}/batches/${id}`)
    if (!response.ok) {
      batchState.value = null
      channelStates.value = []
      return
    }
    batchState.value = await response.json()
    channelStates.value = batchState.value?.channels ?? []
  } catch {
    batchState.value = null
    channelStates.value = []
  }
}

async function loadFiles() {
  const id = Number(batchId.value)
  const query = new URLSearchParams(id ? { batch_id: String(id) } : {}).toString()
  try {
    const response = await request(`${ENDPOINT}/files?${query}`)
    if (!response.ok) throw new Error('分发文件读取失败')
    const payload = await response.json()
    files.value = payload.items ?? []
  } catch (error) {
    files.value = []
    showError(error instanceof Error ? error.message : '分发文件读取失败')
  }
}

async function loadShards() {
  const id = Number(batchId.value)
  if (!id) {
    showError('请先填写结果批次ID')
    return
  }
  errorMessage.value = ''
  const query = new URLSearchParams({ batch_id: String(id) })
  if (channelId.value) query.set('channel_id', channelId.value)
  if (statusFilter.value) query.set('status', statusFilter.value)
  try {
    const response = await request(`${ENDPOINT}?${query.toString()}`)
    if (!response.ok) throw new Error('分片分发记录读取失败')
    const payload = await response.json()
    shardRows.value = payload.items ?? []
    await Promise.all([loadBatchState(), loadFiles()])
  } catch (error) {
    showError(error instanceof Error ? error.message : '分片分发记录读取失败')
  }
}

async function post(path: string, body: Record<string, unknown>) {
  busy.value = true
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${path}`, {
      method: 'POST', body: JSON.stringify(body),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '分片分发操作失败')
    return true
  } catch (error) {
    showError(error instanceof Error ? error.message : '分片分发操作失败')
    return false
  } finally {
    busy.value = false
  }
}

async function prepare() {
  const id = Number(batchId.value)
  if (!id) {
    showError('请先填写结果批次ID')
    return
  }
  const ids = channelId.value ? [Number(channelId.value)] : undefined
  // 生成分片时带上策略，保证后续打包沿用同一口径。
  const ok = await post('prepare', { batch_id: id, channel_ids: ids, strategy: strategy.value })
  if (ok) await loadShards()
}

async function resume() {
  const id = Number(batchId.value)
  if (!id) {
    showError('请先填写结果批次ID')
    return
  }
  const body: Record<string, unknown> = { batch_id: id }
  if (channelId.value) body.channel_id = Number(channelId.value)
  const ok = await post('resume', body)
  if (ok) await loadShards()
}

async function packageFiles() {
  const id = Number(batchId.value)
  if (!id) {
    showError('请先填写结果批次ID')
    return
  }
  const body: Record<string, unknown> = { batch_id: id, strategy: strategy.value }
  if (channelId.value) body.channel_id = Number(channelId.value)
  const ok = await post('package', body)
  if (ok) await loadShards()
}

async function runShardAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '分片动作未生效')
    // 动作后按批次重新读取，分片状态以落库结果为准。
    await loadShards()
  } catch (error) {
    showError(error instanceof Error ? error.message : '分片动作未生效')
  }
}

async function exportShards() {
  errorMessage.value = ''
  const id = Number(batchId.value)
  const params: Record<string, string> = {}
  if (id) params.batch_id = String(id)
  if (channelId.value) params.channel_id = channelId.value
  if (statusFilter.value) params.status = statusFilter.value
  const query = new URLSearchParams(params).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出分片清单失败')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'export_hub_shards.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    showError(error instanceof Error ? error.message : '导出分片清单失败')
  }
}

onMounted(async () => {
  await loadChannels()
  await loadShards()
})
</script>
