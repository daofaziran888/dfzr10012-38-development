import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/latency'
const columns = [
  "记录编号",
  "流作业名",
  "端到端延迟",
  "处理延迟",
  "网络延迟",
  "延迟阈值",
  "超标次数",
  "记录时间"
]
const actions = [
  "标记超标",
  "优化处理",
  "归档记录"
]
const statuses = [
  "采集中",
  "已超标",
  "已恢复",
  "已归档"
]
const stats = [
  {
    "label": "延迟记录总数",
    "value": 0
  },
  {
    "label": "已超标记录",
    "value": 0
  },
  {
    "label": "已恢复记录",
    "value": 0
  }
]
const segmentColumns = [
  "记录编号",
  "流作业名",
  "版本",
  "端到端",
  "分段耗时",
  "分段合计",
  "延迟阈值",
  "超标段",
  "指标采集版本"
]

type Row = Record<string, string | number | null>

type Segment = {
  key: string
  segment: string
  duration_ms: number
  share_percent: number
  derived: boolean
  is_over_threshold_segment: boolean
}

type Snapshot = {
  '记录编号': string
  '流作业名': string
  version_label: string
  metric_version: string
  end_to_end_ms: number
  segment_sum_ms: number
  threshold_ms: number
  over_threshold: boolean
  over_threshold_ms: number
  over_threshold_segment: string | null
  degraded: boolean
  degraded_reason: string | null
  reconciled: boolean
  segments: Segment[]
  lineage: { stage: string; ok: boolean; detail: string }[]
}

type Conflict = {
  message: string
  detected_at: string
}

export default function LatencyView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [snapshots, setSnapshots] = useState<Snapshot[]>([])
  const [segmentsError, setSegmentsError] = useState('')
  const [selected, setSelected] = useState<Snapshot | null>(null)
  const [conflict, setConflict] = useState<Conflict | null>(null)
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('延迟记录列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '延迟追踪管理列表读取失败')
    }
  }, [filters])

  const reloadSegments = useCallback(async () => {
    setSegmentsError('')
    try {
      const response = await request(`${ENDPOINT}/segments`)
      if (!response.ok) {
        throw new Error('延迟分段数据结果读取失败')
      }
      const payload = await response.json()
      setSnapshots(payload.items ?? [])
    } catch (error) {
      setSegmentsError(error instanceof Error ? error.message : '延迟分段数据结果读取失败')
    }
  }, [])

  useEffect(() => {
    void reload()
  }, [reload])

  useEffect(() => {
    void reloadSegments()
  }, [reloadSegments])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      if (!response.ok) {
        throw new Error('延迟追踪管理动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '延迟追踪管理操作失败')
    }
  }

  async function readSegments(row: Row) {
    setSegmentsError('')
    setConflict(null)
    const response = await request(`${ENDPOINT}/${row.id}/segments`)
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}))
      setSelected(null)
      setSegmentsError(payload.detail ?? '该记录尚无延迟分段数据结果')
      return
    }
    const payload = await response.json()
    setSelected(payload.snapshot as Snapshot)
    setConflict(payload.conflict ?? null)
  }

  async function snapshotSegments(row: Row) {
    setErrorMessage('')
    setConflict(null)
    try {
      const response = await request(`${ENDPOINT}/${row.id}/segments`, {
        method: 'POST',
        body: JSON.stringify({}),
      })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(payload.detail ?? '延迟分段数据结果写入失败')
      }
      if (!payload.ok) {
        setErrorMessage(payload.message)
      }
      await reloadSegments()
      await readSegments(row)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '延迟分段数据结果写入失败')
    }
  }

  return (
    <section className="page" data-module="latency">
      <header className="page-head">
        <div>
          <h2>延迟追踪管理管理</h2>
          <p className="page-desc">维护延迟追踪的端到端延迟、分段延迟与超标统计，为延迟追踪管理提供可审计的数据工程流程。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('延迟记录登记入口尚未接入审批流')}>
            登记延迟记录
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出延迟追踪管理清单
          </button>
        </div>
      </header>

      <div className="stat-row">
        {stats.map((item) => (
          <article key={item.label} className="stat-card">
            <span className="stat-label">{item.label}</span>
            <strong className="stat-value">{item.value}</strong>
          </article>
        ))}
      </div>

      <form className="filter-bar" onSubmit={(event) => { event.preventDefault(); void reload() }}>
        {filterFields.map((field) => (
          <label key={field} className="filter-item">
            <span>{field}</span>
            <input
              value={filters[field] ?? ''}
              placeholder={`按${field}检索`}
              onChange={(event) => setFilters((prev) => ({ ...prev, [field]: event.target.value }))}
            />
          </label>
        ))}
        <button className="btn" type="submit">查询</button>
        <button className="btn ghost" type="button" onClick={() => setFilters({})}>重置条件</button>
      </form>

      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => <th key={column}>{column}</th>)}
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={String(row.id)}>
              {columns.map((column) => <td key={column}>{row[column] ?? '—'}</td>)}
              <td className="row-actions">
                {actions.map((action) => (
                  <button key={action} className="link" type="button" onClick={() => void runAction(action, row)}>
                    {action}
                  </button>
                ))}
                <button className="link" type="button" onClick={() => void snapshotSegments(row)}>生成分段结果</button>
                <button className="link" type="button" onClick={() => void readSegments(row)}>查看分段结果</button>
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 1}>暂无延迟追踪管理数据，可先登记延迟记录</td>
            </tr>
          )}
        </tbody>
      </table>

      <section className="segment-panel">
        <header className="segment-panel-head">
          <div>
            <h3>延迟分段数据结果</h3>
            <p className="page-desc">各段耗时之和必须等于端到端总耗时；同一记录只保留最新版本，历史版本不会被消费。</p>
          </div>
        </header>

        <table className="data-table">
          <thead>
            <tr>
              {segmentColumns.map((column) => <th key={column}>{column}</th>)}
            </tr>
          </thead>
          <tbody>
            {snapshots.map((snapshot) => (
              <tr key={snapshot.version_label}>
                <td>{snapshot['记录编号']}</td>
                <td>{snapshot['流作业名'] || '—'}</td>
                <td>{snapshot.version_label}</td>
                <td>{snapshot.end_to_end_ms}ms</td>
                <td>
                  {snapshot.degraded
                    ? '历史记录缺少分段字段，仅保留总耗时'
                    : snapshot.segments.map((item) => (
                      `${item.segment} ${item.duration_ms}ms${item.derived ? '（残差）' : ''}`
                    )).join(' / ')}
                </td>
                <td>{snapshot.segment_sum_ms}ms{snapshot.reconciled ? ' ✔' : ' ✘'}</td>
                <td>{snapshot.threshold_ms}ms</td>
                <td>
                  {snapshot.over_threshold
                    ? `${snapshot.over_threshold_segment ?? '—'}（超 ${snapshot.over_threshold_ms}ms）`
                    : '未超标'}
                </td>
                <td>{snapshot.metric_version}</td>
              </tr>
            ))}
            {snapshots.length === 0 && (
              <tr>
                <td className="empty-state" colSpan={segmentColumns.length}>暂无延迟分段数据结果，可在记录行点「生成分段结果」</td>
              </tr>
            )}
          </tbody>
        </table>

        {selected && (
          <div className="segment-lineage">
            <h4>{selected.version_label} 处理经过</h4>
            <ol>
              {selected.lineage.map((step, index) => (
                <li key={`${step.stage}-${index}`} className={step.ok ? undefined : 'error-text'}>
                  <strong>{step.stage}</strong>：{step.detail}
                </li>
              ))}
            </ol>
          </div>
        )}
        {conflict && <p className="error-text">最近一次采样冲突：{conflict.message}（{conflict.detected_at}）</p>}
        {segmentsError && <p className="error-text">{segmentsError}</p>}
      </section>

      <footer className="page-foot">
        <span>共 {total} 条延迟追踪管理记录</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
