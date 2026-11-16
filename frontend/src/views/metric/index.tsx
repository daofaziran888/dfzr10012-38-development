import { useCallback, useEffect, useMemo, useState } from 'react'

import { request } from '@/api/client'

const ENDPOINT = '/api/metric'
const columns = [
  "采集器编号",
  "作业名称",
  "指标名称",
  "采集间隔",
  "聚合方式",
  "保留天数",
  "最近采集",
  "采集器状态"
]
const actions = [
  "启动采集",
  "暂停采集",
  "关闭采集"
]
const statuses = [
  "草稿",
  "采集中",
  "已暂停",
  "已关闭"
]
const stats = [
  {
    "label": "指标采集器总数",
    "value": 0
  },
  {
    "label": "采集中采集器",
    "value": 0
  },
  {
    "label": "已暂停采集器",
    "value": 0
  }
]

type Row = Record<string, string | number | null>

type LatencyResult = {
  version_label: string
  metric_version: string
  end_to_end_ms: number
  over_threshold: boolean
  over_threshold_ms: number
  over_threshold_segment: string | null
  degraded: boolean
}

export default function MetricView() {
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [selected, setSelected] = useState<{ job: string; result: LatencyResult | null; message: string; version: string | null; conflict: string | null } | null>(null)
  const filterFields = useMemo(() => columns.slice(0, 3), [])

  const reload = useCallback(async () => {
    setErrorMessage('')
    const query = new URLSearchParams(filters).toString()
    try {
      const response = await request(`${ENDPOINT}?${query}`)
      if (!response.ok) {
        throw new Error('指标采集器列表读取失败')
      }
      const payload = await response.json()
      setRows(payload.items ?? [])
      setTotal(payload.total ?? 0)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '流作业指标采集列表读取失败')
    }
  }, [filters])

  useEffect(() => {
    void reload()
  }, [reload])

  async function runAction(action: string, row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/actions`, {
        method: 'POST',
        body: JSON.stringify({ action }),
      })
      if (!response.ok) {
        throw new Error('流作业指标采集动作未生效，请稍后重试')
      }
      await reload()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '流作业指标采集操作失败')
    }
  }

  async function readLatencyResult(row: Row) {
    setErrorMessage('')
    try {
      const response = await request(`${ENDPOINT}/${row.id}/latency-result`)
      if (!response.ok) {
        throw new Error('延迟分段数据结果读取失败')
      }
      const payload = await response.json()
      setSelected({
        job: payload['作业名称'] ?? '',
        result: payload.result ?? null,
        message: payload.message ?? '',
        version: payload['version_label'] ?? null,
        conflict: payload.conflict?.message ?? null,
      })
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '延迟分段数据结果读取失败')
    }
  }

  return (
    <section className="page" data-module="metric">
      <header className="page-head">
        <div>
          <h2>流作业指标采集管理</h2>
          <p className="page-desc">维护流作业指标的采集间隔、聚合方式与保留策略，为流作业指标采集提供可审计的数据工程流程。</p>
        </div>
        <div className="page-actions">
          <button className="btn primary" type="button" onClick={() => setErrorMessage('指标采集器登记入口尚未接入审批流')}>
            登记指标采集器
          </button>
          <button className="btn" type="button" onClick={() => window.open(`${ENDPOINT}/export`, '_blank')}>
            导出流作业指标采集清单
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
                <button className="link" type="button" onClick={() => void readLatencyResult(row)}>读取延迟分段结果</button>
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={columns.length + 1}>暂无流作业指标采集数据，可先登记指标采集器</td>
            </tr>
          )}
        </tbody>
      </table>

      <section className="segment-panel">
        <header className="segment-panel-head">
          <div>
            <h3>延迟分段数据结果（指标采集关联）</h3>
            <p className="page-desc">与延迟追踪模块读取同一份延迟分段数据结果，沿用该结果的指标采集版本，不另行计算超标结论。</p>
          </div>
        </header>
        {selected ? (
          <div className="segment-lineage">
            <p><strong>作业</strong>：{selected.job || '—'}　<strong>结果版本</strong>：{selected.version ?? '—'}</p>
            <p>{selected.message}</p>
            {selected.result ? (
              <ul>
                <li>端到端：{selected.result.end_to_end_ms}ms</li>
                <li>
                  结论：
                  {selected.result.over_threshold
                    ? `超标 ${selected.result.over_threshold_ms}ms，超标段 ${selected.result.over_threshold_segment ?? '—'}`
                    : '未超标'}
                </li>
                <li>指标采集版本：{selected.result.metric_version}</li>
                {selected.result.degraded && <li>历史记录缺少分段字段，仅保留总耗时</li>}
              </ul>
            ) : (
              <p className="error-text">尚无可用延迟分段数据结果，未上报任何超标结论。</p>
            )}
            {selected.conflict && <p className="error-text">最近一次采样冲突：{selected.conflict}</p>}
          </div>
        ) : (
          <p className="page-desc">点记录行的「读取延迟分段结果」查看该作业沿用的延迟分段数据结果。</p>
        )}
      </section>

      <footer className="page-foot">
        <span>共 {total} 条流作业指标采集记录</span>
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
