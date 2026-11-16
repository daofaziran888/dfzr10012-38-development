"""流作业指标采集接口：维护指标采集器，覆盖启动采集、暂停采集、关闭采集等动作。

指标采集关联处理统一从延迟追踪模块读取同一份「延迟分段数据结果」，并沿用该结果里的
指标采集版本，不另外计算一套延迟结论。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.latency_segments import latency_segment_service
from app.services.metric import MetricService

router = APIRouter(prefix="/api/metric", tags=["流作业指标采集"])

service = MetricService()

LIST_FIELDS = ["采集器编号", "作业名称", "指标名称", "采集间隔", "聚合方式", "保留天数", "最近采集", "采集器状态"]
STATUSES = ["草稿", "采集中", "已暂停", "已关闭"]


def _attach_latency_result(entry: dict[str, Any]) -> dict[str, Any]:
    """把该采集器作业对应的延迟分段结果挂到明细上，消费侧直接沿用同一个版本。"""
    enriched = dict(entry)
    result = service.latency_result(entry_id=int(entry.get("id", 0)))
    if result is None:
        enriched["latency_segment_result"] = None
        enriched["延迟分段结论"] = "尚无可用延迟分段数据结果"
    else:
        enriched["latency_segment_result"] = result
        enriched["延迟分段版本"] = result.get("version_label")
        enriched["指标采集版本"] = result.get("metric_version")
        enriched["延迟分段结论"] = (
            f"端到端 {result.get('end_to_end_ms', '—')}ms，"
            + (
                f"超标 {result.get('over_threshold_ms', 0)}ms，超标段 {result.get('over_threshold_segment') or '—'}"
                if result.get("over_threshold")
                else "未超标"
            )
        )
    return enriched


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按采集器编号检索"),
    status: str | None = Query(default=None, description="草稿、采集中、已暂停、已关闭"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按采集器编号与状态过滤流作业指标采集列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{entry_id}/latency-result")
def get_latency_result(entry_id: int) -> dict[str, Any]:
    """指标采集关联处理：与延迟追踪模块读取同一份延迟分段数据结果，并沿用其版本。

    没有可归因结果时不臆造结论；若该作业最近一次采样因分段冲突被拒，只带回冲突说明，
    对外始终只给出一条已定位结果或空结论。
    """
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"指标采集器 {entry_id} 不存在或已归档")
    job_name = str(entry.get("作业名称") or "").strip()
    record_no, snapshot, conflict = latency_segment_service.resolve_for_job(job_name)
    if record_no is None:
        return {
            "module": "metric",
            "entry_id": entry_id,
            "作业名称": job_name,
            "result": None,
            "message": f"作业「{job_name}」没有对应的延迟记录，暂无延迟分段数据结果",
        }
    return {
        "module": "metric",
        "entry_id": entry_id,
        "作业名称": job_name,
        "记录编号": record_no,
        "result": snapshot,
        "metric_version": snapshot.get("metric_version") if snapshot else None,
        "version_label": snapshot.get("version_label") if snapshot else None,
        "conflict": conflict,
        "message": (
            f"沿用延迟分段数据结果 {snapshot['version_label']}"
            if snapshot is not None
            else f"记录 {record_no} 尚无延迟分段数据结果，未形成超标结论"
        ),
    }


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条指标采集器明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"指标采集器 {entry_id} 不存在或已归档")
    return _attach_latency_result(entry)


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条指标采集器，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="指标采集器已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条指标采集器执行启动采集、暂停采集、关闭采集；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出流作业指标采集清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "metric", "total": total, "items": items}


