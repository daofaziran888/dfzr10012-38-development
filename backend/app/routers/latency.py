"""延迟追踪管理接口：维护延迟记录，覆盖标记超标、优化处理、归档记录等动作。

除原有记录接口外，这里补充延迟分段数据结果：
``POST /{entry_id}/segments`` 采样并幂等写入，``GET /{entry_id}/segments`` 读取最新结果，
``GET /segments`` 列出所有记录的最新分段快照。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.latency import LatencyService
from app.services.latency_segments import latency_segment_service

router = APIRouter(prefix="/api/latency", tags=["延迟追踪管理"])

service = LatencyService()

LIST_FIELDS = ["记录编号", "流作业名", "端到端延迟", "处理延迟", "网络延迟", "延迟阈值", "超标次数", "记录时间"]
STATUSES = ["采集中", "已超标", "已恢复", "已归档"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按记录编号检索"),
    status: str | None = Query(default=None, description="采集中、已超标、已恢复、已归档"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按记录编号与状态过滤延迟追踪管理列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/segments")
def list_segment_snapshots() -> dict[str, Any]:
    """列出每条延迟记录的最新延迟分段数据结果；历史版本不会出现在这里。"""
    items = latency_segment_service.list_latest()
    return {
        "module": "latency",
        "result": "延迟分段数据结果",
        "total": len(items),
        "items": items,
    }


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出延迟追踪管理清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "latency", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条延迟记录明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"延迟记录 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条延迟记录，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="延迟记录已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条延迟记录执行标记超标、优化处理、归档记录；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/segments", response_model=ActionResult)
def snapshot_segments(entry_id: int, payload: dict[str, Any] | None = Body(default=None)) -> ActionResult:
    """读取延迟记录 → 拆三段 → 核对对账 → 幂等写入快照。

    请求体可选地带上 ``端到端延迟``、``处理延迟``、``网络延迟``、``输出延迟``、
    ``延迟阈值``、``指标采集版本``，用于按新的采样值覆盖；既可平铺提交，也可包在
    ``values`` 里。同一内容重复提交返回同一版本；分段合计与端到端总耗时冲突时不写入
    新快照，只保留已定位的那一份结果。
    """
    body = payload or {}
    values = body.get("values") if isinstance(body.get("values"), dict) else None
    overrides = {key: value for key, value in body.items() if key not in {"values", "remark"}}
    if values is not None:
        overrides = {**overrides, **values}
    result = latency_segment_service.snapshot_record(entry_id, overrides)
    entry = result.get("snapshot")
    if result["kind"] == "not_found":
        raise HTTPException(status_code=404, detail=result["message"])
    return ActionResult(
        ok=bool(result["ok"]),
        message=str(result["message"]),
        entry=entry,
    )



@router.get("/{entry_id}/segments")
def get_segments(entry_id: int, record_no: str | None = Query(default=None)) -> dict[str, Any]:
    """读取单条延迟记录的最新分段结果；没有快照时给出可读说明而不是空对象。"""
    record = service.get_entry(entry_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"延迟记录 {entry_id} 不存在或已归档")
    resolved_no = record_no or str(record.get("记录编号") or f"ID-{entry_id}")
    snapshot, conflict = latency_segment_service.consume(resolved_no)
    if snapshot is None:
        detail = f"记录 {resolved_no} 尚无延迟分段数据结果"
        if conflict:
            detail += f"；最近一次采样因「{conflict['message']}」未写入"
        raise HTTPException(status_code=404, detail=detail)
    return {
        "module": "latency",
        "result": "延迟分段数据结果",
        "record": {
            "id": entry_id,
            "记录编号": record.get("记录编号"),
            "流作业名": record.get("流作业名"),
        },
        "snapshot": snapshot,
        "conflict": conflict,
    }
