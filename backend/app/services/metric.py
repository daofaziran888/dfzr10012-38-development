"""流作业指标采集业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.services.latency_segments import latency_segment_service
from app.store import store

MODULE = "metric"
REQUIRED_FIELDS = ["采集器编号", "作业名称", "指标名称"]
STATUS_ORDER = ["草稿", "采集中", "已暂停", "已关闭"]
ACTION_RULES = {"启动采集": "草稿", "暂停采集": "采集中", "关闭采集": "已暂停"}
NEGATIVE_ACTIONS = []


class MetricService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("采集器编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"指标采集器 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流作业指标采集可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"指标采集器已{action}"

    def latency_result(self, entry_id: int) -> dict[str, Any] | None:
        """读取该采集器关联作业的延迟分段数据结果。

        与延迟追踪模块读同一份快照，并沿用快照里的指标采集版本；没有快照时返回 None。
        """
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        job_name = str(entry.get("作业名称") or "").strip()
        for snapshot in latency_segment_service.list_latest():
            if snapshot.get("流作业名") == job_name:
                return snapshot
        return None
