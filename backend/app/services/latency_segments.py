"""延迟分段数据结果：把端到端延迟拆成处理段、网络段、输出段并做幂等快照。

延迟追踪记录本身只保存端到端总耗时、处理延迟、网络延迟与延迟阈值，缺少可归因的
分段结果。本模块沿延迟追踪的现有数据链路补齐这一段：

1. 读取延迟记录（``latency`` 模块）里的总耗时、处理延迟、网络延迟与阈值；
2. 解析成毫秒整数并校验；
3. 输出段按「端到端总耗时 - 处理延迟 - 网络延迟」的残差补齐，凑齐三段；
4. 核对各段耗时之和与端到端总耗时，冲突时拒绝写入、不产生第二个超标结论；
5. 以内容摘要做幂等快照，并发采样在锁内串行分配版本，只保留最新版本；
6. 历史记录缺少处理/网络字段时降级为「仅保留总耗时」，不丢记录。

快照是跨模块共享的唯一事实来源：指标采集等消费侧通过 ``consume`` 读到同一份
结果，并沿用快照里的 ``metric_version``（指标采集版本）。
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import threading
from datetime import datetime, timezone
from typing import Any

from app.store import store

LATENCY_MODULE = "latency"

FIELD_RECORD_NO = "记录编号"
FIELD_JOB = "流作业名"
FIELD_TOTAL = "端到端延迟"
FIELD_PROCESSING = "处理延迟"
FIELD_NETWORK = "网络延迟"
FIELD_OUTPUT = "输出延迟"
FIELD_THRESHOLD = "延迟阈值"
FIELD_METRIC_VERSION = "指标采集版本"

# 指标采集链路当前使用的数据版本；采样时可覆盖，消费侧原样沿用。
METRIC_COLLECTION_VERSION = "metric-collect-v1"

SEGMENT_SPECS: tuple[tuple[str, str, str], ...] = (
    ("processing", "处理段", FIELD_PROCESSING),
    ("network", "网络段", FIELD_NETWORK),
    ("output", "输出段", FIELD_OUTPUT),
)

_DURATION_RE = re.compile(r"^([0-9]+(?:\.[0-9]+)?)\s*(ms|毫秒|s|秒)?$", re.IGNORECASE)
_BLANK_VALUES = {"", "-", "—", "n/a", "na", "未采集", "未知"}


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _is_blank(value: Any) -> bool:
    return _text(value).lower() in _BLANK_VALUES


def parse_duration_ms(value: Any) -> int | None:
    """把 ``920`` / ``"920ms"`` / ``"0.92s"`` 统一解析成毫秒整数，非法值返回 None。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if value < 0 or value != value:
            return None
        return int(round(value))
    text = _text(value).replace(",", "").lower()
    if not text:
        return None
    matched = _DURATION_RE.match(text)
    if matched is None:
        return None
    number = float(matched.group(1))
    unit = (matched.group(2) or "ms").lower()
    if unit in {"s", "秒"}:
        number *= 1000
    if number < 0:
        return None
    return int(round(number))


def _pick(values: dict[str, Any], record: dict[str, Any], field: str) -> Any:
    """采样值优先，但采样值显式传空串时视为「沿用记录原值」，避免误清空。"""
    if field in values and not _is_blank(values.get(field)):
        return values.get(field)
    return record.get(field)


def _digest(basis: dict[str, Any]) -> str:
    payload = json.dumps(basis, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _share(duration: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(duration * 100 / total, 1)


class LatencySegmentService:
    """延迟分段结果的构建、版本化快照与跨模块读取。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # 只保留每个记录编号的最新版本，避免历史版本被误读成第二份结论。
        self._latest: dict[str, dict[str, Any]] = {}
        # 最近一次被拒绝的冲突采样，仅用于提示，不参与任何消费结论。
        self._conflicts: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------ 读取
    def list_latest(self) -> list[dict[str, Any]]:
        with self._lock:
            snapshots = [copy.deepcopy(item) for item in self._latest.values()]
        return sorted(snapshots, key=lambda item: str(item.get("记录编号", "")))

    def latest(self, record_no: str) -> dict[str, Any] | None:
        with self._lock:
            snapshot = self._latest.get(_text(record_no))
            return copy.deepcopy(snapshot) if snapshot is not None else None

    def find_record(self, record_no: str | None = None, job_name: str | None = None) -> dict[str, Any] | None:
        """按记录编号或作业名定位延迟记录，供跨模块采样入口使用。"""
        wanted_no = _text(record_no)
        wanted_job = _text(job_name)
        matched: list[dict[str, Any]] = []
        for row in store.rows(LATENCY_MODULE):
            if wanted_no and _text(row.get(FIELD_RECORD_NO)) == wanted_no:
                return row
            if not wanted_no and wanted_job and _text(row.get(FIELD_JOB)) == wanted_job:
                matched.append(row)
        if len(matched) > 1:
            # 同一作业有多条记录时优先取已有分段结果的记录，保证消费侧读到的是已定位版本。
            for row in matched:
                row_no = _text(row.get(FIELD_RECORD_NO)) or f"ID-{row.get('id')}"
                if row_no in self._latest:
                    return row
        return matched[0] if matched else None

    def last_conflict(self, record_no: str) -> dict[str, Any] | None:
        with self._lock:
            conflict = self._conflicts.get(_text(record_no))
            return copy.deepcopy(conflict) if conflict is not None else None

    def consume(self, record_no: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        """消费侧原子读取：同一把锁下拿到唯一一份最新快照及其冲突提示。"""
        with self._lock:
            snapshot = self._latest.get(_text(record_no))
            conflict = self._conflicts.get(_text(record_no))
            return (
                copy.deepcopy(snapshot) if snapshot is not None else None,
                copy.deepcopy(conflict) if conflict is not None else None,
            )

    def resolve_for_job(
        self, job_name: str
    ) -> tuple[str | None, dict[str, Any] | None, dict[str, Any] | None]:
        """按作业名定位延迟记录并原子读取其最新分段结果。

        返回 ``(记录编号, 快照, 冲突提示)``；同一把锁内完成定位与读取，避免拿到
        已被并发采样覆盖的旧版本。
        """
        with self._lock:
            record = self.find_record(job_name=job_name)
            if record is None:
                return None, None, None
            record_no = _text(record.get(FIELD_RECORD_NO)) or f"ID-{record.get('id')}"
            snapshot = self._latest.get(record_no)
            conflict = self._conflicts.get(record_no)
            return (
                record_no,
                copy.deepcopy(snapshot) if snapshot is not None else None,
                copy.deepcopy(conflict) if conflict is not None else None,
            )

    def reset(self) -> None:
        """测试辅助：清空快照与冲突记录。"""
        with self._lock:
            self._latest.clear()
            self._conflicts.clear()

    # ------------------------------------------------------------------ 构建
    def _evaluate(
        self,
        record: dict[str, Any],
        values: dict[str, Any],
    ) -> dict[str, Any]:
        """解析一条延迟记录并构造分段快照；返回构建结果（可能带校验错误或冲突）。"""
        # 记录编号与流作业名是身份字段，不允许被采样值改写，否则同一记录会出现两个版本序列，
        # 或把指标采集的关联关系指到别的作业上。
        record_no = _text(record.get(FIELD_RECORD_NO))
        record_id = record.get("id")
        if not record_no:
            record_no = f"ID-{record_id}"
        job_name = _text(record.get(FIELD_JOB))
        metric_version = (
            _text(values.get(FIELD_METRIC_VERSION))
            or _text(record.get(FIELD_METRIC_VERSION))
            or METRIC_COLLECTION_VERSION
        )

        errors: list[str] = []
        total = parse_duration_ms(_pick(values, record, FIELD_TOTAL))
        threshold = parse_duration_ms(_pick(values, record, FIELD_THRESHOLD))
        processing_raw = _pick(values, record, FIELD_PROCESSING)
        network_raw = _pick(values, record, FIELD_NETWORK)
        output_raw = _pick(values, record, FIELD_OUTPUT)

        if total is None:
            errors.append(f"{FIELD_TOTAL}「{_text(_pick(values, record, FIELD_TOTAL))}」无法解析为毫秒数")
        if threshold is None:
            errors.append(f"{FIELD_THRESHOLD}「{_text(_pick(values, record, FIELD_THRESHOLD))}」无法解析为毫秒数")
        elif threshold <= 0:
            errors.append(f"{FIELD_THRESHOLD}必须大于 0ms")

        processing = parse_duration_ms(processing_raw)
        network = parse_duration_ms(network_raw)
        if not _is_blank(processing_raw) and processing is None:
            errors.append(f"{FIELD_PROCESSING}「{_text(processing_raw)}」无法解析为毫秒数")
        if not _is_blank(network_raw) and network is None:
            errors.append(f"{FIELD_NETWORK}「{_text(network_raw)}」无法解析为毫秒数")

        explicit_output = None
        if not _is_blank(output_raw):
            explicit_output = parse_duration_ms(output_raw)
            if explicit_output is None:
                errors.append(f"{FIELD_OUTPUT}「{_text(output_raw)}」无法解析为毫秒数")

        if errors or total is None or threshold is None:
            return {
                "ok": False,
                "kind": "validation",
                "message": "分段耗时校验失败：" + "；".join(errors),
                "errors": errors,
                "lineage": [
                    {"stage": "读取延迟记录", "ok": True, "detail": f"{record_no}（延迟记录 #{record_id}）"},
                    {"stage": "解析耗时字段", "ok": False, "detail": "；".join(errors)},
                ],
            }

        degraded = processing is None or network is None
        lineage: list[dict[str, Any]] = [
            {
                "stage": "读取延迟记录",
                "ok": True,
                "detail": f"{record_no}（延迟记录 #{record_id}，作业 {job_name or '未命名'}）",
            },
            {
                "stage": "解析耗时字段",
                "ok": True,
                "detail": (
                    f"端到端 {total}ms、处理 {processing_raw if processing is not None else '缺失'}、"
                    f"网络 {network_raw if network is not None else '缺失'}、阈值 {threshold}ms"
                ),
            },
        ]

        if degraded:
            # 历史记录缺少分段字段：不丢记录，保留端到端总耗时，不做三段归因。
            over_threshold = total > threshold
            excess = total - threshold if over_threshold else 0
            lineage.extend([
                {
                    "stage": "校验分段耗时",
                    "ok": True,
                    "detail": "缺少处理延迟或网络延迟字段，跳过三段拆分，保留端到端总耗时",
                },
                {
                    "stage": "端到端核对",
                    "ok": True,
                    "detail": f"沿用端到端总耗时 {total}ms（历史记录无分段字段可比对）",
                },
            ])
            snapshot = {
                "记录编号": record_no,
                "记录ID": record_id,
                "流作业名": job_name,
                "end_to_end_ms": total,
                "processing_ms": processing,
                "network_ms": network,
                "output_ms": None,
                "threshold_ms": threshold,
                "segments": [],
                "segment_sum_ms": total,
                "reconciled": True,
                "degraded": True,
                "degraded_reason": "历史记录缺少处理延迟或网络延迟字段，保留端到端总耗时",
                "over_threshold": over_threshold,
                "over_threshold_ms": excess,
                "over_threshold_segment": None,
                "metric_version": metric_version,
                "digest": _digest({
                    "record": record_no,
                    "record_id": record_id,
                    "job": job_name,
                    "end_to_end_ms": total,
                    "processing_ms": processing,
                    "network_ms": network,
                    "output_ms": None,
                    "threshold_ms": threshold,
                    "metric_version": metric_version,
                    "degraded": True,
                }),
                "lineage": lineage,
            }
            return {"ok": True, "kind": "degraded", "snapshot": snapshot, "conflict": None}

        assert processing is not None and network is not None
        output = total - processing - network if explicit_output is None else explicit_output
        durations = {
            "processing": processing,
            "network": network,
            "output": output,
        }
        segment_sum = sum(durations.values())
        lineage.append({
            "stage": "校验分段耗时",
            "ok": True,
            "detail": (
                f"处理段 {processing}ms、网络段 {network}ms、"
                f"输出段 {'取显式值 ' if explicit_output is not None else '按残差补齐 '}{output}ms"
            ),
        })

        if output < 0 or segment_sum != total:
            difference = segment_sum - total
            negative_output = output < 0
            reason = (
                "处理与网络延迟合计已超过端到端总耗时"
                if negative_output
                else f"分段耗时合计 {segment_sum}ms 与端到端总耗时 {total}ms 相差 {difference}ms"
            )
            conflict = {
                "reason": "segment_sum_mismatch",
                "记录编号": record_no,
                "end_to_end_ms": total,
                "segment_sum_ms": segment_sum,
                "difference_ms": (-output if negative_output else difference),
                "negative_output": negative_output,
                "processing_ms": processing,
                "network_ms": network,
                "output_ms": output,
                "threshold_ms": threshold,
                "message": f"端到端总耗时与分段耗时冲突：{reason}",
                "detected_at": _now(),
            }
            lineage.append({"stage": "端到端核对", "ok": False, "detail": conflict["message"]})
            return {"ok": False, "kind": "conflict", "conflict": conflict, "lineage": lineage}

        segments = [
            {
                "key": key,
                "segment": label,
                "source_field": source_field,
                "duration_ms": durations[key],
                "share_percent": _share(durations[key], total),
                "derived": key == "output" and explicit_output is None,
                "is_over_threshold_segment": False,
            }
            for key, label, source_field in SEGMENT_SPECS
        ]

        over_threshold = total > threshold
        excess = total - threshold if over_threshold else 0
        # 归因规则：超标时取耗时占比最大的分段作为超标段，并列取顺序靠前者；
        # 归因只用于展示，不改变「端到端是否超标」这一唯一结论。
        over_segment = None
        if over_threshold and segments:
            dominant = max(segments, key=lambda item: item["duration_ms"])
            for item in segments:
                if item is dominant:
                    item["is_over_threshold_segment"] = True
                    over_segment = item["segment"]
        lineage.append({
            "stage": "端到端核对",
            "ok": True,
            "detail": f"分段合计 {segment_sum}ms = 端到端 {total}ms，核对通过",
        })

        snapshot = {
            "记录编号": record_no,
            "记录ID": record_id,
            "流作业名": job_name,
            "end_to_end_ms": total,
            "processing_ms": processing,
            "network_ms": network,
            "output_ms": output,
            "threshold_ms": threshold,
            "segments": segments,
            "segment_sum_ms": segment_sum,
            "reconciled": segment_sum == total,
            "degraded": False,
            "degraded_reason": None,
            "over_threshold": over_threshold,
            "over_threshold_ms": excess,
            "over_threshold_segment": over_segment,
            "metric_version": metric_version,
            "digest": _digest({
                "record": record_no,
                "record_id": record_id,
                "job": job_name,
                "end_to_end_ms": total,
                "processing_ms": processing,
                "network_ms": network,
                "output_ms": output,
                "threshold_ms": threshold,
                "metric_version": metric_version,
                "degraded": False,
            }),
            "lineage": lineage,
        }
        return {"ok": True, "kind": "snapshot", "snapshot": snapshot, "conflict": None}

    # ------------------------------------------------------------------ 写入
    def snapshot_record(self, entry_id: int, values: dict[str, Any] | None = None) -> dict[str, Any]:
        """读取延迟记录并写入分段快照。

        - 校验失败 / 对账冲突：不写快照，返回 ``ok=False`` 并附上已有版本（若有）；
        - 相同内容摘要：返回已有版本，``idempotent=True``；
        - 内容变化：版本号 +1 并覆盖旧版本，只保留最新一份。
        """
        overrides = dict(values or {})
        with self._lock:
            record = store.find(LATENCY_MODULE, entry_id)
            if record is None:
                return {
                    "ok": False,
                    "kind": "not_found",
                    "message": f"延迟记录 {entry_id} 不存在或已归档",
                    "snapshot": None,
                    "conflict": None,
                    "lineage": [],
                }

            evaluated = self._evaluate(record, overrides)
            record_no = _text(record.get(FIELD_RECORD_NO)) or f"ID-{entry_id}"
            existing = self._latest.get(record_no)

            if evaluated["kind"] == "conflict":
                conflict = evaluated["conflict"]
                self._conflicts[record_no] = conflict
                message = conflict["message"] + "，本次采样未写入快照"
                if existing is not None:
                    message += f"，沿用已定位版本 {existing['version_label']}"
                else:
                    message += "，当前没有可沿用的延迟分段数据结果"
                return {
                    "ok": False,
                    "kind": "conflict",
                    "message": message,
                    "snapshot": copy.deepcopy(existing) if existing is not None else None,
                    "conflict": conflict,
                    "adopted": existing is not None,
                    "lineage": evaluated["lineage"],
                }

            if not evaluated["ok"]:
                return {
                    "ok": False,
                    "kind": "validation",
                    "message": evaluated["message"],
                    "errors": evaluated["errors"],
                    "snapshot": copy.deepcopy(existing) if existing is not None else None,
                    "conflict": None,
                    "adopted": existing is not None,
                    "lineage": evaluated["lineage"],
                }

            candidate: dict[str, Any] = dict(evaluated["snapshot"])
            candidate["metric_version"] = candidate.get("metric_version") or METRIC_COLLECTION_VERSION

            if existing is not None and existing.get("digest") == candidate.get("digest"):
                return {
                    "ok": True,
                    "kind": "idempotent",
                    "idempotent": True,
                    "message": f"采样内容未变化，命中幂等，沿用版本 {existing['version_label']}",
                    "snapshot": copy.deepcopy(existing),
                    "conflict": None,
                    "adopted": True,
                    "lineage": copy.deepcopy(existing.get("lineage", [])),
                }

            version = int(existing.get("version", 0)) + 1 if existing is not None else 1
            candidate["version"] = version
            candidate["version_label"] = f"{record_no}@v{version}"
            candidate["sampled_at"] = _now()
            candidate["lineage"] = list(candidate.get("lineage", [])) + [{
                "stage": "幂等快照写入",
                "ok": True,
                "detail": (
                    f"内容摘要 {candidate['digest'][:12]}，写入版本 {candidate['version_label']}，"
                    "同一记录仅保留最新版本"
                ),
            }]
            self._latest[record_no] = candidate
            self._conflicts.pop(record_no, None)

            return {
                "ok": True,
                "kind": "written",
                "idempotent": False,
                "message": f"延迟分段数据结果已写入，版本 {candidate['version_label']}",
                "snapshot": copy.deepcopy(candidate),
                "conflict": None,
                "adopted": False,
                "superseded_version": existing["version_label"] if existing is not None else None,
                "lineage": copy.deepcopy(candidate["lineage"]),
            }


latency_segment_service = LatencySegmentService()
