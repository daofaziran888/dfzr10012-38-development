"""延迟分段数据结果的验收测试：直接跑后端业务规则，不依赖 HTTP 服务。

运行：backend/.venv/bin/python -I tests/test_latency_segments.py（在 backend 目录下）。
"""
from __future__ import annotations

import json
import sys
import threading

sys.path.insert(0, ".")

from app.services.latency_segments import (  # noqa: E402
    LatencySegmentService,
    METRIC_COLLECTION_VERSION,
    latency_segment_service,
    parse_duration_ms,
)
from app.services.metric import MetricService  # noqa: E402

FAILURES: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if condition:
        print(f"PASS  {name}")
    else:
        FAILURES.append(name)
        print(f"FAIL  {name}  {detail}")


def test_acceptance_late_0002() -> None:
    latency_segment_service.reset()
    result = latency_segment_service.snapshot_record(2)
    snapshot = result["snapshot"]
    check("LATE-0002 采样写入成功", result["kind"] == "written", result["message"])
    check("LATE-0002 拆成三段", len(snapshot["segments"]) == 3)
    durations = {item["key"]: item["duration_ms"] for item in snapshot["segments"]}
    check("处理段 600ms", durations["processing"] == 600, str(durations))
    check("网络段 250ms", durations["network"] == 250, str(durations))
    check("输出段补齐 70ms", durations["output"] == 70, str(durations))
    check("分段合计 = 端到端 920ms", snapshot["segment_sum_ms"] == snapshot["end_to_end_ms"] == 920)
    check("超出阈值 120ms", snapshot["over_threshold"] and snapshot["over_threshold_ms"] == 120)
    check("标出超标段", snapshot["over_threshold_segment"] == "处理段")
    check("只有处理段被标记超标", [s["key"] for s in snapshot["segments"] if s["is_over_threshold_segment"]] == ["processing"])
    check("保留指标采集版本", snapshot["metric_version"] == METRIC_COLLECTION_VERSION)
    check("记录处理经过", [step["stage"] for step in snapshot["lineage"]] == [
        "读取延迟记录", "解析耗时字段", "校验分段耗时", "端到端核对", "幂等快照写入",
    ])


def test_idempotent_write() -> None:
    latency_segment_service.reset()
    first = latency_segment_service.snapshot_record(2)
    second = latency_segment_service.snapshot_record(2)
    check("重复采样命中幂等", second["kind"] == "idempotent", second["message"])
    check("幂等返回同一版本", first["snapshot"]["version_label"] == second["snapshot"]["version_label"])
    check("幂等不产生新版本号", second["snapshot"]["version"] == 1)


def test_reconcile_conflict_rejects_write() -> None:
    latency_segment_service.reset()
    latency_segment_service.snapshot_record(2)
    conflict = latency_segment_service.snapshot_record(2, {"处理延迟": "700ms", "网络延迟": "300ms"})
    check("冲突采样被拒绝", conflict["ok"] is False and conflict["kind"] == "conflict", conflict["message"])
    check("冲突不覆盖快照", latency_segment_service.latest("LATE-0002")["version_label"] == "LATE-0002@v1")
    check("冲突沿用已定位版本", conflict["snapshot"]["version_label"] == "LATE-0002@v1")
    check("冲突带出差异", conflict["conflict"]["difference_ms"] == 80, json.dumps(conflict["conflict"], ensure_ascii=False))

    explicit = latency_segment_service.snapshot_record(2 + 1, {"输出延迟": "999ms"})
    check("显式分段合计不符也被拒绝", explicit["ok"] is False and explicit["kind"] == "conflict")


def test_concurrent_latest_only() -> None:
    service = LatencySegmentService()
    results: list[dict] = []

    def worker(index: int) -> None:
        results.append(service.snapshot_record(2, {"处理延迟": f"{400 + index}ms", "网络延迟": "200ms"}))

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    latest = service.latest("LATE-0002")
    versions = [item["snapshot"]["version"] for item in results]
    check("并发采样串行发号", versions == list(range(1, 17)), str(sorted(versions)))
    check("只保留最新一个版本", len(service.list_latest()) == 1)
    check("最新版本号最大", latest["version"] == max(versions))
    check("最新快照自身对账一致", latest["segment_sum_ms"] == latest["end_to_end_ms"])


def test_history_without_segments() -> None:
    latency_segment_service.reset()
    result = latency_segment_service.snapshot_record(4)
    snapshot = result["snapshot"]
    check("历史记录不丢", result["ok"] and snapshot["记录编号"] == "LATE-0004")
    check("历史记录保留总耗时", snapshot["end_to_end_ms"] == 900 and snapshot["degraded"] is True)
    check("历史记录不臆造分段归因", snapshot["over_threshold_segment"] is None)
    check("历史记录仍保留超标差值", snapshot["over_threshold_ms"] == 100)


def test_cross_module_single_version() -> None:
    latency_segment_service.reset()
    latency_segment_service.snapshot_record(2)
    metric = MetricService()
    result = metric.latency_result(2)
    check("指标采集读到同一份结果", result is not None and result["version_label"] == "LATE-0002@v1")
    check("指标采集沿用相同版本", result["metric_version"] == METRIC_COLLECTION_VERSION)
    check("指标采集不另算结论", result["over_threshold_ms"] == 120)
    check("无快照时不臆造结论", metric.latency_result(1) is None)


def test_validation() -> None:
    latency_segment_service.reset()
    invalid = latency_segment_service.snapshot_record(1, {"端到端延迟": "abc"})
    check("非法耗时被拦下", invalid["kind"] == "validation", invalid["message"])
    check("阈值必须为正", latency_segment_service.snapshot_record(1, {"延迟阈值": "0ms"})["kind"] == "validation")
    missing = latency_segment_service.snapshot_record(99)
    check("不存在的记录明确报错", missing["kind"] == "not_found")
    for raw, expected in [("920", 920), ("920ms", 920), ("0.92s", 920), ("920 毫秒", 920)]:
        check(f"解析 {raw}", parse_duration_ms(raw) == expected, str(parse_duration_ms(raw)))


def main() -> int:
    latency_segment_service.reset()
    test_acceptance_late_0002()
    test_idempotent_write()
    test_reconcile_conflict_rejects_write()
    test_concurrent_latest_only()
    test_history_without_segments()
    test_cross_module_single_version()
    test_validation()
    latency_segment_service.reset()
    if FAILURES:
        print(f"\n{len(FAILURES)} 项失败：{FAILURES}")
        return 1
    print("\n全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
