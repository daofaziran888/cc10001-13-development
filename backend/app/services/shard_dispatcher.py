"""分片分发调度：把已校验的结果批次按通道拆成可恢复的分片，并追踪每个分片的完成位置。

数据流：result_store -> export_hub -> shard_dispatches -> dispatch_logs

核心约束：
- 同一批次、通道、分片号只保留一条有效传输结果，重复登记复用原记录；
- 已完成的分片不再发送，续传只从失败/待续传的位置继续；
- 重复归档是幂等的，只记录一次“分片完成”证据；
- 只有整条通道的分片全部完成，才允许生成该通道的完整文件；
- 部分成功时由调用方选择立即发布成功通道或暂存到所有通道完成，任何策略都不暴露半成品。
"""
from __future__ import annotations

import hashlib
import json
import math
import time
from typing import Any

from app.store import store
from app.services.export_hub import ExportHubService

RESULT_MODULE = "result_store"
CHANNEL_MODULE = "export_hub"

SHARD_STATUS_PENDING = "待续传"
SHARD_STATUS_RUNNING = "传输中"
SHARD_STATUS_DONE = "已完成"
SHARD_STATUS_FAILED = "传输失败"
SHARD_STATUS_VOID = "已作废"
SHARD_STATUSES = [SHARD_STATUS_PENDING, SHARD_STATUS_RUNNING, SHARD_STATUS_DONE,
                  SHARD_STATUS_FAILED, SHARD_STATUS_VOID]

BATCH_STATUS_PENDING = "待分发"
BATCH_STATUS_RUNNING = "传输中"
BATCH_STATUS_PARTIAL = "部分成功"
BATCH_STATUS_DONE = "已完成"
BATCH_STATUS_INTERRUPTED = "已中断"
BATCH_STATUSES = [BATCH_STATUS_PENDING, BATCH_STATUS_RUNNING, BATCH_STATUS_PARTIAL,
                  BATCH_STATUS_DONE, BATCH_STATUS_INTERRUPTED]

STRATEGY_PUBLISH = "publish_success"
STRATEGY_STAGE = "stage_until_all"
STRATEGIES = [STRATEGY_PUBLISH, STRATEGY_STAGE]
STRATEGY_ALIASES = {
    "立即发布成功通道": STRATEGY_PUBLISH,
    "立即发布": STRATEGY_PUBLISH,
    "publish": STRATEGY_PUBLISH,
    "publish_success": STRATEGY_PUBLISH,
    "暂存至全部完成": STRATEGY_STAGE,
    "暂存": STRATEGY_STAGE,
    "stage": STRATEGY_STAGE,
    "stage_until_all": STRATEGY_STAGE,
}

SHARD_FIELDS = ["批次编号", "通道编号", "分片编号", "目标地址", "分片状态", "尝试次数", "完成时间"]


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _as_int(value: Any, default: int = 0) -> int:
    """只接受完整的数字文本，示例数据里的中文占位不会被误解析。"""
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value if value is not None else "").strip()
    if not text:
        return default
    try:
        return int(text)
    except (TypeError, ValueError):
        try:
            return int(float(text))
        except (TypeError, ValueError):
            return default


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((_as_int(row.get("id"), 0) for row in rows), default=0) + 1


def _normalise_numbers(values: Any) -> set[int]:
    if values is None:
        return set()
    if isinstance(values, (int, float, str)):
        values = [values]
    numbers: set[int] = set()
    for value in values:  # type: ignore[union-attr]
        number = _as_int(value, -1)
        if number > 0:
            numbers.add(number)
    return numbers


class ShardDispatcher:
    def __init__(self) -> None:
        self._hub = ExportHubService()

    # ------------------------------------------------------------------ 存储访问
    def _records(self) -> list[dict[str, Any]]:
        return store.joint_rows("shard_dispatches")

    def _events(self) -> list[dict[str, Any]]:
        return store.joint_rows("shard_events")

    def _batch_states(self) -> list[dict[str, Any]]:
        return store.joint_rows("shard_batches")

    def _packages(self) -> list[dict[str, Any]]:
        return store.joint_rows("shard_packages")

    def _save(self, key: str) -> None:
        store.joint_save(key)

    # ------------------------------------------------------------------ 基础信息
    def _batch(self, batch_id: int) -> dict[str, Any] | None:
        return store.find(RESULT_MODULE, batch_id)

    def _channel(self, channel_id: int) -> dict[str, Any] | None:
        return store.find(CHANNEL_MODULE, channel_id)

    def _resolve_channel(self, value: Any) -> dict[str, Any] | None:
        if isinstance(value, dict):
            return value
        channel_id = _as_int(value, -1)
        if channel_id > 0:
            found = self._channel(channel_id)
            if found is not None:
                return found
        text = str(value or "").strip()
        if text:
            for row in store.rows(CHANNEL_MODULE):
                if str(row.get("通道编号", "")).strip() == text:
                    return row
        return None

    @staticmethod
    def _batch_no(batch: dict[str, Any], batch_id: int) -> str:
        return str(batch.get("批次编号") or f"BATCH-{batch_id}")

    @staticmethod
    def _channel_no(channel: dict[str, Any], channel_id: int) -> str:
        return str(channel.get("通道编号") or channel.get("编号") or f"CHANNEL-{channel_id}")

    @staticmethod
    def _target_for(channel: dict[str, Any], batch_no: str, shard_no: int, total: int,
                    fmt: str) -> str:
        base = str(channel.get("目标地址") or "").rstrip("/")
        if not base:
            base = f"export://{channel.get('通道编号', 'channel')}"
        suffix = f"{batch_no}/shard-{shard_no:03d}-of-{total:03d}.{fmt}"
        return f"{base}/{suffix}"

    def _total_rows(self, batch: dict[str, Any], batch_id: int) -> int:
        for field in ("有效行数", "原始行数"):
            value = _as_int(batch.get(field), 0)
            if value > 0:
                return value
        # 示例数据的占位文本没有行数时，给一个稳定的可复现规模。
        return max(1, batch_id * 20)

    def _shard_size(self, channel: dict[str, Any], override: Any = None) -> int:
        size = _as_int(override, 0)
        if size <= 0:
            size = _as_int(channel.get("分片大小"), 0)
        return size if size > 0 else 1000

    # ------------------------------------------------------------------ 事件与清单
    def _append_event(self, record: dict[str, Any], kind: str, note: str = "",
                      *, once: bool = False) -> dict[str, Any] | None:
        record_id = record.get("id")
        if once:
            for event in self._events():
                if (event.get("record_id") == record_id
                        and event.get("channel_id") == record.get("channel_id")
                        and event.get("kind") == kind):
                    return None
        event = {
            "id": _next_id(self._events()),
            "batch_id": record.get("batch_id"),
            "批次编号": record.get("批次编号"),
            "channel_id": record.get("channel_id"),
            "通道编号": record.get("通道编号"),
            "record_id": record_id,
            "shard_no": record.get("shard_no"),
            "分片编号": record.get("分片编号"),
            "target": record.get("target"),
            "kind": kind,
            "状态": kind,
            "note": note,
            "created_at": _now(),
        }
        store.joint_append("shard_events", event)
        return event

    def _manifest(self, batch: dict[str, Any], channel: dict[str, Any]) -> dict[str, Any]:
        batch_id = _as_int(batch.get("id"), 0)
        channel_id = _as_int(channel.get("id"), 0)
        for log in store.joint_rows("dispatch_logs"):
            if _as_int(log.get("batch_id"), -1) != batch_id:
                continue
            log_channel = log.get("channel_id", log.get("export_entry_id"))
            if _as_int(log_channel, -1) == channel_id:
                return log
        log = {
            "id": _next_id(store.joint_rows("dispatch_logs")),
            "batch_id": batch_id,
            "export_entry_id": channel_id,
            "channel_id": channel_id,
            "batch_no": self._batch_no(batch, batch_id),
            "channel_no": self._channel_no(channel, channel_id),
            "channel_format": str(channel.get("输出格式", "json")),
            "target": str(channel.get("目标地址", "")),
            "dispatched_at": _now(),
            "status": BATCH_STATUS_PENDING,
            "mode": "shard",
        }
        store.joint_append("dispatch_logs", log)
        return log

    def _sync_manifest(self, batch_id: int) -> list[dict[str, Any]]:
        """把分片完成情况回写到分发记录清单，并保留通道级证据。"""
        recs = [r for r in self._records()
                if r.get("batch_id") == batch_id and r.get("valid", True)]
        by_channel: dict[int, list[dict[str, Any]]] = {}
        for record in recs:
            by_channel.setdefault(_as_int(record.get("channel_id"), -1), []).append(record)
        logs = []
        for channel_id, channel_recs in by_channel.items():
            channel = self._channel(channel_id)
            if channel is None:
                continue
            batch = self._batch(batch_id)
            if batch is None:
                continue
            log = self._manifest(batch, channel)
            completed = [r for r in channel_recs if r.get("status") == SHARD_STATUS_DONE]
            failed = [r for r in channel_recs if r.get("status") == SHARD_STATUS_FAILED]
            pending = [r for r in channel_recs if r.get("status") == SHARD_STATUS_PENDING]
            resume = sorted(_as_int(r.get("shard_no"), 0) for r in failed + pending)
            evidence = [
                e.get("id") for e in self._events()
                if e.get("batch_id") == batch_id and e.get("channel_id") == channel_id
            ]
            log.update({
                "batch_id": batch_id,
                "export_entry_id": channel_id,
                "channel_id": channel_id,
                "batch_no": self._batch_no(batch, batch_id),
                "channel_no": self._channel_no(channel, channel_id),
                "channel_format": str(channel.get("输出格式", "json")),
                "target": str(channel.get("目标地址", "")),
                "shard_total": len(channel_recs),
                "shard_completed": len(completed),
                "shard_pending": len(pending),
                "shard_failed": len(failed),
                "resume_from": resume[0] if resume else None,
                "last_completed_shard": max(
                    (_as_int(r.get("shard_no"), 0) for r in completed), default=None),
                "evidence_ids": evidence,
                "updated_at": _now(),
            })
            if len(completed) == len(channel_recs):
                log["status"] = "已完成"
            elif completed:
                log["status"] = "部分成功"
            elif failed:
                log["status"] = "传输失败"
            else:
                log["status"] = "已启用"
            logs.append(log)
        self._save("dispatch_logs")
        return logs

    # ------------------------------------------------------------------ 批次状态
    def _refresh_batch(self, batch_id: int, *, strategy: str | None = None) -> dict[str, Any]:
        batch = self._batch(batch_id)
        batch_no = self._batch_no(batch, batch_id) if batch else f"BATCH-{batch_id}"
        recs = [r for r in self._records()
                if r.get("batch_id") == batch_id and r.get("valid", True)]
        channels: list[dict[str, Any]] = []
        grouped: dict[int, list[dict[str, Any]]] = {}
        for record in recs:
            grouped.setdefault(_as_int(record.get("channel_id"), -1), []).append(record)

        for channel_id, channel_recs in grouped.items():
            channel = self._channel(channel_id)
            channel_no = (self._channel_no(channel, channel_id) if channel
                          else f"CHANNEL-{channel_id}")
            done = [r for r in channel_recs if r.get("status") == SHARD_STATUS_DONE]
            failed = [r for r in channel_recs if r.get("status") == SHARD_STATUS_FAILED]
            pending = [r for r in channel_recs if r.get("status") == SHARD_STATUS_PENDING]
            if len(done) == len(channel_recs):
                status = "已完成"
            elif done and (failed or pending):
                status = BATCH_STATUS_PARTIAL
            elif failed and not done:
                status = BATCH_STATUS_INTERRUPTED
            else:
                status = "待续传"
            resume = sorted(_as_int(r.get("shard_no"), 0) for r in failed + pending)
            channels.append({
                "channel_id": channel_id,
                "通道编号": channel_no,
                "target": str(channel.get("目标地址", "")) if channel else "",
                "total": len(channel_recs),
                "completed": len(done),
                "pending": len(pending),
                "failed": len(failed),
                "status": status,
                "resume_from": resume[0] if resume else None,
                "resume_shards": resume,
            })

        total = len(recs)
        completed = sum(1 for r in recs if r.get("status") == SHARD_STATUS_DONE)
        failed = sum(1 for r in recs if r.get("status") == SHARD_STATUS_FAILED)
        pending = sum(1 for r in recs if r.get("status") == SHARD_STATUS_PENDING)
        if total == 0:
            status = BATCH_STATUS_PENDING
        elif completed == total:
            status = BATCH_STATUS_DONE
        elif completed:
            status = BATCH_STATUS_PARTIAL
        elif failed:
            status = BATCH_STATUS_INTERRUPTED
        else:
            status = BATCH_STATUS_PENDING

        resume_shards = sorted({
            _as_int(r.get("shard_no"), 0) for r in recs
            if r.get("status") in (SHARD_STATUS_PENDING, SHARD_STATUS_FAILED)})
        success_channels = [c for c in channels if c["status"] == "已完成"]
        incomplete_channels = [c for c in channels if c["status"] != "已完成"]
        state = None
        for row in self._batch_states():
            if _as_int(row.get("batch_id"), -1) == batch_id:
                state = row
                break
        if state is None:
            state = {"id": _next_id(self._batch_states()), "batch_id": batch_id}
            store.joint_append("shard_batches", state)
        stored_strategy = (strategy or state.get("strategy") or STRATEGY_PUBLISH)
        stored_strategy = STRATEGY_ALIASES.get(stored_strategy, stored_strategy)
        if stored_strategy not in STRATEGIES:
            stored_strategy = STRATEGY_PUBLISH
        state.update({
            "batch_id": batch_id,
            "批次编号": batch_no,
            "status": status,
            "分发状态": status,
            "strategy": stored_strategy,
            "策略": stored_strategy,
            "total_shards": total,
            "completed_shards": completed,
            "pending_shards": pending,
            "failed_shards": failed,
            "resume_shards": resume_shards,
            "resume_from": resume_shards[0] if resume_shards else None,
            "partial_success": bool(success_channels and incomplete_channels),
            "channels": channels,
            "updated_at": _now(),
        })
        self._save("shard_batches")
        self._sync_manifest(batch_id)
        self._write_back_batch(batch, state)
        return state

    def _write_back_batch(self, batch: dict[str, Any] | None,
                          state: dict[str, Any]) -> None:
        """批次分发状态回写采集仓储；只有全部分片完成才把批次标为已分发。"""
        if batch is None:
            return
        batch["分发状态"] = state.get("status")
        batch["分发进度"] = (
            f"{state.get('completed_shards', 0)}/{state.get('total_shards', 0)}")
        batch["续传位置"] = state.get("resume_from")
        if state.get("status") == BATCH_STATUS_DONE:
            batch["status"] = "已分发"
            batch["pending"] = False
            batch["abnormal"] = False
        elif state.get("status") in (BATCH_STATUS_PARTIAL, BATCH_STATUS_INTERRUPTED):
            batch["pending"] = True
            batch["abnormal"] = state.get("status") == BATCH_STATUS_INTERRUPTED

    def get_batch_state(self, batch_id: int) -> dict[str, Any] | None:
        for row in self._batch_states():
            if _as_int(row.get("batch_id"), -1) == batch_id:
                return row
        return None

    # ------------------------------------------------------------------ 生成分片
    def prepare(self, batch_id: int, channel_ids: Any = None, *,
                shard_size: Any = None, strategy: str | None = None) -> dict[str, Any]:
        batch = self._batch(batch_id)
        if batch is None:
            return {"ok": False, "error": f"结果批次 {batch_id} 不存在"}
        status = str(batch.get("status") or "")
        if status not in ("校验通过", "已分发"):
            return {"ok": False, "error": f"批次 {batch_id} 状态为「{status}」，只有校验通过的批次才能分发"}

        requested: list[dict[str, Any]] = []
        if channel_ids:
            if isinstance(channel_ids, (int, str, dict)):
                channel_ids = [channel_ids]
            for value in channel_ids:  # type: ignore[union-attr]
                channel = self._resolve_channel(value)
                if channel is None:
                    return {"ok": False, "error": f"分发通道 {value} 不存在或已归档"}
                if str(channel.get("status")) == "已停用":
                    return {"ok": False, "error": f"分发通道 {channel.get('通道编号')} 已停用，不能分发"}
                if channel not in requested:
                    requested.append(channel)
        else:
            requested = [row for row in store.rows(CHANNEL_MODULE)
                         if str(row.get("status")) in ("已启用", "传输中")]
        if not requested:
            return {"ok": False, "error": "没有可用的分发通道，请先启用至少一个通道"}

        batch_no = self._batch_no(batch, batch_id)
        total_rows = self._total_rows(batch, batch_id)
        created = 0
        reused = 0
        channel_summaries: list[dict[str, Any]] = []
        for channel in requested:
            channel_id = _as_int(channel.get("id"), 0)
            channel_no = self._channel_no(channel, channel_id)
            size = self._shard_size(channel, shard_size)
            shard_count = max(1, math.ceil(total_rows / size))
            fmt = str(channel.get("输出格式") or "json")
            for shard_no in range(1, shard_count + 1):
                existing = None
                for record in self._records():
                    if (record.get("batch_id") == batch_id
                            and _as_int(record.get("channel_id"), -1) == channel_id
                            and _as_int(record.get("shard_no"), -1) == shard_no
                            and record.get("valid", True)):
                        existing = record
                        break
                rows_in_shard = min(size, total_rows - (shard_no - 1) * size)
                if existing is not None:
                    # 复用已有进度：已完成的分片绝不重置，待续传的保留失败位置。
                    existing.update({
                        "批次编号": batch_no,
                        "通道编号": channel_no,
                        "分片编号": f"{channel_no}-S{shard_no:03d}",
                        "目标地址": existing.get("target")
                        or self._target_for(channel, batch_no, shard_no, shard_count, fmt),
                        "行数": rows_in_shard,
                        "total_shards": shard_count,
                    })
                    reused += 1
                else:
                    target = self._target_for(channel, batch_no, shard_no, shard_count, fmt)
                    record = {
                        "id": _next_id(self._records()),
                        "batch_id": batch_id,
                        "批次编号": batch_no,
                        "channel_id": channel_id,
                        "通道编号": channel_no,
                        "shard_no": shard_no,
                        "分片编号": f"{channel_no}-S{shard_no:03d}",
                        "target": target,
                        "目标地址": target,
                        "输出格式": fmt,
                        "status": SHARD_STATUS_PENDING,
                        "分片状态": SHARD_STATUS_PENDING,
                        "attempts": 0,
                        "行数": rows_in_shard,
                        "total_shards": shard_count,
                        "valid": True,
                        "archived": False,
                        "checksum": hashlib.sha256(
                            f"{batch_id}:{channel_id}:{shard_no}:{target}".encode()
                        ).hexdigest()[:16],
                        "created_at": _now(),
                        "updated_at": _now(),
                        "completed_at": None,
                        "package_id": None,
                    }
                    store.joint_append("shard_dispatches", record)
                    self._append_event(record, "分片登记", "生成可恢复分片分发记录")
                    created += 1
            self._dedupe(batch_id, channel_id)
            self._manifest(batch, channel)
            channel_summaries.append({
                "channel_id": channel_id,
                "通道编号": channel_no,
                "total_shards": shard_count,
                "rows": total_rows,
                "shard_size": size,
            })

        state = self._refresh_batch(
            batch_id, strategy=STRATEGY_ALIASES.get(str(strategy or ""), strategy))
        return {
            "ok": True,
            "batch_id": batch_id,
            "批次编号": batch_no,
            "created": created,
            "reused": reused,
            "channels": channel_summaries,
            "batch_state": state,
            "message": "分片分发记录已生成" if created else "分片分发记录已存在，沿用已有进度",
        }

    def _dedupe(self, batch_id: int, channel_id: int) -> None:
        """同一分片在同一通道只允许一条有效传输结果。"""
        seen: dict[int, dict[str, Any]] = {}
        changed = False
        for record in self._records():
            if (record.get("batch_id") != batch_id
                    or _as_int(record.get("channel_id"), -1) != channel_id
                    or not record.get("valid", True)):
                continue
            shard_no = _as_int(record.get("shard_no"), -1)
            if shard_no in seen:
                # 已完成优先保留；否则保留更早登记的那条。
                keeper = seen[shard_no]
                victim = record
                if (record.get("status") == SHARD_STATUS_DONE
                        and keeper.get("status") != SHARD_STATUS_DONE):
                    keeper, victim = record, keeper
                    seen[shard_no] = keeper
                victim["valid"] = False
                victim["status"] = SHARD_STATUS_VOID
                victim["分片状态"] = SHARD_STATUS_VOID
                victim["updated_at"] = _now()
                changed = True
            else:
                seen[shard_no] = record
        if changed:
            self._save("shard_dispatches")

    # ------------------------------------------------------------------ 传输与续传
    def _complete_record(self, record: dict[str, Any], source: str) -> bool:
        """完成一个分片；重复完成/归档是幂等的，只留一次完成证据。"""
        if record.get("status") == SHARD_STATUS_DONE and record.get("archived"):
            # 重复归档幂等：不增加尝试次数，也不再产生完成证据。
            return False
        record["attempts"] = _as_int(record.get("attempts"), 0) + 1
        record["updated_at"] = _now()
        record["status"] = SHARD_STATUS_DONE
        record["分片状态"] = SHARD_STATUS_DONE
        record["completed_at"] = record.get("completed_at") or _now()
        record["archived"] = True
        record["last_source"] = source
        self._append_event(record, "分片完成", source, once=True)
        return True

    def _fail_record(self, record: dict[str, Any], note: str) -> None:
        record["attempts"] = _as_int(record.get("attempts"), 0) + 1
        record["status"] = SHARD_STATUS_FAILED
        record["分片状态"] = SHARD_STATUS_FAILED
        record["updated_at"] = _now()
        record["archived"] = False
        self._append_event(record, "传输失败", note)

    def _channel_records(self, batch_id: int, channel_id: int) -> list[dict[str, Any]]:
        return sorted(
            [r for r in self._records()
             if r.get("batch_id") == batch_id
             and _as_int(r.get("channel_id"), -1) == channel_id
             and r.get("valid", True)],
            key=lambda r: _as_int(r.get("shard_no"), 0),
        )

    def transfer(self, batch_id: int, channel_id: Any, *,
                 fail_shards: Any = None, interrupt_at: Any = None,
                 fail_after: Any = None, fail_all: bool = False) -> dict[str, Any]:
        channel = self._resolve_channel(channel_id)
        if channel is None:
            return {"ok": False, "error": f"分发通道 {channel_id} 不存在或已归档"}
        channel_id = _as_int(channel.get("id"), 0)
        self._dedupe(batch_id, channel_id)
        records = self._channel_records(batch_id, channel_id)
        if not records:
            prepared = self.prepare(batch_id, [channel], strategy=None)
            if not prepared.get("ok"):
                return prepared
            records = self._channel_records(batch_id, channel_id)

        completed_before = [r for r in records if r.get("status") == SHARD_STATUS_DONE]
        pending = [r for r in records
                   if r.get("status") in (SHARD_STATUS_PENDING, SHARD_STATUS_FAILED)]
        fail_set = _normalise_numbers(fail_shards)
        interrupt_no = _as_int(interrupt_at, 0)
        after = _as_int(fail_after, -1)
        sent: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        interrupted = False

        for index, record in enumerate(pending):
            shard_no = _as_int(record.get("shard_no"), 0)
            should_fail = (
                fail_all
                or shard_no in fail_set
                or (interrupt_no > 0 and shard_no >= interrupt_no)
                or (after >= 0 and index >= after)
            )
            if should_fail:
                note = "通道中断，保留已完成分片" if interrupt_no else "模拟传输失败"
                if record.get("status") != SHARD_STATUS_FAILED:
                    self._fail_record(record, note)
                failed.append(self._brief(record))
                if interrupt_no > 0:
                    interrupted = True
                    break
                if after >= 0:
                    interrupted = True
                    break
                continue
            self._complete_record(record, "传输")
            sent.append(self._brief(record))

        # 已完成的分片不会被重新发送。
        skipped = [self._brief(r) for r in completed_before]
        self._save("shard_dispatches")
        state = self._refresh_batch(batch_id)
        summary = self._channel_summary(batch_id, channel_id)
        return {
            "ok": True,
            "batch_id": batch_id,
            "channel_id": channel_id,
            "通道编号": self._channel_no(channel, channel_id),
            "transferred": len(sent),
            "failed": len(failed),
            "pending": summary["pending"],
            "skipped_completed": len(skipped),
            "sent_shards": [r["分片编号"] for r in sent],
            "failed_shards": [r["分片编号"] for r in failed],
            "skipped_shards": [r["分片编号"] for r in skipped],
            "resume_from": summary["resume_from"],
            "interrupted": interrupted,
            "channel_status": summary["status"],
            "batch_status": state["status"],
            "partial_success": state["partial_success"],
            "batch_state": state,
            "message": "通道传输完成" if summary["status"] == "已完成" else "通道未全部完成，可从失败位置续传",
        }

    def resume(self, batch_id: int, channel_id: Any = None, *,
               fail_shards: Any = None, fail_after: Any = None,
               fail_all: bool = False) -> dict[str, Any]:
        channels: list[dict[str, Any]] = []
        if channel_id is not None:
            channel = self._resolve_channel(channel_id)
            if channel is None:
                return {"ok": False, "error": f"分发通道 {channel_id} 不存在或已归档"}
            channels = [channel]
        else:
            seen: set[int] = set()
            for record in self._records():
                if (record.get("batch_id") == batch_id
                        and record.get("valid", True)
                        and record.get("status") in (SHARD_STATUS_PENDING, SHARD_STATUS_FAILED)):
                    cid = _as_int(record.get("channel_id"), -1)
                    channel = self._channel(cid)
                    if channel is not None and cid not in seen:
                        seen.add(cid)
                        channels.append(channel)
        if not channels:
            return {"ok": True, "resumed": 0, "channels": [],
                    "message": "没有待续传的分片"}

        results = []
        for channel in channels:
            result = self.transfer(
                batch_id, channel, fail_shards=fail_shards,
                fail_after=fail_after, fail_all=fail_all)
            results.append(result)
        state = self._refresh_batch(batch_id)
        total_sent = sum(int(r.get("transferred", 0)) for r in results)
        total_skipped = sum(int(r.get("skipped_completed", 0)) for r in results)
        return {
            "ok": True,
            "resumed": total_sent,
            "skipped_completed": total_skipped,
            "channels": results,
            "batch_status": state["status"],
            "resume_from": state.get("resume_from"),
            "partial_success": state["partial_success"],
            "message": "续传完成" if state["status"] == BATCH_STATUS_DONE else "已从失败位置继续，仍有待续传分片",
        }

    @staticmethod
    def _brief(record: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": record.get("id"),
            "分片编号": record.get("分片编号"),
            "shard_no": record.get("shard_no"),
            "status": record.get("status"),
            "分片状态": record.get("分片状态"),
            "target": record.get("target"),
        }

    def _channel_summary(self, batch_id: int, channel_id: int) -> dict[str, Any]:
        state = self._refresh_batch(batch_id)
        for channel in state.get("channels", []):
            if channel.get("channel_id") == channel_id:
                return channel
        return {"channel_id": channel_id, "pending": 0, "failed": 0,
                "completed": 0, "total": 0, "status": "待续传", "resume_from": None}

    # ------------------------------------------------------------------ 单分片动作
    def get_record(self, record_id: int) -> dict[str, Any] | None:
        for record in self._records():
            if _as_int(record.get("id"), -1) == record_id:
                return record
        return None

    def run_record_action(self, record_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        record = self.get_record(record_id)
        if record is None:
            return None, f"分片分发记录 {record_id} 不存在或已归档"
        if action in ("完成分片", "归档分片", "标记完成"):
            if record.get("status") == SHARD_STATUS_DONE:
                # 重复归档幂等：不再追加完成事件。
                self._refresh_batch(_as_int(record.get("batch_id"), 0))
                return record, "该分片已完成，重复归档不重复记完成"
            self._complete_record(record, action)
            self._save("shard_dispatches")
            self._refresh_batch(_as_int(record.get("batch_id"), 0))
            return record, "分片已完成并归档"
        if action in ("标记失败", "中断分片"):
            if record.get("status") == SHARD_STATUS_DONE:
                return None, "已完成的分片不能被标记失败"
            self._fail_record(record, action)
            self._save("shard_dispatches")
            self._refresh_batch(_as_int(record.get("batch_id"), 0))
            return record, "分片已标记为传输失败"
        if action in ("续传", "续传分片", "恢复传输"):
            if record.get("status") == SHARD_STATUS_DONE:
                return record, "该分片已确认完成，续传不会重复发送"
            batch_id = _as_int(record.get("batch_id"), 0)
            channel_id = _as_int(record.get("channel_id"), -1)
            result = self.transfer(batch_id, channel_id)
            if not result.get("ok"):
                return None, str(result.get("error") or "续传失败")
            self._save("shard_dispatches")
            return self.get_record(record_id), "分片已续传"
        return None, f"动作「{action}」不属于分片分发可执行范围"

    # ------------------------------------------------------------------ 打包与文件
    @staticmethod
    def _strategy(value: Any) -> str | None:
        text = str(value or "").strip()
        if not text:
            return None
        return STRATEGY_ALIASES.get(text, text if text in STRATEGIES else None)

    def _package_content(self, batch: dict[str, Any], channel: dict[str, Any],
                         records: list[dict[str, Any]]) -> str:
        fmt = str(channel.get("输出格式") or "json").lower()
        payload = {
            "批次编号": self._batch_no(batch, _as_int(batch.get("id"), 0)),
            "通道编号": self._channel_no(channel, _as_int(channel.get("id"), 0)),
            "目标地址": str(channel.get("目标地址", "")),
            "分片总数": len(records),
            "分片": [
                {
                    "分片编号": r.get("分片编号"),
                    "目标地址": r.get("target"),
                    "行数": r.get("行数"),
                    "校验摘要": r.get("checksum"),
                }
                for r in sorted(records, key=lambda x: _as_int(x.get("shard_no"), 0))
            ],
        }
        if fmt == "csv":
            lines = ["分片编号,目标地址,行数,校验摘要"]
            for shard in payload["分片"]:
                lines.append(
                    f"{shard['分片编号']},{shard['目标地址']},{shard['行数']},{shard['校验摘要']}")
            return "﻿" + "\n".join(lines) + "\n"
        return json.dumps(payload, ensure_ascii=False, indent=2)

    def package(self, batch_id: int, channel_id: Any = None, *,
                strategy: Any = None) -> dict[str, Any]:
        batch = self._batch(batch_id)
        if batch is None:
            return {"ok": False, "error": f"结果批次 {batch_id} 不存在"}
        requested_strategy = self._strategy(strategy)
        if strategy and requested_strategy is None:
            return {"ok": False, "error": f"未知的部分成功策略：{strategy}"}
        state = self._refresh_batch(batch_id, strategy=requested_strategy)
        current_strategy = state.get("strategy", STRATEGY_PUBLISH)

        selected: list[dict[str, Any]] = []
        if channel_id is not None:
            channel = self._resolve_channel(channel_id)
            if channel is None:
                return {"ok": False, "error": f"分发通道 {channel_id} 不存在或已归档"}
            selected = [channel]
        else:
            seen: set[int] = set()
            for record in self._records():
                if record.get("batch_id") == batch_id and record.get("valid", True):
                    cid = _as_int(record.get("channel_id"), -1)
                    channel = self._channel(cid)
                    if channel is not None and cid not in seen:
                        seen.add(cid)
                        selected.append(channel)
        if not selected:
            return {"ok": False, "error": "该批次尚未生成分片分发记录，请先生成分片"}

        all_channel_ids = {
            _as_int(r.get("channel_id"), -1) for r in self._records()
            if r.get("batch_id") == batch_id and r.get("valid", True)
        }
        all_complete = all(
            self._channel_complete(batch_id, cid) for cid in all_channel_ids)

        packages: list[dict[str, Any]] = []
        blocked: list[dict[str, Any]] = []
        for channel in selected:
            cid = _as_int(channel.get("id"), 0)
            records = self._channel_records(batch_id, cid)
            if not records or not self._channel_complete(batch_id, cid):
                pending = [r for r in records if r.get("status") != SHARD_STATUS_DONE]
                blocked.append({
                    "channel_id": cid,
                    "通道编号": self._channel_no(channel, cid),
                    "reason": "通道仍有未完成分片，不能打包半成品",
                    "pending_shards": [r.get("分片编号") for r in pending],
                    "resume_from": min(
                        (_as_int(r.get("shard_no"), 0) for r in pending), default=None),
                })
                continue
            package = None
            for row in self._packages():
                if (row.get("batch_id") == batch_id
                        and _as_int(row.get("channel_id"), -1) == cid):
                    package = row
                    break
            publish = all_complete or current_strategy == STRATEGY_PUBLISH
            new_status = "已发布" if publish else "暂存"
            if package is None:
                content = self._package_content(batch, channel, records)
                package = {
                    "id": _next_id(self._packages()),
                    "batch_id": batch_id,
                    "批次编号": self._batch_no(batch, batch_id),
                    "channel_id": cid,
                    "通道编号": self._channel_no(channel, cid),
                    "filename": self._filename(batch, channel),
                    "content": content,
                    "size_bytes": len(content.encode("utf-8")),
                    "checksum": hashlib.sha256(content.encode("utf-8")).hexdigest()[:16],
                    "shard_ids": [r.get("id") for r in records],
                    "shard_total": len(records),
                    "status": new_status,
                    "created_at": _now(),
                    "published_at": _now() if publish else None,
                }
                store.joint_append("shard_packages", package)
                self._append_event(
                    {"id": _next_id(self._events()), "batch_id": batch_id, "record_id": None,
                     "批次编号": package["批次编号"], "channel_id": cid,
                     "通道编号": package["通道编号"], "target": package["filename"],
                     "分片编号": None},
                    "打包完成" if publish else "打包暂存",
                    f"{len(records)} 个分片全部完成", once=True)
            else:
                # 已发布的完整文件保持可用；其余按当前策略刷新。
                if package.get("status") != "已发布":
                    package["status"] = new_status
                    package["published_at"] = _now() if publish else package.get("published_at")
                elif all_complete:
                    package["status"] = "已发布"
            for record in records:
                record["package_id"] = package["id"]
                record["updated_at"] = _now()
            packages.append(package)

        self._save("shard_dispatches")
        self._save("shard_packages")
        state = self._refresh_batch(batch_id)
        available = [p for p in self._packages()
                     if p.get("batch_id") == batch_id
                     and p.get("status") == "已发布"
                     and self._channel_complete(batch_id, _as_int(p.get("channel_id"), -1))]
        return {
            "ok": True,
            "batch_id": batch_id,
            "strategy": state.get("strategy"),
            "all_channels_complete": all_complete,
            "packages": [self._public_package(p) for p in packages],
            "available_files": [self._public_package(p) for p in available],
            "blocked_channels": blocked,
            "batch_status": state["status"],
            "resume_from": state.get("resume_from"),
            "message": ("打包完成，完整文件已发布" if available
                        else "通道尚未全部完成，文件仅暂存，不对外暴露半成品"),
        }

    def _channel_complete(self, batch_id: int, channel_id: int) -> bool:
        records = self._channel_records(batch_id, channel_id)
        return bool(records) and all(r.get("status") == SHARD_STATUS_DONE for r in records)

    @staticmethod
    def _filename(batch: dict[str, Any], channel: dict[str, Any]) -> str:
        batch_no = str(batch.get("批次编号") or "batch")
        channel_no = str(channel.get("通道编号") or "channel")
        fmt = str(channel.get("输出格式") or "json").lower()
        safe = "".join(c if c.isalnum() or c in "-_." else "-" for c in f"{batch_no}-{channel_no}")
        return f"{safe}.{fmt}"

    @staticmethod
    def _public_package(package: dict[str, Any]) -> dict[str, Any]:
        public = {k: v for k, v in package.items() if k != "content"}
        public["downloadable"] = package.get("status") == "已发布"
        public["download_url"] = (
            f"/api/export_hub/shards/files/{package.get('id')}/download"
            if public["downloadable"] else None)
        return public

    def list_files(self, batch_id: int | None = None, channel_id: int | None = None,
                   available_only: bool = False) -> list[dict[str, Any]]:
        rows = list(self._packages())
        if batch_id is not None:
            rows = [r for r in rows if _as_int(r.get("batch_id"), -1) == batch_id]
        if channel_id is not None:
            rows = [r for r in rows if _as_int(r.get("channel_id"), -1) == channel_id]
        if available_only:
            rows = [r for r in rows if r.get("status") == "已发布"]
        return [self._public_package(r) for r in rows]

    def get_package(self, package_id: int) -> dict[str, Any] | None:
        for row in self._packages():
            if _as_int(row.get("id"), -1) == package_id:
                return row
        return None

    def get_download(self, package_id: int) -> tuple[dict[str, Any] | None, str | None]:
        package = self.get_package(package_id)
        if package is None:
            return None, f"分发文件 {package_id} 不存在"
        if package.get("status") != "已发布":
            return None, "该通道分片尚未全部完成，暂存文件不对外提供下载"
        if not self._channel_complete(
                _as_int(package.get("batch_id"), -1),
                _as_int(package.get("channel_id"), -1)):
            return None, "通道仍有未完成分片，禁止下载半成品"
        return package, None

    # ------------------------------------------------------------------ 查询
    def list_records(self, *, filters: dict[str, str] | None = None,
                     batch_id: int | None = None, channel_id: int | None = None,
                     status: str | None = None, page: int = 1,
                     size: int = 20) -> tuple[list[dict[str, Any]], int]:
        rows = [r for r in self._records() if r.get("valid", True)]
        if batch_id is not None:
            rows = [r for r in rows if _as_int(r.get("batch_id"), -1) == batch_id]
        if channel_id is not None:
            rows = [r for r in rows if _as_int(r.get("channel_id"), -1) == channel_id]
        for field, expected in (filters or {}).items():
            if field in ("批次编号", "通道编号", "分片编号", "目标地址"):
                rows = [r for r in rows
                        if expected.casefold() in str(r.get(field, "")).casefold()]
        if status:
            rows = [r for r in rows if r.get("status") == status]
        rows.sort(key=lambda r: (_as_int(r.get("batch_id"), 0),
                                 _as_int(r.get("channel_id"), 0),
                                 _as_int(r.get("shard_no"), 0)))
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def list_manifest(self, batch_id: int | None = None) -> list[dict[str, Any]]:
        rows = list(store.joint_rows("dispatch_logs"))
        if batch_id is not None:
            rows = [r for r in rows if _as_int(r.get("batch_id"), -1) == batch_id]
        return rows

    def list_events(self, batch_id: int | None = None) -> list[dict[str, Any]]:
        rows = list(self._events())
        if batch_id is not None:
            rows = [r for r in rows if _as_int(r.get("batch_id"), -1) == batch_id]
        return rows


shard_dispatcher = ShardDispatcher()
