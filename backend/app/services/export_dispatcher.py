"""分发调度器：读取采集仓储批次并生成分发记录。

数据流：result_store -> export_hub -> shard_dispatches
"""
from __future__ import annotations

import time
from typing import Any

from app.store import store
from app.services.export_hub import ExportHubService
from app.services.shard_dispatcher import shard_dispatcher

RESULT_MODULE = "result_store"


def _row_count(value: Any) -> int:
    """把“有效行数”这类文本转成正数；示例数据里的占位文本按 0 处理。"""
    try:
        return max(0, int(str(value or "").strip()))
    except (TypeError, ValueError):
        return 0


class ExportDispatcher:
    def __init__(self) -> None:
        self._export_svc = ExportHubService()

    def _next_log_id(self) -> int:
        rows = store.joint_rows("dispatch_logs")
        return max((int(r.get("id", 0)) for r in rows), default=0) + 1

    def dispatch_batch(self, batch_id: int, channel_format: str = "json") -> dict[str, Any]:
        """读取指定采集仓储批次，生成分发记录并写入数据分发通道。"""
        batch = store.find(RESULT_MODULE, batch_id)
        if batch is None:
            return {"ok": False, "error": f"结果批次 {batch_id} 不存在"}

        if batch.get("status") != "校验通过":
            return {"ok": False, "error": f"批次 {batch_id} 未通过校验，禁止分发"}

        # 已经生成过分片记录的批次不重复建通道，直接返回现有分发结果。
        for existing in store.joint_rows("dispatch_logs"):
            if (int(existing.get("batch_id", -1)) == batch_id
                    and existing.get("mode") == "shard"):
                return {"ok": True, "log": existing, "export_entry": None,
                        "message": "批次已生成分片分发记录，沿用原通道"}

        # 创建分发通道记录（跨模块数据产物）
        rows = max(1, _row_count(batch.get("有效行数")))
        export_values = {
            "通道编号": f"DISP-{batch_id}-{int(time.time())}",
            "输出格式": channel_format,
            "目标地址": f"s3://exports/{batch_id}.{channel_format}",
            "加密方式": "AES-256",
            "压缩选项": "gzip",
            "分片大小": str(max(1, rows // 4)),
            "重试上限": "3",
            "回调地址": f"https://callback.example.com/{batch_id}",
        }
        export_entry, missing = self._export_svc.create_entry(export_values)
        if export_entry is None:
            return {"ok": False, "error": f"分发记录写入失败：缺少 {missing}"}

        # 记录分发日志（联合存储）
        log = {
            "id": self._next_log_id(),
            "batch_id": batch_id,
            "export_entry_id": export_entry["id"],
            "channel_id": export_entry["id"],
            "batch_no": str(batch.get("批次编号", "")),
            "channel_no": export_values["通道编号"],
            "channel_format": channel_format,
            "target": export_values["目标地址"],
            "dispatched_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "已启用",
            "mode": "shard",
        }
        store.joint_append("dispatch_logs", log)

        # 按分片生成可恢复的分片分发记录；只有分片全部完成才把批次标为已分发。
        prepared = shard_dispatcher.prepare(batch_id, [export_entry["id"]])
        if not prepared.get("ok"):
            return {"ok": False, "error": prepared.get("error")}
        batch["pending"] = True

        return {
            "ok": True,
            "log": log,
            "export_entry": export_entry,
            "shards": prepared,
        }

    def dispatch_all_ready(self) -> dict[str, Any]:
        """对所有校验通过的批次执行自动分发。"""
        batches = [b for b in store.rows(RESULT_MODULE) if b.get("status") == "校验通过"]
        results: list[dict[str, Any]] = []
        for batch in batches:
            res = self.dispatch_batch(int(batch["id"]))
            results.append(res)
        succeeded = sum(1 for r in results if r.get("ok"))
        return {"ok": True, "dispatched": len(results), "succeeded": succeeded, "results": results}

    def list_logs(self) -> list[dict[str, Any]]:
        return list(store.joint_rows("dispatch_logs"))

    def get_log(self, log_id: int) -> dict[str, Any] | None:
        return store.joint_find("dispatch_logs", log_id)


export_dispatcher = ExportDispatcher()
