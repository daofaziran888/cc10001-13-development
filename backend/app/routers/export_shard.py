"""分片分发接口：按分片追踪分发完成位置，支持中断续传、打包、下载与导出。

路由统一挂在 /api/export_hub/shards 下，且必须在导出通道的 /{entry_id} 之前注册，
否则 “shards” 会被当成通道编号做整数转换。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response, StreamingResponse
from pydantic import BaseModel, Field

from app.schemas import ActionPayload, ActionResult, PageResult
from app.services.shard_dispatcher import (
    SHARD_FIELDS,
    SHARD_STATUSES,
    STRATEGIES,
    shard_dispatcher,
)

router = APIRouter(prefix="/api/export_hub/shards", tags=["分片分发"])

FILTER_FIELDS = ["批次编号", "通道编号", "分片编号", "目标地址"]


class PreparePayload(BaseModel):
    batch_id: int
    channel_ids: list[int | str] | None = None
    shard_size: int | None = None
    strategy: str | None = None


class TransferPayload(BaseModel):
    batch_id: int
    channel_id: int | str
    fail_shards: list[int] | None = None
    interrupt_at: int | None = None
    fail_after: int | None = None
    fail_all: bool = False


class ResumePayload(BaseModel):
    batch_id: int
    channel_id: int | str | None = None
    fail_shards: list[int] | None = None
    fail_after: int | None = None
    fail_all: bool = False


class PackagePayload(BaseModel):
    batch_id: int
    channel_id: int | str | None = None
    strategy: str | None = Field(default=None, description="publish_success 或 stage_until_all")


def query_filters(request: Request) -> dict[str, str]:
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in FILTER_FIELDS and value.strip()
    }


def _failure(result: dict[str, Any]) -> ActionResult:
    return ActionResult(ok=False, message=str(result.get("error") or "分片分发操作失败"))


def _row_for_export(item: dict[str, Any]) -> list[str]:
    values = [
        item.get("id"),
        item.get("批次编号", ""),
        item.get("通道编号", ""),
        item.get("分片编号", ""),
        item.get("目标地址", ""),
        item.get("分片状态", ""),
        item.get("尝试次数", ""),
        item.get("完成时间", ""),
    ]
    row: list[str] = []
    for value in values:
        text = "" if value is None else str(value)
        row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
    return row


@router.get("", response_model=PageResult[dict])
def list_shards(
    request: Request,
    batch_id: int | None = Query(default=None),
    channel_id: int | None = Query(default=None),
    status: str | None = Query(default=None, description="、".join(SHARD_STATUSES)),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=100, ge=1, le=1000),
) -> PageResult[dict]:
    """分片分发记录数据集：按批次、通道、分片号查看已完成与待续传的分片。"""
    items, total = shard_dispatcher.list_records(
        filters=query_filters(request), batch_id=batch_id,
        channel_id=channel_id, status=status, page=page, size=size)
    rows = []
    for item in items:
        row = dict(item)
        row["分片状态"] = item.get("status", row.get("分片状态"))
        row["尝试次数"] = item.get("attempts", 0)
        row["完成时间"] = item.get("completed_at") or ""
        rows.append(row)
    return PageResult(items=rows, total=total, page=page, size=size)


@router.get("/export")
def export_shards(
    request: Request,
    batch_id: int | None = Query(default=None),
    channel_id: int | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的分片分发清单；必须声明在 /{record_id} 之前。"""
    items, _ = shard_dispatcher.list_records(
        filters=query_filters(request), batch_id=batch_id,
        channel_id=channel_id, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("﻿")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *SHARD_FIELDS])
    for item in items:
        row = dict(item)
        row["分片状态"] = item.get("status")
        row["尝试次数"] = item.get("attempts", 0)
        row["完成时间"] = item.get("completed_at") or ""
        writer.writerow(_row_for_export(row))
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="export_hub_shards.csv"'})


@router.get("/meta/options")
def options() -> dict[str, Any]:
    """枚举值给页面下拉框使用；必须声明在 /{record_id} 之前。"""
    return {"statuses": SHARD_STATUSES, "strategies": STRATEGIES}


@router.get("/manifest")
def list_manifest(batch_id: int | None = Query(default=None)) -> dict[str, Any]:
    """分发记录清单：每个通道的分片完成数量、续传起点与证据编号。"""
    rows = shard_dispatcher.list_manifest(batch_id)
    return {"items": rows, "total": len(rows)}


@router.get("/files")
def list_files(
    batch_id: int | None = Query(default=None),
    channel_id: int | None = Query(default=None),
    available_only: bool = Query(default=False),
) -> dict[str, Any]:
    """已打包的分发文件；available_only 只返回可下载的完整文件。"""
    rows = shard_dispatcher.list_files(batch_id, channel_id, available_only)
    return {"items": rows, "total": len(rows)}


@router.get("/files/{package_id}/download")
def download_file(package_id: int) -> Response:
    """下载完整分发文件；通道未全部完成时拒绝，避免暴露半成品。"""
    package, error = shard_dispatcher.get_download(package_id)
    if package is None:
        raise HTTPException(status_code=409, detail=error)
    media = "text/csv; charset=utf-8" if str(package.get("filename")).endswith(".csv") else "application/json"
    return Response(
        content=str(package.get("content", "")), media_type=media,
        headers={"Content-Disposition": f'attachment; filename="{package.get("filename")}"'})


@router.get("/events")
def list_events(batch_id: int | None = Query(default=None)) -> dict[str, Any]:
    rows = shard_dispatcher.list_events(batch_id)
    return {"items": rows, "total": len(rows)}


@router.get("/batches/{batch_id}", response_model=dict)
def get_batch_state(batch_id: int) -> dict:
    """批次分发状态：分片进度、部分成功冲突与续传范围。"""
    state = shard_dispatcher.get_batch_state(batch_id)
    if state is None:
        from app.store import store

        batch = store.find("result_store", batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail=f"结果批次 {batch_id} 不存在")
        # 批次存在但还没有分片时返回空状态，页面无需把“未开始”当成错误。
        return {
            "batch_id": batch_id,
            "批次编号": batch.get("批次编号"),
            "status": "未生成分片",
            "分发状态": "未生成分片",
            "prepared": False,
            "total_shards": 0,
            "completed_shards": 0,
            "pending_shards": 0,
            "failed_shards": 0,
            "resume_from": None,
            "resume_shards": [],
            "partial_success": False,
            "channels": [],
            "manifest": [],
        }
    logs = shard_dispatcher.list_manifest(batch_id)
    return {**state, "prepared": True, "manifest": logs}


@router.post("/prepare", response_model=ActionResult)
def prepare(payload: PreparePayload) -> ActionResult:
    """读取已校验批次与通道配置，按分片生成可恢复的分片分发记录。"""
    result = shard_dispatcher.prepare(
        payload.batch_id, payload.channel_ids,
        shard_size=payload.shard_size, strategy=payload.strategy)
    if not result.get("ok"):
        return _failure(result)
    return ActionResult(ok=True, message=str(result.get("message")), entry=result)


@router.post("/transfer", response_model=ActionResult)
def transfer(payload: TransferPayload) -> ActionResult:
    """传输通道的待发送分片；已完成分片不会重复发送。"""
    result = shard_dispatcher.transfer(
        payload.batch_id, payload.channel_id, fail_shards=payload.fail_shards,
        interrupt_at=payload.interrupt_at, fail_after=payload.fail_after,
        fail_all=payload.fail_all)
    if not result.get("ok"):
        return _failure(result)
    return ActionResult(ok=True, message=str(result.get("message")), entry=result)


@router.post("/resume", response_model=ActionResult)
def resume(payload: ResumePayload) -> ActionResult:
    """从失败位置继续传输，已确认完成的分片跳过。"""
    result = shard_dispatcher.resume(
        payload.batch_id, payload.channel_id, fail_shards=payload.fail_shards,
        fail_after=payload.fail_after, fail_all=payload.fail_all)
    if not result.get("ok"):
        return _failure(result)
    return ActionResult(ok=True, message=str(result.get("message")), entry=result)


@router.post("/package", response_model=ActionResult)
def package(payload: PackagePayload) -> ActionResult:
    """打包完整文件；部分成功时按策略立即发布或暂存到全部通道完成。"""
    result = shard_dispatcher.package(
        payload.batch_id, payload.channel_id, strategy=payload.strategy)
    if not result.get("ok"):
        return _failure(result)
    return ActionResult(ok=True, message=str(result.get("message")), entry=result)


@router.get("/{record_id}", response_model=dict)
def get_shard(record_id: int) -> dict:
    record = shard_dispatcher.get_record(record_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"分片分发记录 {record_id} 不存在或已归档")
    return {**record, "分片状态": record.get("status"),
            "尝试次数": record.get("attempts", 0),
            "完成时间": record.get("completed_at") or ""}


@router.post("/{record_id}/actions", response_model=ActionResult)
def run_action(record_id: int, payload: ActionPayload) -> ActionResult:
    """对单个分片执行完成分片、标记失败、续传等动作。"""
    record, message = shard_dispatcher.run_record_action(record_id, payload.action.strip())
    if record is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=record)
