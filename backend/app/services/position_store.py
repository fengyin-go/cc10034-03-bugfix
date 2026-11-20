"""位点存储领域服务：持久化和管理Binlog位点信息。

上游：checkpoint_manager 创建检查点时联动更新位点
消费：cdc_channel 启动时查询位点；failover_group 切换时读取位点
下游：backfill_position 基于位点范围执行历史回填
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import PositionSnapshot, pipeline_bus


class PositionStoreService(BaseModuleService):
    MODULE = "position_store"
    ENTITY = "位点记录"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["记录ID", "数据库实例", "Binlog名称", "位点值", "Gtid集合", "更新时间", "位点来源"]
    REQUIRED_FIELDS = ["记录ID", "数据库实例", "Binlog名称"]
    STATUS_ORDER = ["当前位点", "历史位点", "归档位点"]
    ACTION_RULES = {"标记历史": "当前位点", "归档位点": "历史位点"}
    NEGATIVE_ACTIONS = []

    def save_position(self, instance: str, binlog_name: str, position: int,
                      gtid_set: str = "", source: str = "manual") -> PositionSnapshot:
        """人工保存位点。

        这是运维显式发布已确认位点，会成对写入已确认位点数据快照和回执；
        检查点候选登记不走这里，因此不会污染恢复清单。
        """
        ps = PositionSnapshot(
            instance=instance,
            binlog_name=binlog_name,
            position=position,
            gtid_set=gtid_set,
            updated_at=datetime.now(),
            source=source,
        )
        pipeline_bus.publish_manual_position(ps)
        pipeline_bus.write_audit(
            "位点存储", f"{instance}:{binlog_name}",
            f"位点更新至 {binlog_name}:{position}",
            "position_store", "已记录"
        )
        return ps

    def query_current_position(self, instance: str) -> dict[str, Any]:
        """查询指定实例的当前位点。

        只认已确认位点数据快照：检查点候选登记期间仍返回上一次确认的偏移，
        没有任何确认记录时返回 found=False，而不是未确认的候选位置。
        """
        snapshot = pipeline_bus.get_confirmed_snapshot(instance)
        if snapshot is None:
            return {"instance": instance, "found": False}
        return {
            "instance": instance,
            "found": True,
            "binlog_name": snapshot.binlog_file,
            "position": snapshot.offset,
            "gtid_set": snapshot.gtid_set,
            "updated_at": snapshot.updated_at.isoformat(),
            "source": snapshot.source,
            "origin": snapshot.origin,
            "checkpoint_id": snapshot.checkpoint_id,
            "version": snapshot.version,
        }

    def recovery_manifest(self, instance: str) -> dict[str, Any]:
        """位点恢复清单：与检查点管理共用同一份已确认位点数据快照。

        偏移量、确认时间与确认回执在同一份快照里，读到的三者必然一致。
        """
        manifest = pipeline_bus.confirmed_snapshot_manifest(instance)
        if manifest is None:
            return {
                "channel": instance,
                "found": False,
                "recovery_position": None,
                "message": "没有已确认位点，不能从候选日志位置恢复",
            }
        return {**manifest, "recovery_position": manifest["position"]}

    def pending_candidates(self, instance: str) -> dict[str, Any]:
        """候选清单：只用于展示待确认检查点，明确标注不可作为恢复起点。"""
        candidate = pipeline_bus.positions.get(instance)
        if candidate is None:
            return {"channel": instance, "found": False, "usable_for_recovery": False}
        confirmed = pipeline_bus.get_confirmed_snapshot(instance)
        return {
            "channel": instance,
            "found": True,
            "binlog_file": candidate.binlog_name,
            "offset": candidate.position,
            "source": candidate.source,
            "usable_for_recovery": False,
            "confirmed_position": (
                f"{confirmed.binlog_file}:{confirmed.offset}" if confirmed else None
            ),
        }

    def compare_positions(self, pos_a: dict[str, Any], pos_b: dict[str, Any]) -> dict[str, Any]:
        """比较两个位点的差异。"""
        same_file = pos_a.get("binlog_name") == pos_b.get("binlog_name")
        offset_a = int(pos_a.get("position", 0))
        offset_b = int(pos_b.get("position", 0))
        lag_bytes = abs(offset_a - offset_b) if same_file else None
        return {
            "same_binlog_file": same_file,
            "offset_a": offset_a,
            "offset_b": offset_b,
            "lag_bytes": lag_bytes,
            "files_differ": not same_file,
        }

    def parse_gtid(self, gtid_str: str) -> dict[str, Any]:
        """解析 Gtid 集合字符串。"""
        uuid_set: list[str] = []
        intervals: list[dict[str, Any]] = []
        for part in gtid_str.split(","):
            part = part.strip()
            if not part:
                continue
            match = re.match(r'([a-f0-9-]+):(\d+)(?:-(\d+))?', part, re.IGNORECASE)
            if match:
                uuid_val = match.group(1)
                start = int(match.group(2))
                end = int(match.group(3)) if match.group(3) else start
                uuid_set.append(uuid_val)
                intervals.append({"uuid": uuid_val, "start": start, "end": end, "count": end - start + 1})
        return {"uuid_count": len(set(uuid_set)), "total_transactions": sum(it["count"] for it in intervals), "intervals": intervals}

    def estimate_binlog_lag_events(self, source_pos: PositionSnapshot,
                                   target_pos: PositionSnapshot, avg_event_bytes: int = 256) -> int:
        """估算位点之间的积压事件数。"""
        if source_pos.binlog_name != target_pos.binlog_name:
            return -1
        diff = abs(source_pos.position - target_pos.position)
        return diff // max(avg_event_bytes, 1)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "标记历史":
            pipeline_bus.write_audit(
                "位点归档", entry.get("数据库实例", ""),
                f"位点 {entry_id} 已标记为历史",
                "position_store", "已记录"
            )
        return entry, msg