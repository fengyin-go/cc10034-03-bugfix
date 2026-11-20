"""检查点管理领域服务：管理CDC采集的位点检查点提交和回退。

上游：cdc_channel 启动后记录检查点
消费：position_store 消费检查点更新位点记录
下游：failover_group 消费检查点判定是否需要故障切换

位点可见性约定：
- 登记检查点只产生「候选」，写进候选位点表，不进入恢复清单；
- 提交或回退成功后才生成已确认位点数据快照，快照与确认回执成对发布；
- position_store 的恢复清单与当前位点查询只读已确认快照；
- 提交/回退都按检查点终态幂等，重试只回放首次回执，不改写终态。
"""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.pipeline import (
    CheckpointPublishResult,
    CheckpointRecord,
    PositionSnapshot,
    pipeline_bus,
)


class CheckpointManagerService(BaseModuleService):
    MODULE = "checkpoint_manager"
    ENTITY = "检查点"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["检查点ID", "所属通道", "Binlog文件", "偏移量", "确认时间", "是否提交", "数据摘要"]
    REQUIRED_FIELDS = ["检查点ID", "所属通道", "Binlog文件"]
    STATUS_ORDER = ["已记录", "已提交", "已过期", "已回退"]
    ACTION_RULES = {"提交检查点": "已记录", "回退检查点": "已记录", "标记过期": "已提交"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记检查点：只生成候选，不向位点存储发布未确认偏移。"""
        entry, missing = super().create_entry(values)
        if entry is None:
            return entry, missing
        offset = self._parse_offset(entry.get("偏移量"))
        checkpoint = self.create_checkpoint(
            str(entry.get("所属通道") or ""),
            str(entry.get("Binlog文件") or ""),
            offset,
            {"gtid_set": str(entry.get("Gtid集合") or "")},
        )
        entry["checkpoint_id"] = checkpoint.checkpoint_id
        entry["偏移量"] = offset
        entry["是否提交"] = "否"
        return entry, missing

    def create_checkpoint(self, channel_name: str, binlog_file: str, offset: int,
                          data_payload: dict[str, Any] | None = None,
                          checkpoint_id: str | None = None) -> CheckpointRecord:
        """登记检查点候选。

        只写候选位点表：恢复清单和当前位点保持上一次已确认的快照，
        因此候选登记期间 position_store 读到的仍是有效偏移。
        """
        payload = data_payload or {}
        data_checksum = self.checksum_payload(payload)

        with pipeline_bus.locked():
            checkpoint = CheckpointRecord(
                checkpoint_id=checkpoint_id or self._next_checkpoint_id(channel_name),
                channel_name=channel_name,
                binlog_file=binlog_file,
                offset=int(offset),
                confirmed_at=datetime.now(),
                committed=False,
                data_checksum=data_checksum,
                gtid_set=str(payload.get("gtid_set", "")),
                state="候选",
                seq=pipeline_bus.next_checkpoint_seq(),
            )
            pipeline_bus.checkpoints.append(checkpoint)

            # 候选位点单独登记，不替代已确认快照。
            pipeline_bus.register_position(PositionSnapshot(
                instance=channel_name,
                binlog_name=binlog_file,
                position=int(offset),
                gtid_set=checkpoint.gtid_set,
                updated_at=datetime.now(),
                source="checkpoint_manager:candidate",
                channel_id=hash(channel_name) % 10000,
            ))

        pipeline_bus.write_audit(
            "登记检查点", channel_name,
            f"检查点 {checkpoint.checkpoint_id} 候选位点 {binlog_file}:{offset}（未确认）",
            "checkpoint_manager", "已记录"
        )
        return checkpoint

    def commit_checkpoint(self, checkpoint_id: str) -> dict[str, Any]:
        """提交检查点并发布已确认位点数据快照。

        幂等：重复提交回放同一份快照与回执；被回退过的检查点保持回退终态。
        """
        with pipeline_bus.locked():
            result = pipeline_bus.publish_commit(checkpoint_id)
            if not result.accepted:
                cp = result.checkpoint
                return {
                    "checkpoint_id": checkpoint_id,
                    "committed": False,
                    "reason": result.reason,
                    "position": f"{cp.binlog_file}:{cp.offset}" if cp else None,
                    "receipt": (
                        pipeline_bus.checkpoint_receipts.get(checkpoint_id)
                        if cp is not None else None
                    ),
                }
            cp = result.checkpoint
            pipeline_bus.write_audit(
                "提交检查点", cp.channel_name,
                f"检查点 {checkpoint_id} 已提交，位点 {cp.binlog_file}:{cp.offset}",
                "checkpoint_manager", "已提交"
            )
            return self._publish_payload("commit", result, idempotent=result.idempotent)

    def rollback_to_checkpoint(self, checkpoint_id: str) -> dict[str, Any]:
        """回退到指定检查点并以它发布已确认位点数据快照。

        只作用于目标检查点所属通道；重试幂等。
        """
        with pipeline_bus.locked():
            result = pipeline_bus.publish_rollback(checkpoint_id)
            if not result.accepted:
                return {
                    "success": False,
                    "checkpoint_id": checkpoint_id,
                    "reason": result.reason,
                }
            cp = result.checkpoint
            pipeline_bus.write_audit(
                "回退检查点", cp.channel_name,
                f"回退至检查点 {checkpoint_id}，位点 {cp.binlog_file}:{cp.offset}",
                "checkpoint_manager", "已回退"
            )
            payload = self._publish_payload("rollback", result, idempotent=result.idempotent)
            payload["success"] = True
            return payload

    def _publish_payload(self, origin: str, result: CheckpointPublishResult,
                         *, idempotent: bool) -> dict[str, Any]:
        """提交/回退的对外载荷：偏移与确认回执一起返回。"""
        snapshot = result.snapshot
        cp = result.checkpoint
        payload: dict[str, Any] = {
            "checkpoint_id": cp.checkpoint_id if cp else None,
            "committed": bool(cp and cp.committed),
            "position": f"{cp.binlog_file}:{cp.offset}" if cp else None,
            "idempotent": idempotent,
        }
        if snapshot is not None:
            payload["snapshot"] = {
                "channel": snapshot.channel_name,
                "version": snapshot.version,
                "origin": snapshot.origin,
                "checkpoint_id": snapshot.checkpoint_id,
                "binlog_file": snapshot.binlog_file,
                "offset": snapshot.offset,
                "position": f"{snapshot.binlog_file}:{snapshot.offset}",
                "gtid_set": snapshot.gtid_set,
                "confirmed_at": snapshot.confirmed_at.isoformat(),
                "updated_at": snapshot.updated_at.isoformat(),
                "receipt": dict(snapshot.receipt),
            }
        elif cp is not None:
            payload["receipt"] = pipeline_bus.checkpoint_receipts.get(cp.checkpoint_id)
        return payload

    def candidate_position(self, channel_name: str) -> dict[str, Any]:
        """候选登记期间可见的候选位点（仅供排查，不作为恢复起点）。"""
        ps = pipeline_bus.positions.get(channel_name)
        if ps is None:
            return {"channel": channel_name, "found": False}
        return {
            "channel": channel_name,
            "found": True,
            "binlog_file": ps.binlog_name,
            "offset": ps.position,
            "source": ps.source,
            "confirmed": False,
        }

    def checksum_payload(self, payload: dict[str, Any]) -> str:
        """对数据负载计算摘要。"""
        raw = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def compute_backup_position(self, channel_name: str) -> dict[str, Any]:
        """计算备份位点（通道最近一次已确认的位点快照）。"""
        snapshot = pipeline_bus.get_confirmed_snapshot(channel_name)
        if snapshot is None:
            return {"has_backup": False, "reason": "no confirmed checkpoint"}
        return {
            "has_backup": True,
            "checkpoint_id": snapshot.checkpoint_id,
            "position": f"{snapshot.binlog_file}:{snapshot.offset}",
            "confirmed_at": snapshot.confirmed_at.isoformat(),
            "origin": snapshot.origin,
        }

    def compare_checkpoint_lag(self, cp_a: CheckpointRecord, cp_b: CheckpointRecord) -> dict[str, Any]:
        """比较两个检查点的位点距离。"""
        offset_diff = 0
        if cp_a.binlog_file == cp_b.binlog_file:
            offset_diff = abs(cp_a.offset - cp_b.offset)
        else:
            offset_diff = -1
        return {
            "offset_diff": offset_diff,
            "time_diff_sec": abs((cp_a.confirmed_at - cp_b.confirmed_at).total_seconds()),
            "same_file": cp_a.binlog_file == cp_b.binlog_file,
        }

    def _next_checkpoint_id(self, channel_name: str) -> str:
        """检查点 ID 含随机后缀，避免同秒登记的同通道检查点互相覆盖。"""
        stamp = datetime.now().strftime("%Y%m%d%H%M%S")
        digest = hashlib.md5(f"{channel_name}:{uuid.uuid4()}".encode()).hexdigest()[:8]
        return f"ckpt_{stamp}_{digest}"

    def _sync_entry_state(self, entry: dict[str, Any], cp: CheckpointRecord | None,
                          action: str = "") -> None:
        """把检查点终态同步回资源列表行，避免行状态与快照不一致。

        回退目标本身是当前恢复起点，仍按「已提交」展示；只有被回退覆盖掉的
        检查点才落到「已回退」。
        """
        if cp is None:
            return
        entry["checkpoint_id"] = cp.checkpoint_id
        if cp.state == "已确认" and cp.committed:
            entry["status"] = "已提交"
            entry["是否提交"] = "是"
            entry["确认时间"] = (cp.published_at or cp.confirmed_at).isoformat()
            entry["pending"] = False
        elif cp.state == "已回退":
            entry["status"] = "已回退"
            entry["是否提交"] = "否"
            entry["pending"] = True
        elif cp.state == "已过期":
            entry["status"] = "已过期"
            entry["是否提交"] = "否"
            entry["pending"] = False

    def list_entries(self, **kwargs: Any) -> tuple[list[dict[str, Any]], int]:
        """列表查询前把每行绑定的检查点终态回写，保证列表与快照一致。"""
        with pipeline_bus.locked():
            for row in store.rows(self.MODULE):
                checkpoint_id = row.get("checkpoint_id")
                cp = pipeline_bus.find_checkpoint(checkpoint_id) if checkpoint_id else None
                self._sync_entry_state(row, cp)
        return super().list_entries(**kwargs)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry is None:
            return entry, msg
        channel = entry.get("所属通道", "")
        binlog = entry.get("Binlog文件", "mysql-bin.000001")
        offset = self._parse_offset(entry.get("偏移量", "0"))
        if action == "标记过期":
            with pipeline_bus.locked():
                cp = self._checkpoint_for_entry(entry)
                if cp is not None and cp.state != "已过期":
                    cp.state = "已过期"
                    cp.committed = False
                    entry["是否提交"] = "否"
            return entry, msg
        if action == "提交检查点":
            with pipeline_bus.locked():
                checkpoint = self._entry_checkpoint(entry) or self.create_checkpoint(
                    channel, binlog, offset,
                    checkpoint_id=entry.get("checkpoint_id") or None,
                )
                result = pipeline_bus.publish_commit(checkpoint.checkpoint_id)
                self._sync_entry_state(entry, result.checkpoint, action)
                if not result.accepted:
                    msg = f"检查点提交未生效：{result.reason}"
                elif result.idempotent:
                    msg = "检查点已提交，重试已回放首次回执"
                else:
                    msg = "检查点提交成功，已发布已确认位点快照"
            return entry, msg
        if action == "回退检查点":
            with pipeline_bus.locked():
                checkpoint = self._entry_checkpoint(entry) or self.create_checkpoint(
                    channel, binlog, offset,
                    checkpoint_id=entry.get("checkpoint_id") or None,
                )
                result = pipeline_bus.publish_rollback(checkpoint.checkpoint_id)
                self._sync_entry_state(entry, result.checkpoint, action)
                if not result.accepted:
                    msg = f"检查点回退未生效：{result.reason}"
                elif result.idempotent:
                    msg = "检查点已回退，重试已回放首次回执"
                else:
                    msg = "检查点回退成功，已发布已确认位点快照"
            return entry, msg
        return entry, msg

    def _entry_checkpoint(self, entry: dict[str, Any]) -> CheckpointRecord | None:
        """资源行已绑定检查点时直接复用，保证同一行的操作重试幂等。"""
        checkpoint_id = entry.get("checkpoint_id")
        if checkpoint_id:
            cp = pipeline_bus.find_checkpoint(checkpoint_id)
            if cp is not None:
                return cp
        return None

    def _checkpoint_for_entry(self, entry: dict[str, Any]) -> CheckpointRecord | None:
        """资源行尚未绑定检查点时，按通道与位点找回候选记录。"""
        channel = entry.get("所属通道", "")
        binlog = entry.get("Binlog文件", "")
        offset = self._parse_offset(entry.get("偏移量", "0"))
        matches = [
            cp for cp in pipeline_bus.channel_checkpoints(channel)
            if cp.binlog_file == binlog and cp.offset == offset
        ]
        return matches[-1] if matches else None

    @staticmethod
    def _parse_offset(value: Any) -> int:
        try:
            return int(str(value).strip() or 0)
        except (TypeError, ValueError):
            return 0
