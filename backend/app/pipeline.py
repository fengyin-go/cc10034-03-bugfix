"""跨模块管道状态总线：服务间通过管道层完成事件、位点、Schema 的发布-消费。"""
from __future__ import annotations

import hashlib
import threading
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterator


@dataclass
class ChangeEventRecord:
    """变更事件统一结构。"""
    event_id: str
    source_db: str
    source_table: str
    operation: str  # INSERT / UPDATE / DELETE
    before_image: dict[str, Any] | None
    after_image: dict[str, Any] | None
    tx_id: str
    binlog_file: str
    binlog_pos: int
    gtid: str
    event_time: datetime
    raw_format: str = "debezium"
    columns: list[str] = field(default_factory=list)


@dataclass
class DdlRecord:
    """DDL 变更结构。"""
    record_id: str
    database: str
    table_name: str
    ddl_statement: str
    executed_at: datetime
    affected_columns: int
    rollback_statement: str
    column_changes: list[dict[str, Any]] = field(default_factory=list)
    partition_changes: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class PositionSnapshot:
    """位点快照结构。"""
    instance: str
    binlog_name: str
    position: int
    gtid_set: str
    updated_at: datetime
    source: str
    channel_id: int | None = None


@dataclass
class CheckpointRecord:
    """检查点结构。"""
    checkpoint_id: str
    channel_name: str
    binlog_file: str
    offset: int
    confirmed_at: datetime
    committed: bool
    data_checksum: str
    gtid_set: str = ""
    # 候选 →（已确认 | 已回退 | 已过期），以及该检查点是否留下过已确认快照
    state: str = "候选"
    seq: int = 0
    published_at: datetime | None = None


@dataclass(frozen=True)
class ConfirmedPositionSnapshot:
    """已确认位点数据快照。

    只在检查点提交/回退成功时生成；position_store 的恢复清单与
    query_current_position 都只读这份快照，候选登记期间不可见。
    """
    channel_name: str
    checkpoint_id: str
    binlog_file: str
    offset: int
    gtid_set: str
    confirmed_at: datetime
    updated_at: datetime
    source: str
    origin: str  # "commit" | "rollback" | "manual"
    receipt: dict[str, Any]
    version: int = 0
    base_version: int = 0
    # 回退时通道内已存在的最大登记序号；序号不超过它的提交会被回退覆盖
    rollback_ceiling: int = 0


@dataclass
class CheckpointPublishResult:
    """发布结果：只有发布成功时快照和回执才可见。"""
    accepted: bool
    checkpoint: CheckpointRecord | None = None
    snapshot: ConfirmedPositionSnapshot | None = None
    reason: str = ""
    idempotent: bool = False


class PipelineBus:
    """管道级事件总线：持有事件队列、Schema 注册表、位点注册表、SCD 状态存储。

    各领域服务通过 pipeline_bus 单例完成跨模块数据流通。
    """

    def __init__(self) -> None:
        # 事件队列 — binlog_parser 产出，change_event 消费
        self.event_queue: list[ChangeEventRecord] = []
        self.event_log: list[dict[str, Any]] = []
        # DDL 记录 — ddl_tracker 产出，schema_sync/partition_sync 消费
        self.ddl_log: list[DdlRecord] = []
        # 位点注册 — checkpoint_manager/position_store 写入，cdc_channel/failover_group 消费
        self.positions: dict[str, PositionSnapshot] = {}
        self.checkpoints: list[CheckpointRecord] = []
        # 已确认位点数据快照 — 提交/回退成功后才原子发布，position_store 恢复清单与
        # cdc_channel/failover_group 恢复起点都只读这里，候选登记不落在这里。
        self.confirmed_positions: dict[str, ConfirmedPositionSnapshot] = {}
        # 发布回执 — checkpoint_id → 终态回执；终态一旦记录，重试只回放、不改写
        self.checkpoint_receipts: dict[str, dict[str, Any]] = {}
        self._checkpoint_seq = 0
        self._snapshot_version = 0
        # 写锁：提交与回退互斥，保证通道内只发布单一有效快照
        self.state_lock = threading.RLock()
        # Schema 注册 — schema_sync 写入，ddl_tracker/format_adapter 消费
        self.schema_registry: dict[str, list[dict[str, Any]]] = {}
        # SCD 状态 — scd_handler 核心存储
        self.scd_type1_store: dict[str, dict[str, Any]] = {}  # snapshot
        self.scd_type2_store: dict[str, list[dict[str, Any]]] = {}  # history
        self.scd_type3_store: dict[str, dict[str, Any]] = {}  # prev-col
        # 去重状态 — dedup_window 核心存储
        self.dedup_state: dict[str, list[dict[str, Any]]] = defaultdict(list)
        # 路由规则 — route_dispatcher 核心配置
        self.routing_table: list[dict[str, Any]] = []
        # 过滤规则 — event_filter 核心配置
        self.filter_rules: list[dict[str, Any]] = []
        # 快照比对结果
        self.compare_results: list[dict[str, Any]] = []
        # 审计日志
        self.audit_log: list[dict[str, Any]] = []
        # 延迟指标缓冲
        self.lag_buffer: list[dict[str, Any]] = []
        # 解析统计
        self.parse_stats: dict[str, Any] = {
            "total_parsed": 0,
            "total_events": 0,
            "parse_errors": 0,
            "last_batch_time": None,
        }
        # 管道运行时统计
        self.pipeline_metrics: dict[str, Any] = {
            "total_input": 0,
            "total_output": 0,
            "filtered": 0,
            "routed": 0,
            "scd_processed": 0,
            "dedup_hits": 0,
            "backfill_rows": 0,
        }

    def publish_event(self, event: ChangeEventRecord) -> dict[str, Any]:
        """将解析产出的事件发布到队列。返回发布确认。"""
        self.event_queue.append(event)
        self.pipeline_metrics["total_input"] += 1
        self.parse_stats["total_parsed"] += 1
        self.parse_stats["last_batch_time"] = datetime.now().isoformat()
        return {
            "event_id": event.event_id,
            "position": event.binlog_pos,
            "general": self.pipeline_metrics["total_input"],
        }

    def consume_events(self, batch_size: int = 100) -> list[ChangeEventRecord]:
        """从事件队列消费变更事件。返回批量事件。"""
        batch = self.event_queue[:batch_size]
        self.event_queue = self.event_queue[batch_size:]
        self.pipeline_metrics["total_output"] += len(batch)
        return batch

    @contextmanager
    def locked(self) -> Iterator[None]:
        """发布检查点时使用：提交与回退在同一通道上互斥。"""
        with self.state_lock:
            yield

    def register_position(self, pos: PositionSnapshot) -> None:
        """登记候选位点（草稿）。不会进入恢复清单，也不会成为当前位点。"""
        self.positions[pos.instance] = pos

    def get_position(self, instance: str) -> PositionSnapshot | None:
        """查询当前位点。

        只有发不过确认快照的通道才有当前位点；候选登记期间返回 None，
        调用方因此不会从尚未确认的日志位置恢复。
        """
        snapshot = self.confirmed_positions.get(instance)
        if snapshot is None:
            return None
        return PositionSnapshot(
            instance=snapshot.channel_name,
            binlog_name=snapshot.binlog_file,
            position=snapshot.offset,
            gtid_set=snapshot.gtid_set,
            updated_at=snapshot.updated_at,
            source=snapshot.source,
        )

    def get_confirmed_snapshot(self, channel: str) -> ConfirmedPositionSnapshot | None:
        """读取通道的已确认位点数据快照（恢复清单）。"""
        return self.confirmed_positions.get(channel)

    def confirmed_snapshot_manifest(self, channel: str) -> dict[str, Any] | None:
        """通道恢复清单：位置、偏移量、确认回执一起可见。"""
        snapshot = self.confirmed_positions.get(channel)
        if snapshot is None:
            return None
        return {
            "channel": channel,
            "found": True,
            "version": snapshot.version,
            "checkpoint_id": snapshot.checkpoint_id,
            "binlog_file": snapshot.binlog_file,
            "offset": snapshot.offset,
            "position": f"{snapshot.binlog_file}:{snapshot.offset}",
            "gtid_set": snapshot.gtid_set,
            "confirmed_at": snapshot.confirmed_at.isoformat(),
            "updated_at": snapshot.updated_at.isoformat(),
            "source": snapshot.source,
            "origin": snapshot.origin,
            "receipt": dict(snapshot.receipt),
        }

    def next_checkpoint_seq(self) -> int:
        """检查点登记序号：同通道内提交/回退的冲突优先级以此为准。"""
        self._checkpoint_seq += 1
        return self._checkpoint_seq

    def next_snapshot_version(self) -> int:
        """已确认快照版本号：每发布一次递增，未发布不消耗。"""
        self._snapshot_version += 1
        return self._snapshot_version

    def channel_checkpoints(self, channel: str) -> list[CheckpointRecord]:
        """按登记顺序返回通道内检查点。"""
        return [cp for cp in self.checkpoints if cp.channel_name == channel]

    def publish_commit(self, checkpoint_id: str, now: datetime | None = None) -> CheckpointPublishResult:
        """提交检查点：生成并原子发布已确认位点数据快照。

        幂等：同一检查点重复提交只回放首次回执；已被回退的检查点不能再由
        提交重试改写终态。
        """
        now = now or datetime.now()
        with self.state_lock:
            cp = self.find_checkpoint(checkpoint_id)
            if cp is None:
                return CheckpointPublishResult(False, reason="not found")
            current = self.confirmed_positions.get(cp.channel_name)
            if cp.state == "已回退":
                return CheckpointPublishResult(
                    False, checkpoint=cp,
                    reason="checkpoint already rolled back",
                )
            if cp.state == "已过期":
                return CheckpointPublishResult(
                    False, checkpoint=cp, reason="checkpoint expired",
                )
            if cp.committed:
                # 重试：不改写终态，只回放已经发布的回执。
                snapshot = self.confirmed_positions.get(cp.channel_name)
                if snapshot is not None and snapshot.checkpoint_id == cp.checkpoint_id:
                    return CheckpointPublishResult(
                        True, checkpoint=cp, snapshot=snapshot, idempotent=True,
                    )
                return CheckpointPublishResult(
                    True, checkpoint=cp, idempotent=True, reason="already committed",
                )
            # 回退之后登记的新检查点（seq 大于回退时的上限）可以正常提交；
            # 回退之前已登记、但没被这次回退覆盖掉终态的检查点则不能把起点推回去。
            if (
                current is not None
                and current.origin == "rollback"
                and cp.seq <= current.rollback_ceiling
            ):
                return CheckpointPublishResult(
                    False, checkpoint=cp,
                    reason=f"superseded by rollback to {current.checkpoint_id}",
                )
            cp.committed = True
            cp.state = "已确认"
            cp.confirmed_at = now
            cp.published_at = now
            return self._publish_snapshot(cp, origin="commit", now=now)

    def publish_rollback(self, checkpoint_id: str, now: datetime | None = None) -> CheckpointPublishResult:
        """回退到指定检查点：以目标检查点为已确认位点数据快照。

        只影响目标检查点所属通道；回退本身幂等，重复回退不会改变已记录终态。
        """
        now = now or datetime.now()
        with self.state_lock:
            cp = self.find_checkpoint(checkpoint_id)
            if cp is None:
                return CheckpointPublishResult(False, reason="checkpoint not found")
            if cp.state == "已过期":
                return CheckpointPublishResult(
                    False, checkpoint=cp, reason="checkpoint expired",
                )
            current = self.confirmed_positions.get(cp.channel_name)
            if (
                current is not None
                and current.origin == "rollback"
                and current.checkpoint_id == cp.checkpoint_id
            ):
                return CheckpointPublishResult(
                    True, checkpoint=cp, snapshot=current, idempotent=True,
                )
            channel = cp.channel_name
            # 通道内序号大于目标的检查点视为回退范围内的候选：撤销提交标记。
            # 同一通道之外的检查点一概不动。
            for other in self.channel_checkpoints(channel):
                if other.seq > cp.seq and other.state != "已回退":
                    other.committed = False
                    other.state = "已回退"
            cp.committed = True
            cp.state = "已确认"
            cp.published_at = now
            rollback_ceiling = max(
                (other.seq for other in self.channel_checkpoints(channel)), default=cp.seq)
            return self._publish_snapshot(
                cp, origin="rollback", now=now, rollback_ceiling=rollback_ceiling)

    def _publish_snapshot(self, cp: CheckpointRecord, *, origin: str,
                          now: datetime,
                          rollback_ceiling: int = 0) -> CheckpointPublishResult:
        """构造快照与回执，成对写入后才对外可见（调用方已持锁）。"""
        confirmed_at = cp.confirmed_at if cp.state == "已确认" else now
        receipt = {
            "checkpoint_id": cp.checkpoint_id,
            "channel": cp.channel_name,
            "result": "已提交" if origin == "commit" else "已回退",
            "result_at": now.isoformat(),
            "binlog_file": cp.binlog_file,
            "offset": cp.offset,
            "position": f"{cp.binlog_file}:{cp.offset}",
            "idempotent_replay": False,
        }
        self.checkpoint_receipts.setdefault(cp.checkpoint_id, receipt)
        snapshot = ConfirmedPositionSnapshot(
            channel_name=cp.channel_name,
            checkpoint_id=cp.checkpoint_id,
            binlog_file=cp.binlog_file,
            offset=cp.offset,
            gtid_set=cp.gtid_set,
            confirmed_at=confirmed_at,
            updated_at=now,
            source="checkpoint_manager",
            origin=origin,
            receipt=receipt,
            version=self.next_snapshot_version(),
            base_version=cp.seq,
            rollback_ceiling=rollback_ceiling,
        )
        # 快照与回执同一把锁内成对替换，读方永远看不到半份数据。
        self.confirmed_positions[cp.channel_name] = snapshot
        return CheckpointPublishResult(True, checkpoint=cp, snapshot=snapshot)

    def publish_manual_position(self, pos: PositionSnapshot) -> ConfirmedPositionSnapshot:
        """人工保存位点：由运维显式发布，等价于一次已确认发布。"""
        now = pos.updated_at
        receipt = {
            "checkpoint_id": "",
            "channel": pos.instance,
            "result": "已发布",
            "result_at": now.isoformat(),
            "binlog_file": pos.binlog_name,
            "offset": pos.position,
            "position": f"{pos.binlog_name}:{pos.position}",
            "idempotent_replay": False,
        }
        with self.state_lock:
            snapshot = ConfirmedPositionSnapshot(
                channel_name=pos.instance,
                checkpoint_id="",
                binlog_file=pos.binlog_name,
                offset=pos.position,
                gtid_set=pos.gtid_set,
                confirmed_at=now,
                updated_at=now,
                source=pos.source,
                origin="manual",
                receipt=receipt,
                version=self.next_snapshot_version(),
            )
            self.confirmed_positions[pos.instance] = snapshot
        return snapshot

    def find_checkpoint(self, checkpoint_id: str) -> CheckpointRecord | None:
        for cp in self.checkpoints:
            if cp.checkpoint_id == checkpoint_id:
                return cp
        return None

    def calc_position_lag(self, source_pos: PositionSnapshot, target_pos: PositionSnapshot) -> int:
        """计算两个位点之间的落后字节数。"""
        if source_pos.binlog_name != target_pos.binlog_name:
            return -1  # 跨文件无法直接比较
        return abs(source_pos.position - target_pos.position)

    def add_ddl(self, ddl: DdlRecord) -> None:
        """添加 DDL 记录。"""
        self.ddl_log.append(ddl)

    def get_recent_ddls(self, limit: int = 50) -> list[DdlRecord]:
        """查询最近 DDL 记录。"""
        return self.ddl_log[-limit:]

    def compute_event_hash(self, event: ChangeEventRecord) -> str:
        """计算事件的业务去重哈希。"""
        raw = f"{event.event_id}:{event.source_table}:{event.operation}:{event.binlog_file}:{event.binlog_pos}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def apply_scd_type1(self, dim_key: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type1：覆盖式更新。"""
        existing = self.scd_type1_store.get(dim_key)
        if existing:
            existing.update(row)
        else:
            existing = row
        self.scd_type1_store[dim_key] = existing
        return existing

    def apply_scd_type2(self, dim_key: str, row: dict[str, Any]) -> list[dict[str, Any]]:
        """SCD Type2：保留完整历史。"""
        history = self.scd_type2_store.setdefault(dim_key, [])
        row["version"] = len(history) + 1
        history.append(dict(row))
        return list(history)

    def apply_scd_type3(self, dim_key: str, col_name: str, row: dict[str, Any]) -> dict[str, Any]:
        """SCD Type3：保留前值列。"""
        existing = self.scd_type3_store.get(dim_key)
        result = dict(row)
        if existing:
            result[f"_previous_{col_name}"] = existing.get(col_name)
        self.scd_type3_store[dim_key] = result
        return result

    def window_dedup(self, dedup_key: str, event_id: str, window_sec: float) -> bool:
        """窗口去重：检查指定 key 在窗口期内是否已有事件。"""
        now = time.time()
        buffer = self.dedup_state[dedup_key]
        buffer[:] = [e for e in buffer if now - e["ts"] < window_sec]
        for existing in buffer:
            if existing["event_id"] == event_id:
                self.pipeline_metrics["dedup_hits"] += 1
                return True  # 重复
        buffer.append({"event_id": event_id, "ts": now})
        return False

    def evaluate_filter(self, event: ChangeEventRecord) -> tuple[bool, list[str]]:
        """评估过滤规则，返回 (是否通过, 匹配规则列表)。"""
        passed = True
        matched: list[str] = []
        for rule in self.filter_rules:
            if rule.get("status") != "已启用":
                continue
            match = self._match_rule(event, rule)
            if match:
                matched.append(rule["name"])
                if rule.get("type") == "黑名单":
                    passed = False
            elif rule.get("type") == "白名单":
                passed = False
        if not passed:
            self.pipeline_metrics["filtered"] += 1
        return passed, matched

    def evaluate_route(self, event: ChangeEventRecord) -> list[str]:
        """评估路由规则，返回目标通道列表。"""
        targets: list[str] = []
        for rule in self.routing_table:
            if not rule.get("enabled"):
                continue
            if self._match_route(event, rule):
                targets.append(rule["target"])
        self.pipeline_metrics["routed"] += len(targets)
        return targets

    def _match_rule(self, event: ChangeEventRecord, rule: dict[str, Any]) -> bool:
        """匹配过滤规则。"""
        expr = rule.get("expression", "")
        if not expr:
            return False
        return (
            expr in event.source_table
            or expr in event.source_db
            or expr in event.operation
        )

    def _match_route(self, event: ChangeEventRecord, rule: dict[str, Any]) -> bool:
        """匹配路由规则。"""
        cond = rule.get("condition", "")
        if not cond:
            return False
        return (
            cond in event.source_table
            or cond in event.source_db
            or cond in event.operation
        )

    def write_audit(self, operation: str, target: str, summary: str,
                    source_info: str, result: str, remark: str = "") -> dict[str, Any]:
        """写入审计日志。"""
        entry = {
            "id": len(self.audit_log) + 1,
            "审计时间": datetime.now().isoformat(),
            "操作类型": operation,
            "目标对象": target,
            "变更摘要": summary,
            "操作来源": source_info,
            "结果状态": result,
            "审计备注": remark,
        }
        self.audit_log.append(entry)
        return entry

    def compute_backfill_estimate(self, source_pos: int, target_pos: int,
                                  batch_size: int, avg_row_bytes: int = 256) -> dict[str, Any]:
        """计算回填任务的预估指标。"""
        diff = abs(target_pos - source_pos) if target_pos >= source_pos else 0
        estimated_rows = diff // avg_row_bytes
        estimated_batches = (estimated_rows + batch_size - 1) // batch_size
        return {
            "position_diff_bytes": diff,
            "estimated_rows": estimated_rows,
            "estimated_batches": estimated_batches,
            "estimated_rows_str": f"{estimated_rows:,}",
        }

    def compare_snapshots(self, source_rows: int, target_rows: int,
                          diff_rows: int, sample_ratio: float) -> dict[str, Any]:
        """执行快照比对并计算差异指标。"""
        total = max(source_rows, target_rows, 1)
        diff_pct = round(diff_rows / total * 100, 2)
        return {
            "source_count": source_rows,
            "target_count": target_rows,
            "diff_count": diff_rows,
            "diff_percentage": diff_pct,
            "sample_ratio": sample_ratio,
            "status": "正常" if diff_pct < 5 else "差异发现",
            "checked_at": datetime.now().isoformat(),
        }

    def compute_lag(self, channel_name: str, lag_seconds: float,
                    backlog_events: int, source_pos: str, target_pos: str) -> dict[str, Any]:
        """计算延迟指标并缓冲。"""
        throughput = backlog_events / max(lag_seconds, 1) if lag_seconds > 0 else 0
        metric = {
            "指标时间": datetime.now().isoformat(),
            "所属通道": channel_name,
            "延迟秒数": round(lag_seconds, 1),
            "积压事件数": backlog_events,
            "吞吐量": f"{throughput:.1f}",
            "源位点": source_pos,
            "目标位点": target_pos,
        }
        self.lag_buffer.append(metric)
        if len(self.lag_buffer) > 1000:
            self.lag_buffer = self.lag_buffer[-1000:]
        return metric

    def snapshot(self) -> dict[str, Any]:
        """全管道状态快照。"""
        return {
            "parse_stats": dict(self.parse_stats),
            "pipeline_metrics": dict(self.pipeline_metrics),
            "queue_depth": len(self.event_queue),
            "ddl_count": len(self.ddl_log),
            "position_count": len(self.positions),
            "confirmed_position_count": len(self.confirmed_positions),
            "checkpoint_count": len(self.checkpoints),
            "register_count": len(self.schema_registry),
            "scd_type1_keys": len(self.scd_type1_store),
            "scd_type2_keys": len(self.scd_type2_store),
            "scd_type3_keys": len(self.scd_type3_store),
            "audit_count": len(self.audit_log),
            "lag_buffer_size": len(self.lag_buffer),
        }


pipeline_bus = PipelineBus()