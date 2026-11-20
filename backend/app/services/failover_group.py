"""故障切换领域服务：管理CDC通道的主备切换。

上游：cdc_channel + checkpoint_manager 产出通道状态和检查点 → failover_group 消费
消费：position_store 根据切换结果更新当前位点
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.pipeline import pipeline_bus


class FailoverGroupService(BaseModuleService):
    MODULE = "failover_group"
    ENTITY = "切换组"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["组名", "主通道", "备用通道", "切换条件", "自动切换", "心跳超时", "上次切换时间"]
    REQUIRED_FIELDS = ["组名", "主通道", "备用通道"]
    STATUS_ORDER = ["主运行", "备运行", "切换中", "已失效"]
    ACTION_RULES = {"执行切换": "主运行", "标记失效": "主运行"}
    NEGATIVE_ACTIONS = []

    def execute_failover(self, config: dict[str, Any]) -> dict[str, Any]:
        """执行主备切换。

        根据切换条件评估是否需要切换：
        - 主通道心跳超时
        - 主通道检查点停滞
        - 手动触发
        """
        group_name = config["组名"]
        primary_channel = config["主通道"]
        standby_channel = config["备用通道"]
        switch_condition = config.get("切换条件", "heartbeat_timeout")
        auto_switch = config.get("自动切换", "否") == "是"
        heartbeat_timeout = int(config.get("心跳超时", "30"))
        last_switch = config.get("上次切换时间", "")

        should_switch = self._evaluate_condition(
            switch_condition, primary_channel, heartbeat_timeout
        )

        result: dict[str, Any] = {
            "group": group_name,
            "primary": primary_channel,
            "standby": standby_channel,
            "switch_triggered": False,
            "switch_reason": "",
            "new_primary": primary_channel,
        }

        if should_switch or not auto_switch:
            result["switch_triggered"] = True
            result["switch_reason"] = "heartbeat_timeout" if should_switch else "manual"
            result["new_primary"] = standby_channel
            result["new_standby"] = primary_channel
            result["switched_at"] = datetime.now().isoformat()
            result["last_switch"] = result["switched_at"]

            pipeline_bus.write_audit(
                "故障切换", group_name,
                f"切换组 {group_name} 主通道从 {primary_channel} 切换至 {standby_channel}",
                "failover_group", "已切换"
            )
        else:
            result["switch_reason"] = "healthy"
            result["switch_triggered"] = False

        return result

    def _evaluate_condition(self, condition: str, channel: str, timeout: int) -> bool:
        """评估切换条件。"""
        if condition == "heartbeat_timeout":
            # 检查通道位点的最后更新时间
            pos = pipeline_bus.get_position(channel)
            if pos is None:
                return True
            elapsed = (datetime.now() - pos.updated_at).total_seconds()
            return elapsed > timeout
        if condition == "checkpoint_stale":
            snapshot = pipeline_bus.get_confirmed_snapshot(channel)
            if snapshot is None:
                return True
            elapsed = (datetime.now() - snapshot.confirmed_at).total_seconds()
            return elapsed > timeout * 3
        if condition == "always":
            return True
        return False

    def heartbeat_check(self, channel_name: str, timeout_sec: float = 30.0) -> dict[str, Any]:
        """心跳检查。"""
        pos = pipeline_bus.get_position(channel_name)
        healthy = True
        elapsed = 0.0
        if pos:
            elapsed = (datetime.now() - pos.updated_at).total_seconds()
            healthy = elapsed <= timeout_sec
        else:
            elapsed = -1

        return {
            "channel": channel_name,
            "healthy": healthy,
            "last_seen_sec_ago": round(elapsed, 1),
            "timeout_sec": timeout_sec,
            "needs_failover": not healthy and channel_name in {
                cp.channel_name for cp in pipeline_bus.checkpoints
            },
        }

    def compute_failover_risk(self, config: dict[str, Any]) -> dict[str, Any]:
        """计算故障风险评分。"""
        primary = config["主通道"]
        heartbeat = self.heartbeat_check(primary)
        snapshot = pipeline_bus.get_confirmed_snapshot(primary)
        checkpoint_commits = 1 if snapshot is not None else 0
        risk_score = 0
        if not heartbeat["healthy"]:
            risk_score += 50
        if checkpoint_commits == 0:
            risk_score += 30
        elapsed = heartbeat.get("last_seen_sec_ago", -1)
        if elapsed > 120:
            risk_score += 20
        risk_level = "低" if risk_score < 30 else ("中" if risk_score < 60 else "高")
        return {
            "group": config["组名"],
            "primary_channel": primary,
            "risk_score": min(risk_score, 100),
            "risk_level": risk_level,
            "healthy": heartbeat["healthy"],
            "checkpoint_commits": checkpoint_commits,
            "recommends_manual_check": risk_score >= 50,
        }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, msg = super().run_action(entry_id, action)
        if entry and action == "执行切换":
            result = self.execute_failover(entry)
            if result["switch_triggered"]:
                entry["status"] = "切换中"
        return entry, msg