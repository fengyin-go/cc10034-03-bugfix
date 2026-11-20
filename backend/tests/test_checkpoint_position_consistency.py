"""检查点提交/回退与位点恢复清单的一致性回归测试。

覆盖场景：
- 同通道偏移 120 与 480 两个检查点，候选登记前后位点查询结果；
- 提交、回退交错后恢复清单与恢复起点；
- 提交/回退重试幂等，不改写竞争双方终态，也不影响其他通道。

运行：cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.pipeline import pipeline_bus  # noqa: E402
from app.services.checkpoint_manager import CheckpointManagerService  # noqa: E402
from app.services.position_store import PositionStoreService  # noqa: E402


class CheckpointPositionConsistencyTest(unittest.TestCase):
    def setUp(self) -> None:
        pipeline_bus.positions.clear()
        pipeline_bus.confirmed_positions.clear()
        pipeline_bus.checkpoints.clear()
        pipeline_bus.checkpoint_receipts.clear()
        pipeline_bus.audit_log.clear()
        pipeline_bus.positions.clear()
        self.cm = CheckpointManagerService()
        self.ps = PositionStoreService()

    def register_pair(self, channel: str, binlog: str):
        low = self.cm.create_checkpoint(channel, binlog, 120, {"gtid_set": "uuid:1-10"})
        high = self.cm.create_checkpoint(channel, binlog, 480, {"gtid_set": "uuid:1-40"})
        return low, high

    # ---- 候选登记期间 ----
    def test_candidate_registration_does_not_publish_position(self) -> None:
        self.assertFalse(self.ps.query_current_position("chA")["found"])
        low, high = self.register_pair("chA", "mysql-bin.000007")
        self.assertNotEqual(low.checkpoint_id, high.checkpoint_id)

        current = self.ps.query_current_position("chA")
        self.assertFalse(current["found"], "候选登记后不应出现当前位点")
        self.assertIsNone(self.ps.recovery_manifest("chA")["recovery_position"])

        candidate = self.ps.pending_candidates("chA")
        self.assertEqual(candidate["offset"], 480)
        self.assertFalse(candidate["usable_for_recovery"])

    def test_candidate_keeps_last_confirmed_offset(self) -> None:
        low, _ = self.register_pair("chA", "mysql-bin.000007")
        self.cm.commit_checkpoint(low.checkpoint_id)
        self.cm.create_checkpoint("chA", "mysql-bin.000007", 960)
        self.assertEqual(self.ps.query_current_position("chA")["position"], 120)
        self.assertEqual(
            self.ps.recovery_manifest("chA")["recovery_position"], "mysql-bin.000007:120")

    # ---- 提交 / 回退 ----
    def test_commit_then_rollback_updates_manifest(self) -> None:
        low, high = self.register_pair("chA", "mysql-bin.000007")
        self.assertTrue(self.cm.commit_checkpoint(high.checkpoint_id)["committed"])
        self.assertEqual(self.ps.recovery_manifest("chA")["recovery_position"],
                         "mysql-bin.000007:480")

        result = self.cm.rollback_to_checkpoint(low.checkpoint_id)
        self.assertTrue(result["success"])
        manifest = self.ps.recovery_manifest("chA")
        self.assertEqual(manifest["recovery_position"], "mysql-bin.000007:120")
        self.assertEqual(manifest["origin"], "rollback")
        self.assertEqual(manifest["receipt"]["result"], "已回退")
        self.assertEqual(manifest["offset"], manifest["receipt"]["offset"])

    def test_snapshot_and_receipt_published_together(self) -> None:
        low, _ = self.register_pair("chA", "mysql-bin.000007")
        result = self.cm.commit_checkpoint(low.checkpoint_id)
        snapshot = result["snapshot"]
        self.assertEqual(snapshot["offset"], snapshot["receipt"]["offset"])
        self.assertEqual(snapshot["checkpoint_id"], snapshot["receipt"]["checkpoint_id"])
        manifest = self.ps.recovery_manifest("chA")
        self.assertEqual(manifest["version"], snapshot["version"])
        self.assertEqual(manifest["receipt"], snapshot["receipt"])

    def test_interleaved_commit_and_rollback_publish_single_snapshot(self) -> None:
        low, high = self.register_pair("chA", "mysql-bin.000007")
        barrier = threading.Barrier(2)
        outcomes: dict[str, dict] = {}

        def commit() -> None:
            barrier.wait()
            outcomes["commit"] = self.cm.commit_checkpoint(high.checkpoint_id)

        def rollback() -> None:
            barrier.wait()
            outcomes["rollback"] = self.cm.rollback_to_checkpoint(low.checkpoint_id)

        threads = [threading.Thread(target=commit), threading.Thread(target=rollback)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        manifest = self.ps.recovery_manifest("chA")
        # 回退成功即生效：恢复起点回到 120，480 的提交不能再覆盖它。
        self.assertTrue(outcomes["rollback"]["success"])
        self.assertEqual(manifest["recovery_position"], "mysql-bin.000007:120")
        self.assertEqual(manifest["origin"], "rollback")
        self.assertFalse(outcomes["commit"]["committed"])

    def test_commit_after_rollback_target_not_reverted(self) -> None:
        low, high = self.register_pair("chA", "mysql-bin.000007")
        self.cm.commit_checkpoint(low.checkpoint_id)
        self.cm.commit_checkpoint(high.checkpoint_id)
        self.cm.rollback_to_checkpoint(low.checkpoint_id)
        self.assertEqual(self.ps.recovery_manifest("chA")["recovery_position"],
                         "mysql-bin.000007:120")
        # 回退之后新登记的检查点仍可提交，恢复起点可以正常向前推进。
        fresh = self.cm.create_checkpoint("chA", "mysql-bin.000007", 600)
        self.assertTrue(self.cm.commit_checkpoint(fresh.checkpoint_id)["committed"])
        self.assertEqual(self.ps.recovery_manifest("chA")["recovery_position"],
                         "mysql-bin.000007:600")

    # ---- 幂等 ----
    def test_commit_retry_is_idempotent(self) -> None:
        low, _ = self.register_pair("chA", "mysql-bin.000007")
        first = self.cm.commit_checkpoint(low.checkpoint_id)
        before = self.ps.recovery_manifest("chA")
        retry = self.cm.commit_checkpoint(low.checkpoint_id)
        after = self.ps.recovery_manifest("chA")
        self.assertTrue(retry["idempotent"])
        self.assertEqual(first["snapshot"]["receipt"], retry["snapshot"]["receipt"])
        self.assertEqual(before, after)

    def test_rollback_retry_is_idempotent(self) -> None:
        low, high = self.register_pair("chA", "mysql-bin.000007")
        self.cm.commit_checkpoint(high.checkpoint_id)
        self.cm.rollback_to_checkpoint(low.checkpoint_id)
        before = self.ps.recovery_manifest("chA")
        retry = self.cm.rollback_to_checkpoint(low.checkpoint_id)
        after = self.ps.recovery_manifest("chA")
        self.assertTrue(retry["idempotent"])
        self.assertEqual(before, after)

    def test_commit_retry_cannot_undo_recorded_rollback(self) -> None:
        low, high = self.register_pair("chA", "mysql-bin.000007")
        self.cm.commit_checkpoint(high.checkpoint_id)
        self.cm.rollback_to_checkpoint(low.checkpoint_id)
        before = self.ps.recovery_manifest("chA")
        retry = self.cm.commit_checkpoint(high.checkpoint_id)
        self.assertFalse(retry["committed"])
        self.assertEqual(self.ps.recovery_manifest("chA"), before)
        states = {cp.offset: cp.state for cp in pipeline_bus.channel_checkpoints("chA")}
        self.assertEqual(states, {120: "已确认", 480: "已回退"})

    # ---- 通道隔离 ----
    def test_other_channel_untouched(self) -> None:
        a_low, a_high = self.register_pair("chA", "mysql-bin.000007")
        b_low, b_high = self.register_pair("chB", "mysql-bin.000009")
        self.cm.commit_checkpoint(b_high.checkpoint_id)
        b_before = self.ps.recovery_manifest("chB")

        self.cm.commit_checkpoint(a_high.checkpoint_id)
        self.cm.rollback_to_checkpoint(a_low.checkpoint_id)
        self.cm.commit_checkpoint(a_high.checkpoint_id)
        self.cm.rollback_to_checkpoint(a_low.checkpoint_id)

        self.assertEqual(self.ps.recovery_manifest("chB"), b_before)
        self.assertEqual(self.ps.recovery_manifest("chB")["recovery_position"],
                         "mysql-bin.000009:480")
        states_b = {cp.offset: cp.state for cp in pipeline_bus.channel_checkpoints("chB")}
        self.assertEqual(states_b, {120: "候选", 480: "已确认"})


if __name__ == "__main__":
    unittest.main()
