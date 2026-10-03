"""提交水位与历史回放的自动化验证。

覆盖：正常推进、同秒多版本（first/last/at）、旧库迁移确定性与幂等、
并发提交水位唯一连续、服务重启与幂等重放不产生新水位、审计链与字段
裁剪使用同一排序语义，以及按水位证明证据可见性。
"""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.audit import AuditLog
from civicflow.cases import CaseService
from civicflow.corrections import CorrectionService
from civicflow.evidence import EvidenceService
from civicflow.errors import NotFoundError, ValidationError
from civicflow.security import AccessContext
from civicflow.transfers import TransferService


SAME_SECOND = "2026-09-28T12:00:00+08:00"
# 旧库实际持久化的是归一化后的 UTC（Z）形式。
LEGACY_TIME = "2026-09-28T04:00:00Z"

# 升级前的旧表结构（仅回放迁移所需的三张表），其余表由当前 schema 补建。
LEGACY_SCHEMA = r"""
CREATE TABLE entities (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id)
);
CREATE TABLE entity_versions (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id, version)
);
CREATE INDEX entity_versions_asof ON entity_versions(entity_type, entity_id, valid_from, version);
CREATE TABLE audit_entries (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    detail_json TEXT NOT NULL,
    previous_digest TEXT NOT NULL,
    entry_digest TEXT NOT NULL
);
"""


def canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def build_legacy_db(path: Path, *, same_second: str = LEGACY_TIME) -> None:
    """用旧结构写入同秒多版本，并按旧格式串好审计哈希链。"""
    import sqlite3

    connection = sqlite3.connect(path)
    connection.executescript(LEGACY_SCHEMA)
    previous = "0" * 64
    # 顺序刻意交错：e1#1 -> t1#1 -> e1#2，全部同一业务时间。
    entries = [
        ("evidence", "e1", 1, "create", {"state": "received"}),
        ("transfers", "t1", 1, "create", {"state": "prepared"}),
        ("evidence", "e1", 2, "update", {"state": "verified"}),
    ]
    for entity_type, entity_id, version, action, detail in entries:
        body = canonical({
            "occurred_at": same_second, "actor_id": "officer", "action": action,
            "entity_type": entity_type, "entity_id": entity_id, "version": version,
            "detail": detail, "previous": previous,
        })
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
        connection.execute(
            "INSERT INTO entity_versions VALUES (?,?,?,?,?,?,?,?)",
            (entity_type, entity_id, version, detail.get("state", "draft"),
             canonical(detail), same_second, "officer", "legacy-key"),
        )
        connection.execute(
            "INSERT INTO audit_entries(occurred_at,actor_id,action,entity_type,entity_id,"
            "version,detail_json,previous_digest,entry_digest) VALUES (?,?,?,?,?,?,?,?,?)",
            (same_second, "officer", action, entity_type, entity_id, version,
             canonical(detail), previous, digest),
        )
        previous = digest
    # entities 只保存最新指针：e1 为 v2，t1 为 v1。
    connection.execute(
        "INSERT INTO entities VALUES (?,?,?,?,?,?,?,?,?)",
        ("evidence", "e1", 2, "verified", canonical({"state": "verified"}),
         same_second, same_second, "officer", "officer"),
    )
    connection.execute(
        "INSERT INTO entities VALUES (?,?,?,?,?,?,?,?,?)",
        ("transfers", "t1", 1, "prepared", canonical({"state": "prepared"}),
         same_second, same_second, "officer", "officer"),
    )
    connection.commit()
    connection.close()


class WatermarkTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "platform.sqlite3"
        self.system = AccessContext.system("officer")

    def tearDown(self):
        self.temp.cleanup()

    def open_app(self, fixed_now: str | None = SAME_SECOND) -> CivicFlow:
        return CivicFlow.open(self.db_path, fixed_now=fixed_now)

    def test_normal_progression_returns_persistent_watermark(self):
        app = self.open_app()
        evidence = EvidenceService(app.repository)
        first = evidence.create(self.system, {
            "case_id": "case:1", "kind": "交易记录", "digest": "d1",
            "source": "platform", "occurred_at": app.clock.now(),
        }, request_key="ev-1")
        self.assertEqual(first["commit_seq"], 1)
        self.assertEqual(app.repository.head_watermark(), 1)

        app2 = self.open_app("2026-09-28T12:00:05+08:00")
        evidence = EvidenceService(app2.repository)
        second = evidence.transition(
            self.system, first["entity_id"], "verified",
            expected_version=1, reason="权利人鉴别通过", request_key="ev-2",
        )
        self.assertEqual(second["commit_seq"], 2)
        self.assertEqual(app2.repository.head_watermark(), 2)
        # 历史按水位升序。
        history = evidence.history(self.system, first["entity_id"])
        self.assertEqual([row["commit_seq"] for row in history], [1, 2])
        self.assertEqual(app2.verify()["audit_entries"], 2)

    def test_same_second_versions_first_last_and_explicit_watermark(self):
        app = self.open_app()
        evidence = EvidenceService(app.repository)
        values = {
            "case_id": "case:ipr", "kind": "原始交易证据", "digest": "hash:tx",
            "source": "平台导出", "occurred_at": app.clock.now(),
        }
        registered = evidence.create(self.system, values, request_key="register")
        # 同一秒内补录鉴别结论（新进程、同一冻结时间）。
        app2 = self.open_app(SAME_SECOND)
        evidence = EvidenceService(app2.repository)
        concluded = evidence.revise(
            self.system, registered["entity_id"], {"digest": "hash:tx:identified"},
            expected_version=1, request_key="conclusion",
        )
        self.assertEqual(registered["updated_at"], concluded["updated_at"])
        seq_registered = registered["commit_seq"]
        seq_concluded = concluded["commit_seq"]
        self.assertEqual(seq_concluded, seq_registered + 1)

        cutoff = SAME_SECOND
        # 默认与 last：截止时点看到最后可见版本（向后兼容）。
        default = evidence.snapshot(self.system, registered["entity_id"], as_of=cutoff)
        last = evidence.snapshot(self.system, registered["entity_id"], as_of=cutoff, bound="last")
        self.assertEqual(default["version"], 2)
        self.assertEqual(last["commit_seq"], seq_concluded)
        # first：该时点最先可见的版本——登记材料当时已经在卷。
        first = evidence.snapshot(self.system, registered["entity_id"], as_of=cutoff, bound="first")
        self.assertEqual(first["version"], 1)
        self.assertEqual(first["commit_seq"], seq_registered)
        # at：精确选择指定水位。
        at_one = evidence.snapshot(
            self.system, registered["entity_id"], as_of=cutoff,
            bound="at", watermark=seq_registered,
        )
        self.assertEqual(at_one["version"], 1)
        at_two = evidence.snapshot(
            self.system, registered["entity_id"], as_of=cutoff,
            bound="at", watermark=seq_concluded,
        )
        self.assertEqual(at_two["version"], 2)
        # 非法边界与缺失水位被拒绝。
        with self.assertRaises(ValidationError):
            evidence.snapshot(self.system, registered["entity_id"], as_of=cutoff, bound="middle")
        with self.assertRaises(ValidationError):
            evidence.snapshot(self.system, registered["entity_id"], as_of=cutoff, bound="at")
        # 早于该秒没有任何版本可见。
        with self.assertRaises(NotFoundError):
            evidence.snapshot(self.system, registered["entity_id"], as_of="2026-09-28T11:59:59+08:00", bound="first")
        # 历史顺序不依赖业务时间，仍为登记在前、结论在后。
        self.assertEqual(
            [row["version"] for row in evidence.history(self.system, registered["entity_id"])],
            [1, 2],
        )

    def test_watermark_proves_evidence_visibility_for_handover(self):
        app = self.open_app()
        evidence = EvidenceService(app.repository)
        registered = evidence.create(self.system, {
            "case_id": "case:ipr", "kind": "原始交易证据", "digest": "hash:tx",
            "source": "平台导出", "occurred_at": app.clock.now(),
        }, request_key="register")
        app2 = self.open_app(SAME_SECOND)
        transfers = TransferService(app2.repository)
        handover = transfers.create(self.system, {
            "case_id": "case:ipr", "from_org": "侦查机构", "to_org": "接收机构",
            "manifest": "清单", "sent_at": app2.clock.now(),
        }, request_key="handover")
        app3 = self.open_app(SAME_SECOND)
        evidence = EvidenceService(app3.repository)
        # 交接水位：原始证据首版本水位更早，已可见。
        visible = evidence.snapshot_at_watermark(
            self.system, registered["entity_id"], watermark=handover["commit_seq"]
        )
        self.assertEqual(visible["commit_seq"], registered["commit_seq"])
        # 证据登记之前的水位：尚不可见。
        with self.assertRaises(NotFoundError):
            evidence.snapshot_at_watermark(
                self.system, registered["entity_id"], watermark=registered["commit_seq"] - 1
            )

    def test_same_second_transfer_and_correction_ordering(self):
        app = self.open_app()
        transfers = TransferService(app.repository)
        prepared = transfers.create(self.system, {
            "case_id": "case:2", "from_org": "o1", "to_org": "o2",
            "manifest": "m", "sent_at": app.clock.now(),
        }, request_key="tr-1")
        app2 = self.open_app(SAME_SECOND)
        corrections = CorrectionService(app2.repository)
        correction = corrections.create(self.system, {
            "target_type": "transfers", "target_id": prepared["entity_id"],
            "before_digest": "b", "after_digest": "a", "reason": "状态更正",
        }, request_key="co-1")
        app3 = self.open_app(SAME_SECOND)
        transfers = TransferService(app3.repository)
        sent = transfers.transition(
            self.system, prepared["entity_id"], "sent",
            expected_version=1, reason="保管交接发出", request_key="tr-2",
        )
        self.assertLess(prepared["commit_seq"], correction["commit_seq"])
        self.assertLess(correction["commit_seq"], sent["commit_seq"])
        at_handover = transfers.snapshot(
            self.system, prepared["entity_id"], as_of=SAME_SECOND, bound="first"
        )
        self.assertEqual(at_handover["state"], "prepared")

    def test_legacy_database_gets_deterministic_order(self):
        build_legacy_db(self.db_path)
        app = self.open_app()  # 触发迁移
        evidence = EvidenceService(app.repository)
        # 同秒三行按 (业务时间, 版本, 实体类型, 实体标识) 确定性分配：
        # evidence#1 -> 1, transfers#1 -> 2, evidence#2 -> 3。
        history = evidence.history(self.system, "e1")
        self.assertEqual([(row["version"], row["commit_seq"]) for row in history], [(1, 1), (2, 3)])
        first = evidence.snapshot(self.system, "e1", as_of=SAME_SECOND, bound="first")
        last = evidence.snapshot(self.system, "e1", as_of=SAME_SECOND)
        self.assertEqual((first["version"], first["commit_seq"]), (1, 1))
        self.assertEqual((last["version"], last["commit_seq"]), (2, 3))
        transfers = TransferService(app.repository)
        t1 = transfers.snapshot(self.system, "t1", as_of=SAME_SECOND)
        self.assertEqual(t1["commit_seq"], 2)
        self.assertEqual(app.repository.head_watermark(), 3)
        self.assertEqual(app.verify()["audit_entries"], 3)

        # 再次打开不改变任何水位或摘要（迁移幂等）。
        app_again = self.open_app(SAME_SECOND)
        self.assertEqual(app_again.repository.head_watermark(), 3)
        self.assertEqual(
            [(row["version"], row["commit_seq"]) for row in EvidenceService(app_again.repository).history(self.system, "e1")],
            [(1, 1), (2, 3)],
        )
        self.assertEqual(app_again.verify()["audit_entries"], 3)

        # 迁移后的新写入严格接续水位。
        created = EvidenceService(app_again.repository).create(self.system, {
            "case_id": "case:new", "kind": "k", "digest": "d",
            "source": "s", "occurred_at": SAME_SECOND,
        }, request_key="after-migration")
        self.assertEqual(created["commit_seq"], 4)

    def test_legacy_migration_is_repeatable_and_new_data_keeps_order(self):
        # 全新库上重复初始化不应回填或改动既有水位。
        app = self.open_app()
        CaseService(app.repository).create(self.system, {
            "case_type": "协作", "subject": "迁移幂等", "owner_org": "o",
            "priority": "high", "opened_at": app.clock.now(),
        }, request_key="c1")
        self.open_app()
        app2 = self.open_app()
        self.assertEqual(app2.repository.head_watermark(), 1)

    def test_concurrent_commits_get_unique_contiguous_watermarks(self):
        app = self.open_app(fixed_now=None)

        def submit(index: int) -> int:
            service = EvidenceService(app.repository)
            row = service.create(AccessContext.system(f"officer-{index}"), {
                "case_id": f"case:{index}", "kind": "k", "digest": f"d{index}",
                "source": "s", "occurred_at": app.clock.now(),
            }, request_key=f"concurrent-{index}")
            return row["commit_seq"]

        with ThreadPoolExecutor(max_workers=8) as pool:
            sequences = list(pool.map(submit, range(40)))
        self.assertEqual(sorted(sequences), list(range(1, 41)))
        self.assertEqual(len(set(sequences)), 40)
        # 重启后水位与历史保持一致，审计链仍可验。
        reopened = self.open_app(fixed_now=None)
        self.assertEqual(reopened.repository.head_watermark(), 40)
        self.assertEqual(reopened.verify()["audit_entries"], 40)

    def test_restart_and_idempotent_replay_create_no_new_watermark(self):
        app = self.open_app()
        values = {
            "case_type": "协作", "subject": "同秒重放", "owner_org": "o",
            "priority": "high", "opened_at": SAME_SECOND,
        }
        first = CaseService(app.repository).create(self.system, values, request_key="idem")
        self.assertEqual(first["commit_seq"], 1)
        # 同秒重放：同进程、新进程各一次。
        replay_same = CaseService(app.repository).create(self.system, values, request_key="idem")
        reopened = self.open_app(SAME_SECOND)
        replay_restart = CaseService(reopened.repository).create(self.system, values, request_key="idem")
        self.assertEqual(replay_same["entity_id"], first["entity_id"])
        self.assertEqual(replay_restart["entity_id"], first["entity_id"])
        self.assertEqual(replay_restart["commit_seq"], 1)
        self.assertEqual(reopened.repository.head_watermark(), 1)
        self.assertEqual(reopened.verify()["audit_entries"], 1)
        # 重启后按水位回放结果稳定。
        evidence_snapshot = CaseService(reopened.repository).snapshot(
            self.system, first["entity_id"], as_of=SAME_SECOND, bound="first"
        )
        self.assertEqual(evidence_snapshot["commit_seq"], 1)

    def test_redaction_preserves_watermark_ordering(self):
        app = self.open_app()
        evidence = EvidenceService(app.repository)
        registered = evidence.create(self.system, {
            "case_id": "case:r", "kind": "k", "digest": "d",
            "source": "保密来源", "occurred_at": app.clock.now(),
        }, request_key="r1")
        app2 = self.open_app(SAME_SECOND)
        evidence = EvidenceService(app2.repository)
        evidence.revise(self.system, registered["entity_id"], {"digest": "d2"},
                        expected_version=1, request_key="r2")
        reader = AccessContext(actor_id="reader", permissions=frozenset({"history:evidence"}))
        first = evidence.snapshot(reader, registered["entity_id"], as_of=SAME_SECOND, bound="first")
        last = evidence.snapshot(reader, registered["entity_id"], as_of=SAME_SECOND, bound="last")
        at = evidence.snapshot(reader, registered["entity_id"], as_of=SAME_SECOND,
                               bound="at", watermark=first["commit_seq"])
        # 裁剪后水位字段保留，排序语义不变，敏感字段被遮蔽。
        self.assertEqual(first["commit_seq"], registered["commit_seq"])
        self.assertEqual(first["source"], "***")
        self.assertEqual(last["source"], "***")
        self.assertEqual(at["version"], first["version"])
        self.assertEqual(
            [row["commit_seq"] for row in evidence.history(reader, registered["entity_id"])],
            [1, 2],
        )


if __name__ == "__main__":
    unittest.main()
