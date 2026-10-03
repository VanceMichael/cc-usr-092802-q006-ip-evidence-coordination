"""写入水位（write_seq）与截止时点查询的自动化验证。

覆盖：正常推进、同秒多版本、旧库迁移、幂等重放、并发提交、
服务重启回放、审计链一致性以及字段裁剪的同一排序语义。
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.cases import CaseService
from civicflow.corrections import CorrectionService
from civicflow.errors import ConflictError, NotFoundError, ValidationError
from civicflow.evidence import EvidenceService
from civicflow.jsonutil import canonical_json
from civicflow.security import AccessContext
from civicflow.transfers import TransferService

T0 = "2026-09-28T12:00:00+08:00"
T1 = "2026-09-28T12:00:01+08:00"
T2 = "2026-09-28T12:00:02+08:00"

CASE_VALUES = {"case_type": "侵权线索", "subject": "某平台售假", "owner_org": "org:ipr", "priority": "high", "opened_at": T0}
EVIDENCE_VALUES = {"case_id": "case:ipr-1", "kind": "交易快照", "digest": "sha256:origin", "source": "platform:portal", "occurred_at": T0}


class WatermarkTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "test.sqlite3"
        self.app = CivicFlow.open(self.path, fixed_now=T0)
        self.system = AccessContext.system("tester")

    def tearDown(self):
        self.temp.cleanup()

    def reopen(self, *, fixed_now: str = T0) -> CivicFlow:
        """模拟服务重启：同一数据库文件重新打开。"""
        return CivicFlow.open(self.path, fixed_now=fixed_now)

    # 正常推进：水位随每次写入稳定递增，并体现在当前记录与历史中。
    def test_normal_progression_assigns_increasing_watermarks(self):
        service = CaseService(self.app.repository)
        created = service.create(self.system, CASE_VALUES, request_key="prog-1")
        self.assertEqual(created["write_seq"], 1)

        service = CaseService(self.reopen(fixed_now=T1).repository)
        second = service.revise(self.system, created["entity_id"], {"priority": "normal"}, expected_version=1, request_key="prog-2")
        service = CaseService(self.reopen(fixed_now=T2).repository)
        third = service.revise(self.system, created["entity_id"], {"priority": "low"}, expected_version=2, request_key="prog-3")

        seqs = [created["write_seq"], second["write_seq"], third["write_seq"]]
        self.assertEqual(seqs, sorted(seqs))
        self.assertEqual(len(set(seqs)), 3)
        self.assertEqual(self.app.repository.current_watermark(), seqs[-1])

        history = service.history(self.system, created["entity_id"])
        self.assertEqual([row["write_seq"] for row in history], seqs)
        self.assertEqual([row["version"] for row in history], [1, 2, 3])
        self.assertEqual(service.get(self.system, created["entity_id"])["write_seq"], seqs[-1])
        self.assertEqual(service.list_current(self.system)[0]["write_seq"], seqs[-1])
        # 默认（last）截止时点查询保持既有行为。
        self.assertEqual(service.snapshot(self.system, created["entity_id"], as_of=T1)["version"], 2)
        self.assertEqual(service.snapshot(self.system, created["entity_id"], as_of=T0)["version"], 1)

    # 同秒多版本：先登记原始交易证据，同秒补录鉴别结论。
    # 截止时点查询必须能区分该秒最先可见、最后可见与指定水位的版本。
    def test_same_second_versions_are_distinguishable(self):
        evidence = EvidenceService(self.app.repository)
        original = evidence.create(self.system, EVIDENCE_VALUES, request_key="ev-1")
        appraised = evidence.revise(
            self.system,
            original["entity_id"],
            {"digest": "sha256:origin+appraisal", "kind": "交易快照+鉴别结论"},
            expected_version=1,
            request_key="ev-2",
        )
        self.assertEqual(original["created_at"], appraised["updated_at"])  # 同一秒
        self.assertLess(original["write_seq"], appraised["write_seq"])

        history = evidence.history(self.system, original["entity_id"])
        self.assertEqual([row["write_seq"] for row in history], [original["write_seq"], appraised["write_seq"]])

        # 默认行为向后兼容：仍返回该秒最后可见（含鉴别结论）的版本。
        default_view = evidence.snapshot(self.system, original["entity_id"], as_of=T0)
        self.assertEqual(default_view["version"], 2)
        self.assertEqual(default_view["write_seq"], appraised["write_seq"])
        # 最先可见：该秒第一次写入时的原始证据，没有后补的鉴别结论。
        first_view = evidence.snapshot(self.system, original["entity_id"], as_of=T0, mode="first")
        self.assertEqual(first_view["version"], 1)
        self.assertEqual(first_view["digest"], "sha256:origin")
        # 指定水位：移交接收方可凭水位精确取回当时掌握的版本。
        by_watermark = evidence.snapshot(self.system, original["entity_id"], as_of=T0, mode="watermark", watermark=original["write_seq"])
        self.assertEqual(by_watermark["digest"], "sha256:origin")
        self.assertEqual(
            evidence.snapshot(self.system, original["entity_id"], as_of=T0, mode="watermark", watermark=appraised["write_seq"])["digest"],
            "sha256:origin+appraisal",
        )
        # 该水位在更早的截止时点尚不可见。
        with self.assertRaises(NotFoundError):
            evidence.snapshot(self.system, original["entity_id"], as_of="2026-09-28T11:59:59+08:00", mode="watermark", watermark=appraised["write_seq"])

    # 同一秒内的保管交接与状态更正也按同一水位语义区分。
    def test_same_second_custody_transfer_and_correction(self):
        transfers = TransferService(self.app.repository)
        prepared = transfers.create(
            self.system,
            {"case_id": "case:ipr-1", "from_org": "org:ipr", "to_org": "org:court", "manifest": "evidence-list-1", "sent_at": T0},
            request_key="tr-1",
        )
        sent = transfers.transition(self.system, prepared["entity_id"], "sent", expected_version=1, reason="移交", request_key="tr-2")
        received = transfers.transition(self.system, prepared["entity_id"], "received", expected_version=2, reason="签收", request_key="tr-3")

        self.assertEqual(transfers.snapshot(self.system, prepared["entity_id"], as_of=T0, mode="first")["state"], "prepared")
        self.assertEqual(transfers.snapshot(self.system, prepared["entity_id"], as_of=T0)["state"], "received")
        self.assertEqual(
            transfers.snapshot(self.system, prepared["entity_id"], as_of=T0, mode="watermark", watermark=sent["write_seq"])["state"],
            "sent",
        )
        seqs = [row["write_seq"] for row in transfers.history(self.system, prepared["entity_id"])]
        self.assertEqual(seqs, [prepared["write_seq"], sent["write_seq"], received["write_seq"]])

        corrections = CorrectionService(self.app.repository)
        proposed = corrections.create(
            self.system,
            {"target_type": "evidence", "target_id": "evidence:1", "before_digest": "sha256:a", "after_digest": "sha256:b", "reason": "补录鉴别"},
            request_key="co-1",
        )
        corrections.transition(self.system, proposed["entity_id"], "review", expected_version=1, reason="复核", request_key="co-2")
        self.assertEqual(corrections.snapshot(self.system, proposed["entity_id"], as_of=T0, mode="first")["state"], "proposed")
        self.assertEqual(corrections.snapshot(self.system, proposed["entity_id"], as_of=T0)["state"], "review")

    # 截止时点之后还有更早分组时，first/last 作用于截止前最近一个业务时点。
    def test_first_and_last_apply_to_latest_visible_instant(self):
        service = CaseService(self.app.repository)
        created = service.create(self.system, CASE_VALUES, request_key="grp-1")
        service.revise(self.system, created["entity_id"], {"priority": "normal"}, expected_version=1, request_key="grp-2")
        later = CaseService(self.reopen(fixed_now=T2).repository)
        later.revise(self.system, created["entity_id"], {"priority": "low"}, expected_version=2, request_key="grp-3")

        # T1 截止：最近可见时点是 T0，该时点有 v1、v2 两个版本。
        self.assertEqual(service.snapshot(self.system, created["entity_id"], as_of=T1, mode="first")["version"], 1)
        self.assertEqual(service.snapshot(self.system, created["entity_id"], as_of=T1)["version"], 2)
        # T2 截止：最近可见时点是 T2，只有 v3。
        self.assertEqual(later.snapshot(self.system, created["entity_id"], as_of=T2, mode="first")["version"], 3)

    # 参数校验：未知模式、水位误用、不存在的水位都要给出明确错误。
    def test_snapshot_mode_validation(self):
        service = CaseService(self.app.repository)
        created = service.create(self.system, CASE_VALUES, request_key="val-1")
        other = service.create(self.system, {**CASE_VALUES, "subject": "另一案件"}, request_key="val-2")
        entity_id = created["entity_id"]

        with self.assertRaises(ValidationError):
            service.snapshot(self.system, entity_id, as_of=T0, mode="sideways")
        with self.assertRaises(ValidationError):
            service.snapshot(self.system, entity_id, as_of=T0, mode="last", watermark=1)
        with self.assertRaises(ValidationError):
            service.snapshot(self.system, entity_id, as_of=T0, mode="watermark")
        with self.assertRaises(ValidationError):
            service.snapshot(self.system, entity_id, as_of=T0, mode="watermark", watermark=0)
        with self.assertRaises(ValidationError):
            service.snapshot(self.system, entity_id, as_of=T0, mode="watermark", watermark=True)
        with self.assertRaises(NotFoundError):
            service.snapshot(self.system, entity_id, as_of=T0, mode="watermark", watermark=999)
        # 水位属于另一实体时不能张冠李戴。
        with self.assertRaises(NotFoundError):
            service.snapshot(self.system, entity_id, as_of=T0, mode="watermark", watermark=other["write_seq"])
        with self.assertRaises(NotFoundError):
            service.snapshot(self.system, entity_id, as_of="2026-09-28T11:59:59+08:00")

    # 幂等重放：相同请求键重放不制造新水位，重启后依然如此。
    def test_idempotent_replay_allocates_no_watermark(self):
        service = CaseService(self.app.repository)
        first = service.create(self.system, CASE_VALUES, request_key="idem-1")
        replay = service.create(self.system, CASE_VALUES, request_key="idem-1")
        self.assertEqual(replay["write_seq"], first["write_seq"])
        self.assertEqual(self.app.repository.current_watermark(), first["write_seq"])

        revised = service.revise(self.system, first["entity_id"], {"priority": "normal"}, expected_version=1, request_key="idem-2")
        replayed = service.revise(self.system, first["entity_id"], {"priority": "normal"}, expected_version=1, request_key="idem-2")
        self.assertEqual(replayed["write_seq"], revised["write_seq"])
        self.assertEqual(self.app.repository.current_watermark(), revised["write_seq"])
        self.assertEqual(len(service.history(self.system, first["entity_id"])), 2)

        # 重启后用同一请求键重放，仍返回原水位且不新增版本。
        service = CaseService(self.reopen().repository)
        again = service.create(self.system, CASE_VALUES, request_key="idem-1")
        self.assertEqual(again["write_seq"], first["write_seq"])
        self.assertEqual(self.app.repository.current_watermark(), revised["write_seq"])
        self.assertEqual(len(service.history(self.system, first["entity_id"])), 2)

    # 并发提交：水位全局唯一且连续；同一实体的乐观锁冲突不产生水位空洞以外的写入。
    def test_concurrent_commits_get_unique_watermarks(self):
        service = CaseService(self.app.repository)
        barrier = threading.Barrier(8)
        results: list[dict] = []
        errors: list[Exception] = []
        lock = threading.Lock()

        def worker(index: int) -> None:
            barrier.wait()
            for step in range(5):
                try:
                    row = service.create(self.system, CASE_VALUES, request_key=f"cc-{index}-{step}")
                    with lock:
                        results.append(row)
                except Exception as exc:  # pragma: no cover - 失败即测试失败
                    with lock:
                        errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        seqs = sorted(row["write_seq"] for row in results)
        self.assertEqual(len(seqs), 40)
        self.assertEqual(seqs, list(range(1, 41)))
        self.assertEqual(self.app.repository.current_watermark(), 40)

        # 同一实体的并发修订：恰好一个成功，水位仍然唯一。
        created = service.create(self.system, CASE_VALUES, request_key="cc-target")
        barrier = threading.Barrier(2)
        outcomes: list[object] = []

        def racer(tag: str) -> None:
            barrier.wait()
            try:
                outcomes.append(service.revise(self.system, created["entity_id"], {"priority": tag}, expected_version=1, request_key=f"cc-race-{tag}"))
            except ConflictError as exc:
                outcomes.append(exc)

        pair = [threading.Thread(target=racer, args=("a",)), threading.Thread(target=racer, args=("b",))]
        for thread in pair:
            thread.start()
        for thread in pair:
            thread.join()

        winners = [item for item in outcomes if isinstance(item, dict)]
        losers = [item for item in outcomes if isinstance(item, ConflictError)]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(losers), 1)
        all_seqs = {row["write_seq"] for row in service.history(self.system, created["entity_id"])}
        self.assertEqual(len(all_seqs), 2)

    # 服务重启：水位计数持久化，回放结果与重启前一致。
    def test_restart_replay_returns_consistent_results(self):
        service = CaseService(self.app.repository)
        created = service.create(self.system, CASE_VALUES, request_key="rs-1")
        revised = service.revise(self.system, created["entity_id"], {"priority": "normal"}, expected_version=1, request_key="rs-2")
        before_last = service.snapshot(self.system, created["entity_id"], as_of=T0)
        before_first = service.snapshot(self.system, created["entity_id"], as_of=T0, mode="first")
        before_history = service.history(self.system, created["entity_id"])
        watermark = self.app.repository.current_watermark()

        restarted = CaseService(self.reopen().repository)
        self.assertEqual(restarted.snapshot(self.system, created["entity_id"], as_of=T0), before_last)
        self.assertEqual(restarted.snapshot(self.system, created["entity_id"], as_of=T0, mode="first"), before_first)
        self.assertEqual(restarted.history(self.system, created["entity_id"]), before_history)
        self.assertEqual(self.app.repository.current_watermark(), watermark)

        continued = restarted.create(self.system, {**CASE_VALUES, "subject": "重启后新案件"}, request_key="rs-3")
        self.assertEqual(continued["write_seq"], watermark + 1)
        self.assertEqual(revised["write_seq"], watermark)

    # 审计链：条目携带与版本库一致的水位，顺序语义相同，链可校验。
    def test_audit_chain_uses_same_watermark_order(self):
        service = CaseService(self.app.repository)
        created = service.create(self.system, CASE_VALUES, request_key="au-1")
        revised = service.revise(self.system, created["entity_id"], {"priority": "normal"}, expected_version=1, request_key="au-2")

        with self.app.database.connect() as connection:
            entries = connection.execute("SELECT * FROM audit_entries ORDER BY audit_id").fetchall()
        self.assertEqual(len(entries), 2)
        detail_seqs = [json.loads(entry["detail_json"])["write_seq"] for entry in entries]
        history_seqs = [row["write_seq"] for row in service.history(self.system, created["entity_id"])]
        self.assertEqual(detail_seqs, history_seqs)
        self.assertEqual(detail_seqs, sorted(detail_seqs))
        self.assertEqual(self.app.verify()["audit_entries"], 2)

    # 字段裁剪：历史与各模式时点查询都按同一水位顺序裁剪受限字段。
    def test_redaction_follows_same_ordering(self):
        evidence = EvidenceService(self.app.repository)
        created = evidence.create(self.system, EVIDENCE_VALUES, request_key="rd-1")
        evidence.revise(self.system, created["entity_id"], {"digest": "sha256:updated"}, expected_version=1, request_key="rd-2")
        reader = AccessContext(actor_id="reader", permissions=frozenset({"read:evidence", "history:evidence"}))

        history = evidence.history(reader, created["entity_id"])
        self.assertEqual([row["source"] for row in history], ["***", "***"])
        self.assertEqual([row["write_seq"] for row in history], sorted(row["write_seq"] for row in history))
        for mode in ("first", "last"):
            view = evidence.snapshot(reader, created["entity_id"], as_of=T0, mode=mode)
            self.assertEqual(view["source"], "***")
        view = evidence.snapshot(reader, created["entity_id"], as_of=T0, mode="watermark", watermark=created["write_seq"])
        self.assertEqual(view["source"], "***")

        transfers = TransferService(self.app.repository)
        transfer = transfers.create(
            self.system,
            {"case_id": "case:ipr-1", "from_org": "org:ipr", "to_org": "org:court", "manifest": "secret-list", "sent_at": T0},
            request_key="rd-3",
        )
        transfer_reader = AccessContext(actor_id="reader", permissions=frozenset({"read:transfers", "history:transfers"}))
        self.assertEqual(transfers.snapshot(transfer_reader, transfer["entity_id"], as_of=T0, mode="first")["manifest"], "***")


class LegacyMigrationTest(unittest.TestCase):
    """旧库迁移：既有历史记录保留，旧数据按插入顺序获得确定水位。"""

    LEGACY_DDL = """
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
    """

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "legacy.sqlite3"
        self.system = AccessContext.system("tester")
        self._build_legacy_db()

    def tearDown(self):
        self.temp.cleanup()

    def _insert_version(self, connection, entity_id, version, state, mark, valid_from):
        payload = {"mark": mark, "state": state}
        connection.execute(
            "INSERT INTO entity_versions(entity_type,entity_id,version,state,payload_json,valid_from,actor_id,request_key) VALUES(?,?,?,?,?,?,?,?)",
            ("cases", entity_id, version, state, canonical_json(payload), valid_from, "legacy-writer", f"legacy-{entity_id}-{version}"),
        )
        connection.execute(
            "INSERT INTO entities(entity_type,entity_id,version,state,payload_json,created_at,updated_at,created_by,updated_by) VALUES(?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(entity_type,entity_id) DO UPDATE SET version=excluded.version,state=excluded.state,payload_json=excluded.payload_json,updated_at=excluded.updated_at",
            ("cases", entity_id, version, state, canonical_json(payload), valid_from, valid_from, "legacy-writer", "legacy-writer"),
        )

    def _build_legacy_db(self):
        connection = sqlite3.connect(self.path)
        try:
            connection.executescript(self.LEGACY_DDL)
            t0 = "2026-09-28T04:00:00Z"
            # 交错插入两个实体的版本；case:b 的业务时间倒退，证明水位按插入顺序而非业务时间分配。
            self._insert_version(connection, "case:a", 1, "draft", "a1", t0)
            self._insert_version(connection, "case:a", 2, "draft", "a2", t0)  # 与 v1 同秒
            self._insert_version(connection, "case:b", 1, "draft", "b1", "2026-09-28T04:00:10Z")
            self._insert_version(connection, "case:a", 3, "open", "a3", "2026-09-28T04:00:05Z")
            self._insert_version(connection, "case:b", 2, "open", "b2", "2026-09-28T04:00:02Z")
            connection.commit()
        finally:
            connection.close()

    def test_legacy_history_gets_deterministic_watermarks(self):
        app = CivicFlow.open(self.path, fixed_now=T0)
        repository = app.repository
        service = CaseService(repository)

        history_a = service.history(self.system, "case:a")
        history_b = service.history(self.system, "case:b")
        self.assertEqual([row["write_seq"] for row in history_a], [1, 2, 4])
        self.assertEqual([row["write_seq"] for row in history_b], [3, 5])
        self.assertEqual([row["version"] for row in history_a], [1, 2, 3])
        self.assertEqual([row["version"] for row in history_b], [1, 2])
        # 当前记录也带上水位。
        self.assertEqual(service.get(self.system, "case:a")["write_seq"], 4)
        self.assertEqual(service.get(self.system, "case:b")["write_seq"], 5)
        self.assertEqual(repository.current_watermark(), 5)

        t0 = "2026-09-28T04:00:00Z"
        # 默认行为与旧语义一致：同秒组内取最后写入（a2）。
        self.assertEqual(service.snapshot(self.system, "case:a", as_of=t0)["mark"], "a2")
        # 新能力：同秒最先可见（a1）与指定水位。
        self.assertEqual(service.snapshot(self.system, "case:a", as_of=t0, mode="first")["mark"], "a1")
        self.assertEqual(service.snapshot(self.system, "case:a", as_of=t0, mode="watermark", watermark=2)["mark"], "a2")
        self.assertEqual(service.snapshot(self.system, "case:a", as_of=t0, mode="watermark", watermark=1)["mark"], "a1")
        # 业务时间倒退的旧数据：按水位排序仍是真实写入顺序。
        self.assertEqual(service.snapshot(self.system, "case:b", as_of="2026-09-28T04:00:10Z")["mark"], "b1")

        # 迁移后继续写入，水位从旧数据最大值之后连续推进。
        created = service.create(self.system, CASE_VALUES, request_key="legacy-new")
        self.assertEqual(created["write_seq"], 6)

        # 迁移幂等：再次打开不改变既有水位与计数。
        reopened = CivicFlow.open(self.path, fixed_now=T0)
        service = CaseService(reopened.repository)
        self.assertEqual([row["write_seq"] for row in service.history(self.system, "case:a")], [1, 2, 4])
        self.assertEqual(reopened.repository.current_watermark(), 6)
        self.assertEqual(reopened.verify()["audit_entries"], 1)


if __name__ == "__main__":
    unittest.main()
