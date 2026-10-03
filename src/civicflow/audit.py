"""追加式审计链。

审计行按全局提交水位（commit_seq）串接哈希链。水位在数据库内分配，
同一业务时间内的多次写入也有确定先后，验链顺序因此在并发提交和服务
重启后保持一致。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass

from .errors import InvariantViolation
from .jsonutil import canonical_json
from .timeutil import Clock
from .watermarks import next_watermark


def compute_entry_digest(
    *,
    occurred_at: str,
    actor_id: str,
    action: str,
    entity_type: str,
    entity_id: str,
    version: int,
    commit_seq: int,
    detail: dict,
    previous: str,
) -> str:
    """按统一规范计算单条审计摘要（迁移与运行期共用）。"""
    body = canonical_json({
        "occurred_at": occurred_at,
        "actor_id": actor_id,
        "action": action,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "version": version,
        "commit_seq": commit_seq,
        "detail": detail,
        "previous": previous,
    })
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class AuditLog:
    clock: Clock

    def append(self, connection: sqlite3.Connection, *, actor_id: str, action: str, entity_type: str, entity_id: str, version: int, detail: dict, commit_seq: int | None = None) -> tuple[str, int]:
        if commit_seq is None:
            commit_seq = next_watermark(connection)
        row = connection.execute(
            "SELECT entry_digest FROM audit_entries WHERE commit_seq=(SELECT MAX(commit_seq) FROM audit_entries)"
        ).fetchone()
        previous = row["entry_digest"] if row else "0" * 64
        occurred_at = self.clock.now()
        digest = compute_entry_digest(
            occurred_at=occurred_at,
            actor_id=actor_id,
            action=action,
            entity_type=entity_type,
            entity_id=entity_id,
            version=version,
            commit_seq=commit_seq,
            detail=detail,
            previous=previous,
        )
        connection.execute(
            "INSERT INTO audit_entries(occurred_at,actor_id,action,entity_type,entity_id,version,detail_json,previous_digest,entry_digest,commit_seq) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (occurred_at, actor_id, action, entity_type, entity_id, version, canonical_json(detail), previous, digest, commit_seq),
        )
        return digest, commit_seq

    def verify(self, connection: sqlite3.Connection) -> int:
        previous = "0" * 64
        count = 0
        seen: set[int] = set()
        for row in connection.execute("SELECT * FROM audit_entries ORDER BY commit_seq"):
            if row["commit_seq"] in seen:
                raise InvariantViolation(f"审计链在水位 {row['commit_seq']} 处重复")
            seen.add(row["commit_seq"])
            expected = compute_entry_digest(
                occurred_at=row["occurred_at"],
                actor_id=row["actor_id"],
                action=row["action"],
                entity_type=row["entity_type"],
                entity_id=row["entity_id"],
                version=row["version"],
                commit_seq=row["commit_seq"],
                detail=json.loads(row["detail_json"]),
                previous=previous,
            )
            if row["previous_digest"] != previous or row["entry_digest"] != expected:
                raise InvariantViolation(f"审计链在水位 {row['commit_seq']} 处不连续")
            previous = expected
            count += 1
        return count
