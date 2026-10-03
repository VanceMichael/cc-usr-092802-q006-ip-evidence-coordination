"""版本化实体持久化。

每个实体版本在写入事务内取得一个数据库分配的全局提交水位
``commit_seq``：即使多个版本共享同一业务时间（valid_from 相同到秒），
水位仍提供稳定、可持久化的先后顺序。历史、截止时点回放、列表和审计链
全部以水位为同一排序语义。
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, NotFoundError, ValidationError
from .identifiers import new_id, require_safe
from .idempotency import IdempotencyStore
from .jsonutil import canonical_json
from .timeutil import Clock, canonical_instant
from .watermarks import SequenceMode, current_watermark, next_watermark, resolve_mode


@dataclass(frozen=True)
class EntityRepository:
    database: Database
    clock: Clock
    audit: AuditLog
    idempotency: IdempotencyStore

    def create(self, entity_type: str, payload: dict, *, actor: str, request_key: str) -> dict:
        require_safe(entity_type, "实体类型")
        with self.database.transaction() as connection:
            def operation() -> dict:
                entity_id = new_id(entity_type)
                now = self.clock.now()
                commit_seq = next_watermark(connection)
                state = str(payload.get("state", "draft"))
                body = dict(payload)
                body["state"] = state
                connection.execute("INSERT INTO entities(entity_type,entity_id,version,state,payload_json,created_at,updated_at,created_by,updated_by,commit_seq) VALUES(?,?,?,?,?,?,?,?,?,?)", (entity_type, entity_id, 1, state, canonical_json(body), now, now, actor, actor, commit_seq))
                connection.execute("INSERT INTO entity_versions(entity_type,entity_id,version,state,payload_json,valid_from,actor_id,request_key,commit_seq) VALUES(?,?,?,?,?,?,?,?,?)", (entity_type, entity_id, 1, state, canonical_json(body), now, actor, request_key, commit_seq))
                self.audit.append(connection, actor_id=actor, action="create", entity_type=entity_type, entity_id=entity_id, version=1, detail=body, commit_seq=commit_seq)
                return self._row_to_dict(connection.execute("SELECT * FROM entities WHERE entity_type=? AND entity_id=?", (entity_type, entity_id)).fetchone())
            return self.idempotency.execute(connection, scope=f"create:{entity_type}", request_key=request_key, request=payload, operation=operation)

    def update(self, entity_type: str, entity_id: str, changes: dict, *, actor: str, expected_version: int, request_key: str) -> dict:
        if not changes:
            raise ValidationError("修改内容不能为空")
        with self.database.transaction() as connection:
            def operation() -> dict:
                row = connection.execute("SELECT * FROM entities WHERE entity_type=? AND entity_id=?", (entity_type, entity_id)).fetchone()
                if not row:
                    raise NotFoundError(f"{entity_type}/{entity_id} 不存在")
                if row["version"] != expected_version:
                    raise ConflictError(f"版本冲突，当前为 {row['version']}")
                payload = json.loads(row["payload_json"]); payload.update(changes)
                version = expected_version + 1
                state = str(payload.get("state", row["state"]))
                now = self.clock.now()
                commit_seq = next_watermark(connection)
                changed = connection.execute("UPDATE entities SET version=?,state=?,payload_json=?,updated_at=?,updated_by=?,commit_seq=? WHERE entity_type=? AND entity_id=? AND version=?", (version, state, canonical_json(payload), now, actor, commit_seq, entity_type, entity_id, expected_version)).rowcount
                if changed != 1:
                    raise ConflictError("并发修改导致版本变化")
                connection.execute("INSERT INTO entity_versions(entity_type,entity_id,version,state,payload_json,valid_from,actor_id,request_key,commit_seq) VALUES(?,?,?,?,?,?,?,?,?)", (entity_type, entity_id, version, state, canonical_json(payload), now, actor, request_key, commit_seq))
                self.audit.append(connection, actor_id=actor, action="update", entity_type=entity_type, entity_id=entity_id, version=version, detail=changes, commit_seq=commit_seq)
                return self._row_to_dict(connection.execute("SELECT * FROM entities WHERE entity_type=? AND entity_id=?", (entity_type, entity_id)).fetchone())
            return self.idempotency.execute(connection, scope=f"update:{entity_type}:{entity_id}", request_key=request_key, request={"changes": changes, "expected_version": expected_version}, operation=operation)

    def get(self, entity_type: str, entity_id: str) -> dict:
        with self.database.connect() as connection:
            row = connection.execute("SELECT * FROM entities WHERE entity_type=? AND entity_id=?", (entity_type, entity_id)).fetchone()
            if not row:
                raise NotFoundError(f"{entity_type}/{entity_id} 不存在")
            return self._row_to_dict(row)

    def list(self, entity_type: str, *, state: str | None = None, limit: int = 100) -> list[dict]:
        if limit < 1 or limit > 500:
            raise ValidationError("limit 必须在 1 到 500 之间")
        sql = "SELECT * FROM entities WHERE entity_type=?"; params: list[object] = [entity_type]
        if state is not None:
            sql += " AND state=?"; params.append(state)
        sql += " ORDER BY commit_seq, entity_id LIMIT ?"; params.append(limit)
        with self.database.connect() as connection:
            return [self._row_to_dict(row) for row in connection.execute(sql, params)]

    def search(self, entity_type: str, field: str, value: object, *, limit: int = 100) -> list[dict]:
        rows = self.list(entity_type, limit=500)
        return [row for row in rows if row.get(field) == value][:limit]

    def history(self, entity_type: str, entity_id: str) -> list[dict]:
        """按提交水位升序返回版本，业务时间相同时顺序仍然确定。"""
        with self.database.connect() as connection:
            rows = connection.execute("SELECT * FROM entity_versions WHERE entity_type=? AND entity_id=? ORDER BY commit_seq", (entity_type, entity_id)).fetchall()
            return [self._version_to_dict(row) for row in rows]

    def snapshot(
        self,
        entity_type: str,
        entity_id: str,
        *,
        as_of: str,
        bound: str | SequenceMode | None = None,
        watermark: int | None = None,
    ) -> dict:
        """截止时点回放。

        ``bound`` 取 ``last``（默认，向后兼容，返回该时点最后可见版本）、
        ``first``（该时点最先可见版本）或 ``at``（配合 ``watermark`` 精
        确选择指定水位版本）。
        """
        instant = canonical_instant(as_of)
        mode = resolve_mode(bound, watermark)
        with self.database.connect() as connection:
            if mode.bound == "at":
                row = connection.execute(
                    "SELECT * FROM entity_versions WHERE entity_type=? AND entity_id=? "
                    "AND valid_from<=? AND commit_seq=?",
                    (entity_type, entity_id, instant, mode.watermark),
                ).fetchone()
            else:
                direction = "ASC" if mode.bound == "first" else "DESC"
                row = connection.execute(
                    f"SELECT * FROM entity_versions WHERE entity_type=? AND entity_id=? "
                    f"AND valid_from<=? ORDER BY commit_seq {direction} LIMIT 1",
                    (entity_type, entity_id, instant),
                ).fetchone()
            if not row:
                raise NotFoundError("指定时点没有可见版本")
            return self._version_to_dict(row)

    def snapshot_at_watermark(self, entity_type: str, entity_id: str, *, watermark: int) -> dict:
        """返回给定全局水位下该实体可见的版本（不晚于该水位的最新版本）。

        移交方可据此证明某份材料在指定水位时是否已经可见：若材料首版本
        的水位不大于给定水位，即已可见。水位 0 表示首个写入之前，必然
        不可见。
        """
        if isinstance(watermark, bool) or not isinstance(watermark, int) or watermark < 0:
            raise ValidationError("水位必须是非负整数")
        with self.database.connect() as connection:
            row = connection.execute(
                "SELECT * FROM entity_versions WHERE entity_type=? AND entity_id=? "
                "AND commit_seq<=? ORDER BY commit_seq DESC LIMIT 1",
                (entity_type, entity_id, watermark),
            ).fetchone()
            if not row:
                raise NotFoundError("指定水位时该实体尚不存在")
            return self._version_to_dict(row)

    def head_watermark(self) -> int:
        """当前已提交的全局水位。"""
        with self.database.connect() as connection:
            return current_watermark(connection)

    @staticmethod
    def _row_to_dict(row) -> dict:
        payload = json.loads(row["payload_json"]); payload.update({"entity_type": row["entity_type"], "entity_id": row["entity_id"], "version": row["version"], "state": row["state"], "created_at": row["created_at"], "updated_at": row["updated_at"], "created_by": row["created_by"], "updated_by": row["updated_by"], "commit_seq": row["commit_seq"]}); return payload

    @staticmethod
    def _version_to_dict(row) -> dict:
        payload = json.loads(row["payload_json"]); payload.update({"entity_type": row["entity_type"], "entity_id": row["entity_id"], "version": row["version"], "state": row["state"], "valid_from": row["valid_from"], "actor_id": row["actor_id"], "request_key": row["request_key"], "commit_seq": row["commit_seq"]}); return payload
