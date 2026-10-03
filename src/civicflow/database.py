"""SQLite 连接、事务、数据库初始化与旧库迁移。"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .audit import compute_entry_digest
from .watermarks import create_tables, seed_watermark


SCHEMA = r"""
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS entities (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    commit_seq INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(entity_type, entity_id)
);
CREATE TABLE IF NOT EXISTS entity_versions (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    commit_seq INTEGER NOT NULL,
    PRIMARY KEY(entity_type, entity_id, version)
);
CREATE TABLE IF NOT EXISTS commit_watermark (
    seq_key TEXT PRIMARY KEY,
    next_value INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS idempotency_keys (
    scope TEXT NOT NULL,
    request_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, request_key)
);
CREATE TABLE IF NOT EXISTS audit_entries (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    detail_json TEXT NOT NULL,
    previous_digest TEXT NOT NULL,
    entry_digest TEXT NOT NULL,
    commit_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS inbox_messages (
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    payload_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY(source, source_key, sequence)
);
CREATE TABLE IF NOT EXISTS inbox_conflicts (
    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    existing_digest TEXT NOT NULL,
    incoming_digest TEXT NOT NULL,
    received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox_messages (
    message_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    available_at TEXT NOT NULL,
    lease_until TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    delivered_at TEXT
);
CREATE INDEX IF NOT EXISTS outbox_ready ON outbox_messages(status, available_at, lease_until);
CREATE TABLE IF NOT EXISTS journal_entries (
    entry_id TEXT PRIMARY KEY,
    journal_key TEXT NOT NULL,
    account TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    direction TEXT NOT NULL,
    reference TEXT NOT NULL,
    reversed_entry_id TEXT,
    occurred_at TEXT NOT NULL,
    posted_by TEXT NOT NULL,
    FOREIGN KEY(reversed_entry_id) REFERENCES journal_entries(entry_id)
);
CREATE INDEX IF NOT EXISTS journal_reference ON journal_entries(journal_key, reference, occurred_at);
CREATE TABLE IF NOT EXISTS resource_reservations (
    reservation_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    status TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_by TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reservation_window ON resource_reservations(resource_id, start_at, end_at, status);
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    run_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    lease_until TEXT,
    last_error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS jobs_due ON scheduled_jobs(status, run_at, lease_until);
"""


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}


def _migrate_commit_order(connection: sqlite3.Connection) -> None:
    """为缺少水位列的旧库补齐确定顺序。

    旧版本按 (业务时间, 实体内版本号, 实体类型, 实体标识) 升序分配全局
    水位，使共享同一业务时间的写入也具有稳定、可复现的先后；随后按同一
    顺序重建审计哈希链。迁移幂等：重复执行不会产生新水位，也不会改变
    既有摘要。
    """
    if "commit_seq" not in _columns(connection, "entities"):
        connection.execute("ALTER TABLE entities ADD COLUMN commit_seq INTEGER NOT NULL DEFAULT 0")

    version_legacy = "commit_seq" not in _columns(connection, "entity_versions")
    if version_legacy:
        connection.execute("ALTER TABLE entity_versions ADD COLUMN commit_seq INTEGER")
        connection.execute(
            "CREATE INDEX IF NOT EXISTS entity_versions_commit ON entity_versions(entity_type, entity_id, valid_from, commit_seq)"
        )
    audit_legacy = "commit_seq" not in _columns(connection, "audit_entries")
    if audit_legacy:
        connection.execute("ALTER TABLE audit_entries ADD COLUMN commit_seq INTEGER")
        connection.execute("CREATE INDEX IF NOT EXISTS audit_chain_order ON audit_entries(commit_seq)")
    create_tables(connection)
    connection.execute(
        "CREATE INDEX IF NOT EXISTS entity_versions_commit ON entity_versions(entity_type, entity_id, valid_from, commit_seq)"
    )
    connection.execute("CREATE INDEX IF NOT EXISTS audit_chain_order ON audit_entries(commit_seq)")
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS entity_versions_commit_seq_uq ON entity_versions(commit_seq)"
    )
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS audit_entries_commit_seq_uq ON audit_entries(commit_seq)"
    )

    pending = connection.execute(
        "SELECT EXISTS(SELECT 1 FROM entity_versions WHERE commit_seq IS NULL) AS n"
    ).fetchone()["n"]
    if not version_legacy and not audit_legacy and not pending:
        return

    rows = connection.execute(
        "SELECT entity_type, entity_id, version, valid_from FROM entity_versions "
        "WHERE commit_seq IS NULL ORDER BY valid_from, version, entity_type, entity_id"
    ).fetchall()
    max_seq = 0
    for seq, row in enumerate(rows, start=1):
        connection.execute(
            "UPDATE entity_versions SET commit_seq=? WHERE entity_type=? AND entity_id=? AND version=?",
            (seq, row["entity_type"], row["entity_id"], row["version"]),
        )
        max_seq = seq
    current = connection.execute("SELECT COALESCE(MAX(commit_seq),0) AS m FROM entity_versions").fetchone()["m"]
    seed_watermark(connection, max(current, max_seq))

    # 当前实体指针水位回填为该实体最新版本的水位。
    connection.execute(
        "UPDATE entities SET commit_seq=COALESCE(("
        "SELECT MAX(v.commit_seq) FROM entity_versions v "
        "WHERE v.entity_type=entities.entity_type AND v.entity_id=entities.entity_id), 0) "
        "WHERE commit_seq=0"
    )

    # 审计行与版本行一一对应（每次写库同时产生），借实体三元组确定水位；
    # 个别无法对应的旧行按发生时间与原自增主键追加在末尾。
    version_seq = {
        (row["entity_type"], row["entity_id"], row["version"]): row["commit_seq"]
        for row in connection.execute(
            "SELECT entity_type, entity_id, version, commit_seq FROM entity_versions"
        )
    }
    audit_rows = connection.execute(
        "SELECT audit_id, occurred_at, actor_id, action, entity_type, entity_id, version, detail_json "
        "FROM audit_entries WHERE commit_seq IS NULL"
    ).fetchall()
    matched: list[tuple[int, sqlite3.Row]] = []
    unmatched: list[sqlite3.Row] = []
    for row in audit_rows:
        seq = version_seq.get((row["entity_type"], row["entity_id"], row["version"]))
        (matched if seq is not None else unmatched).append((seq, row) if seq is not None else row)
    unmatched.sort(key=lambda r: (r["occurred_at"], r["audit_id"]))
    ordered: list[tuple[int, sqlite3.Row]] = list(matched)
    ordered.sort(key=lambda item: item[0])
    next_seq = max(current, max_seq, len(matched))
    for row in unmatched:
        next_seq += 1
        ordered.append((next_seq, row))

    previous = "0" * 64
    for seq, row in ordered:
        detail = json.loads(row["detail_json"])
        digest = compute_entry_digest(
            occurred_at=row["occurred_at"],
            actor_id=row["actor_id"],
            action=row["action"],
            entity_type=row["entity_type"],
            entity_id=row["entity_id"],
            version=row["version"],
            commit_seq=seq,
            detail=detail,
            previous=previous,
        )
        connection.execute(
            "UPDATE audit_entries SET commit_seq=?, previous_digest=?, entry_digest=? WHERE audit_id=?",
            (seq, previous, digest, row["audit_id"]),
        )
        previous = digest
    if ordered:
        seed_watermark(connection, next_seq)


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(SCHEMA)
            _migrate_commit_order(connection)

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
