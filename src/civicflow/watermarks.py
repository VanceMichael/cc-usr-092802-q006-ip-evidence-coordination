"""全局提交水位。

每次写入在事务内取得一个持久化的、单调递增的整数水位；即使多笔写入
共享同一业务时间（valid_from 相同到秒甚至更细粒度），水位也给出稳定、
可复现的先后顺序。水位完全由 SQLite 序列表分配，不依赖进程内计数，
因此多个连接并发写入或服务重启后仍然连续一致。

截止时点回放支持三种边界：

- ``last``（默认，向后兼容）：时点上最后可见的版本；
- ``first``：时点上最先可见的版本，用于证明某时刻“当时已经掌握”的材料；
- ``at``：精确指定水位的版本。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from .errors import ValidationError

SEQUENCE_KEY = "entity_versions"

FIRST = "first"
LAST = "last"
AT = "at"
BOUNDS = (FIRST, LAST, AT)


@dataclass(frozen=True)
class SequenceMode:
    """截止时点查询的边界选择。"""

    bound: str = LAST
    watermark: int | None = None

    @classmethod
    def first(cls) -> "SequenceMode":
        return cls(bound=FIRST)

    @classmethod
    def last(cls) -> "SequenceMode":
        return cls(bound=LAST)

    @classmethod
    def at(cls, watermark: int) -> "SequenceMode":
        watermark = require_watermark(watermark)
        return cls(bound=AT, watermark=watermark)


def require_watermark(value: object) -> int:
    """校验并返回正整数水位。"""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError("水位必须是正整数")
    if value <= 0:
        raise ValidationError("水位必须是正整数")
    return value


def create_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        "CREATE TABLE IF NOT EXISTS commit_watermark (seq_key TEXT PRIMARY KEY, next_value INTEGER NOT NULL) WITHOUT ROWID"
    )


def next_watermark(connection: sqlite3.Connection) -> int:
    """在当前事务内分配下一个全局水位。

    调用方持有写事务（BEGIN IMMEDIATE），对单行的 UPSERT 会被写锁
    串行化，因此并发提交绝不会拿到相同水位。
    """
    create_tables(connection)
    connection.execute(
        "INSERT INTO commit_watermark(seq_key,next_value) VALUES(?,1) "
        "ON CONFLICT(seq_key) DO UPDATE SET next_value=next_value+1",
        (SEQUENCE_KEY,),
    )
    row = connection.execute(
        "SELECT next_value FROM commit_watermark WHERE seq_key=?", (SEQUENCE_KEY,)
    ).fetchone()
    return int(row["next_value"])


def current_watermark(connection: sqlite3.Connection) -> int:
    """返回已分配的最大水位；尚未写入时为 0。"""
    create_tables(connection)
    row = connection.execute(
        "SELECT next_value FROM commit_watermark WHERE seq_key=?", (SEQUENCE_KEY,)
    ).fetchone()
    return int(row["next_value"]) if row else 0


def seed_watermark(connection: sqlite3.Connection, value: int) -> None:
    """迁移时把序列定位到已回填的最大水位（单调推进，不回退）。"""
    connection.execute(
        "INSERT INTO commit_watermark(seq_key,next_value) VALUES(?,?) "
        "ON CONFLICT(seq_key) DO UPDATE SET next_value=MAX(excluded.next_value, next_value)",
        (SEQUENCE_KEY, int(value)),
    )


def resolve_mode(
    bound: str | SequenceMode | None,
    watermark: int | None = None,
) -> SequenceMode:
    """把对外参数归一成 SequenceMode。"""
    if isinstance(bound, SequenceMode):
        mode = bound
        if watermark is not None and mode.watermark != require_watermark(watermark):
            raise ValidationError("边界水位参数冲突")
        return mode
    if bound is None:
        bound = LAST
    if bound not in BOUNDS:
        raise ValidationError("边界必须是 first、last 或 at")
    if bound == AT:
        if watermark is None:
            raise ValidationError("at 边界必须提供水位")
        return SequenceMode.at(watermark)
    if watermark is not None:
        raise ValidationError(f"{bound} 边界不接受水位")
    return SequenceMode(bound=bound)
