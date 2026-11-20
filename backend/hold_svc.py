"""旁路暂存：关窗切版时新到事件可进 hold。

正确性口径（见同目录 hold_visibility_pact.json「暂存可见册」，当日禁止改口）：
- 择定甲路径：create_hold 与列表同读 hold_staging 实表，写后同库提交即可见，
  禁止写后不可见；不引入 hold_shadow，禁止双源混用。
- 两人同时写同窗暂存须占 hold:{window_name} 暂存锁，后到只记争用不写业务。
- 偏离可见册的写入一律拒写并审计「可见偏了」。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException

from auth import AuthHeader, require_user
from db import connect, write_audit
from locks import current_holder, record_contention, release, try_acquire
from models import HoldIn

router = APIRouter(tags=["hold"])

_PACT_PATH = Path(__file__).resolve().parent / "hold_visibility_pact.json"


def _load_visibility_pact() -> dict:
    """读暂存可见册；册缺失即视为可见口径无据，拒绝猜测式写入。"""
    try:
        pact = json.loads(_PACT_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:  # pragma: no cover - 册在仓内
        raise RuntimeError(f"暂存可见册不可读，拒绝写入: {exc}") from exc
    required = ("path", "write_table", "read_table")
    if any(k not in pact for k in required):
        raise RuntimeError("暂存可见册缺项，拒绝写入")
    return pact


# 进程启动即载入可见册；当日口径以此为准（测试可用 monkeypatch 模拟漂移）
VISIBILITY_PACT = _load_visibility_pact()


def _assert_pact_followed(conn, user: str) -> None:
    """偏离可见册（如被改成影子表路径）：拒写并记 hold_visibility_drift。"""
    if (
        VISIBILITY_PACT.get("path") != "甲"
        or VISIBILITY_PACT.get("write_table") != "hold_staging"
        or VISIBILITY_PACT.get("read_table") != "hold_staging"
        or VISIBILITY_PACT.get("shadow_table")
    ):
        write_audit(
            conn,
            user,
            "hold_visibility_drift",
            None,
            {"pact": VISIBILITY_PACT, "detail": "可见偏了"},
        )
        conn.commit()
        raise HTTPException(409, "可见偏了：偏离暂存可见册，拒写")


@router.get("/api/holds")
def list_holds(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        # 甲路径：列表与 create_hold 同读 hold_staging 实表，读写同源
        rows = conn.execute(
            f"""SELECT h.*, w.name AS window_name
                FROM {VISIBILITY_PACT['read_table']} h
                JOIN windows w ON w.id=h.window_id
                ORDER BY h.id DESC LIMIT 200"""
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.post("/api/holds")
def create_hold(body: HoldIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    lock_key = f"hold:{body.window_name}"

    # 第一步：纯读判锁（不启写事务、不阻塞）。锁已被他人持有——
    # 典型「后到」场景——独立连接登记争用并 409，业务笔完全不写。
    with connect() as probe:
        holder = current_holder(probe, lock_key)
    if holder is not None and holder != user:
        record_contention(lock_key, user, f"持有者={holder}，后到争用")
        raise HTTPException(409, "争用：后到只记争用，不写业务")

    # 第二步：零超时抢占式 BEGIN IMMEDIATE。两人并发同时通过第一步时，
    # 只有一人抢到写锁，另一人立即 SQLITE_BUSY——独立连接登记争用并 409。
    try:
        conn = connect(immediate=True, busy_timeout=0)
    except sqlite3.OperationalError as exc:
        if "locked" in str(exc).lower():
            record_contention(lock_key, user, "同窗并发抢写锁，后到争用")
            raise HTTPException(409, "争用：后到只记争用，不写业务") from exc
        raise

    with conn:
        _assert_pact_followed(conn, user)
        try:
            # 写锁已在手，UPSERT 原子占应用锁；极端情况下锁行刚被他人写入，
            # 仍判争用（独立连接登记，与业务事务隔离）
            if not try_acquire(conn, lock_key, user, external_log=True):
                conn.commit()
                raise HTTPException(409, "争用：后到只记争用，不写业务")
            w = conn.execute(
                "SELECT * FROM windows WHERE name=?", (body.window_name,)
            ).fetchone()
            if not w:
                raise HTTPException(404, "窗口不存在")
            now = datetime.now().isoformat(timespec="seconds")
            cur = conn.execute(
                f"""INSERT INTO {VISIBILITY_PACT['write_table']}
                       (window_id,biz_key,event_ts,amount,status,created_at)
                   VALUES (?,?,?,?,?,?)""",
                (w["id"], body.biz_key, body.event_ts, body.amount, "暂存中", now),
            )
            write_audit(
                conn,
                user,
                "hold_create",
                body.biz_key,
                {"window_id": w["id"], "hold_id": cur.lastrowid},
            )
            conn.commit()
            return {"ok": True, "hold_id": cur.lastrowid, "status": "暂存中"}
        finally:
            release(conn, lock_key, user)
            conn.commit()


@router.post("/api/holds/{hold_id}/release-stub")
def release_hold_stub(hold_id: int, authorization: AuthHeader = None):
    """空桩：暂存晋升正式或整笔回滚，写路径待题面择一。"""
    user = require_user(authorization)
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM hold_staging WHERE id=?", (hold_id,)
        ).fetchone()
        if not row:
            raise HTTPException(404, "暂存不存在")
        write_audit(
            conn,
            user,
            "hold_release_stub",
            str(hold_id),
            {
                "paths": ["晋升正式同事务", "作废暂存零改正式"],
                "note": "母仓不落地，仅登记",
            },
        )
        conn.commit()
    return {"ok": True, "stub": True}
