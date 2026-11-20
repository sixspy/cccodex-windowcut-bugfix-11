"""窗口锁与争用登记（公共并发面，题面争用句应对得上）。"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from auth import AuthHeader, require_user
from db import connect, write_audit

router = APIRouter(tags=["locks"])


class LockIn(BaseModel):
    lock_key: str
    action: str = "acquire"


def record_contention(lock_key: str, actor: str, detail: str) -> None:
    """用独立短连接登记争用并提交。

    供「本连接开不了写事务」的后到者使用。SQLite 同一时刻只许一个写者，
    故以短 busy_timeout 排队等先到者提交（持锁临界区很短），拿到写锁后
    落账，确保争用记录独立于被拒请求、不随其回滚。
    """
    now = datetime.now().isoformat(timespec="seconds")
    with connect(busy_timeout=2) as conn:
        conn.execute(
            """INSERT INTO contention_log(lock_key,actor,action,detail,created_at)
               VALUES (?,?,?,?,?)""",
            (lock_key, actor, "争用", detail, now),
        )
        conn.commit()


def current_holder(conn, lock_key: str):
    """纯读当前持锁者，不开启写事务（deferred 连接上安全）。"""
    row = conn.execute(
        "SELECT holder FROM window_locks WHERE lock_key=?", (lock_key,)
    ).fetchone()
    return row["holder"] if row else None


def try_acquire(
    conn,
    lock_key: str,
    holder: str,
    external_log: bool = False,
) -> bool:
    """在已开启的写事务内原子占锁（UPSERT），失败则登记争用。

    调用方应已持写事务（BEGIN IMMEDIATE）。ON CONFLICT DO NOTHING 单语句
    完成「无则插、有则判」，消除旧版 SELECT 后再 INSERT 的 check-then-insert
    竞态——两人同窗并发时只有一人能插中，后到者只记争用。

    external_log=False（默认）：争用写在调用方当前事务内（如 /api/locks/acquire
    自行管事务）；external_log=True：争用走独立短连接登记（hold 路径在零超时
    抢锁后需要与业务事务隔离落账）。
    """
    now = datetime.now().isoformat(timespec="seconds")
    conn.execute(
        "INSERT INTO window_locks(lock_key,holder,acquired_at) VALUES (?,?,?)\n"
        "ON CONFLICT(lock_key) DO NOTHING",
        (lock_key, holder, now),
    )
    row = conn.execute(
        "SELECT holder FROM window_locks WHERE lock_key=?", (lock_key,)
    ).fetchone()
    if row is not None and row["holder"] == holder:
        return True
    detail = f"持有者={row['holder'] if row else '未知'}"
    if external_log:
        record_contention(lock_key, holder, detail)
    else:
        conn.execute(
            """INSERT INTO contention_log(lock_key,actor,action,detail,created_at)
               VALUES (?,?,?,?,?)""",
            (lock_key, holder, "争用", detail, now),
        )
    return False


def release(conn, lock_key: str, holder: str) -> None:
    conn.execute(
        "DELETE FROM window_locks WHERE lock_key=? AND holder=?",
        (lock_key, holder),
    )


def require_lock(conn, lock_key: str, holder: str) -> None:
    if not try_acquire(conn, lock_key, holder):
        raise HTTPException(409, "争用：后到只记争用，不写业务")


@router.get("/api/locks")
def list_locks(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM window_locks ORDER BY acquired_at DESC"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/contention")
def list_contention(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM contention_log ORDER BY id DESC LIMIT 100"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.post("/api/locks/acquire")
def acquire_lock(body: LockIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    with connect() as conn:
        ok = try_acquire(conn, body.lock_key, user)
        write_audit(
            conn,
            user,
            "lock_acquire" if ok else "lock_contention",
            body.lock_key,
            {"ok": ok},
        )
        conn.commit()
        if not ok:
            raise HTTPException(409, "争用：后到只记争用")
        return {"ok": True, "lock_key": body.lock_key, "holder": user}


@router.post("/api/locks/release")
def release_lock(body: LockIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    with connect() as conn:
        release(conn, body.lock_key, user)
        write_audit(conn, user, "lock_release", body.lock_key, {})
        conn.commit()
    return {"ok": True}
