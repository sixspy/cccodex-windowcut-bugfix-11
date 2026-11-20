"""BUGFIX-11 暂存可见性 / 同窗争用：先红后绿，本地 SQLite 夹具。

纪律：
1. 先复现「写入回执成功但列表恒空」——直读实表证明确已落库；
2. 暂存不可见失败测、双人同时暂存争用失败测先红；
3. 两红之后才许改 hold_svc / audit_svc / locks；
4. 正确性口径走暂存可见册择定的单一路径，读写同源，禁止双源混用。
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import app, init_db
import db as db_mod
import hold_svc

PACT_PATH = (
    Path(__file__).resolve().parents[1] / "backend" / "hold_visibility_pact.json"
)


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("db.DB_PATH", tmp_path / "hold_visibility.db")
    init_db()
    yield


def _client_auth(username: str, password: str):
    c = TestClient(app)
    tok = c.post(
        "/api/login", json={"username": username, "password": password}
    ).json()["token"]
    return c, {"Authorization": "Bearer " + tok}


def _hold_payload(biz_key: str) -> dict:
    return {
        "window_name": "W-1102",
        "biz_key": biz_key,
        "event_ts": "2026-11-02T15:00:00",
        "amount": 3.5,
    }


def test_reproduce_write_succeeds_but_list_is_always_empty():
    """复现缺陷：回执成功 + 实表有行，列表却恒空（暂存页与回执互撕）。"""
    c, h = _client_auth("admin", "123456")
    r = c.post("/api/holds", headers=h, json=_hold_payload("REPRO-1"))
    assert r.status_code == 200, r.text
    hid = r.json()["hold_id"]

    # 直读实表：写确实落库了，问题在列表过滤/可见性，不是没写进去
    with db_mod.connect() as conn:
        row = conn.execute(
            "SELECT * FROM hold_staging WHERE id=?", (hid,)
        ).fetchone()
    assert row is not None, "前置：写入必须已落 hold_staging 实表"
    assert row["biz_key"] == "REPRO-1"

    # 缺陷点：同库同表，列表接口却恒空
    items = c.get("/api/holds", headers=h).json()["items"]
    assert any(i["id"] == hid for i in items), "写后列表必须立即可见"


def test_hold_visibility_five_place_homomorphism():
    """暂存列表 / 写入回执 / 窗口 / 审计 / 争用五处同态（争用另见专测）。"""
    c, h = _client_auth("admin", "123456")
    r = c.post("/api/holds", headers=h, json=_hold_payload("HOMO-1"))
    assert r.status_code == 200
    receipt = r.json()
    assert receipt["status"] == "暂存中"

    items = c.get("/api/holds", headers=h).json()["items"]
    mine = next((i for i in items if i["id"] == receipt["hold_id"]), None)
    assert mine is not None, "暂存列表与写入回执必须同源对得上"
    assert mine["biz_key"] == "HOMO-1"
    assert mine["status"] == receipt["status"], "列表状态与回执状态同态"
    assert mine["window_name"] == "W-1102", "列表行须带得出窗口名"

    dash = c.get("/api/dashboard", headers=h).json()
    assert dash["holds"] >= 1, "看板暂存计数与实表同态"

    audits = c.get("/api/audit", headers=h).json()["items"]
    assert any(
        a["action"] == "hold_create" and a["target"] == "HOMO-1" for a in audits
    ), "审计流水与暂存写入同事务可见"


def test_two_users_concurrent_same_window_hold_contention():
    """两人同时写同窗暂存：一进一争，后到 409 且争用落账，业务不写。

    create_hold 用零超时抢占式 BEGIN IMMEDIATE：先到者持写锁期间，
    后到者在事务入口立即 SQLITE_BUSY，走争用 409 并独立连接登记。
    """
    c1, h1 = _client_auth("admin", "123456")
    c2, h2 = _client_auth("closer", "close123")
    # 独立 TestClient = 独立连接/门户，贴近两个真实请求并发
    c2 = TestClient(app)
    # 两写线程在发请求前对齐，最大化写窗口重叠；先到者持写锁临界区内，
    # 后到者要么第一步读到锁行、要么零超时抢锁撞 busy，二者皆判争用 409
    barrier = threading.Barrier(2)
    results: list[tuple[str, int]] = []

    def write(client, headers, key):
        barrier.wait(timeout=10)
        resp = client.post("/api/holds", headers=headers, json=_hold_payload(key))
        results.append((key, resp.status_code))

    t1 = threading.Thread(target=write, args=(c1, h1, "CONC-A"))
    t2 = threading.Thread(target=write, args=(c2, h2, "CONC-B"))
    t1.start()
    t2.start()
    t1.join(15)
    t2.join(15)
    assert len(results) == 2

    statuses = sorted(s for _, s in results)
    assert statuses == [200, 409], f"应为一成一争，实际 {results}"

    items = c1.get("/api/holds", headers=h1).json()["items"]
    written = {"CONC-A", "CONC-B"} & {i["biz_key"] for i in items}
    assert len(written) == 1, "败者业务笔不得落暂存"

    contention = c1.get("/api/contention", headers=h1).json()["items"]
    assert any(x["lock_key"] == "hold:W-1102" for x in contention), "后到须记争用"


def test_loser_contention_persists_when_business_write_rejected():
    """确定性变体：锁已被占时，争用登记必须先落库再拒，不许随异常回滚掉。"""
    c_admin, h_admin = _client_auth("admin", "123456")
    c_closer, h_closer = _client_auth("closer", "close123")

    assert (
        c_admin.post(
            "/api/locks/acquire",
            headers=h_admin,
            json={"lock_key": "hold:W-1102"},
        ).status_code
        == 200
    )

    rejected = c_closer.post(
        "/api/holds", headers=h_closer, json=_hold_payload("LOCKED-OUT")
    )
    assert rejected.status_code == 409

    contention = c_admin.get("/api/contention", headers=h_admin).json()["items"]
    assert any(
        x["lock_key"] == "hold:W-1102" and x["actor"] == "closer"
        for x in contention
    ), "争用记录不能被拒写异常回滚"

    items = c_admin.get("/api/holds", headers=h_admin).json()["items"]
    assert all(i["biz_key"] != "LOCKED-OUT" for i in items), "争用败者不写业务"


def test_visibility_pact_register_locks_single_path():
    """暂存可见册须落档：甲路径（实表同读 hold_staging），无影子表。"""
    assert PACT_PATH.is_file(), "须先写入暂存可见册再改代码"
    pact = json.loads(PACT_PATH.read_text(encoding="utf-8"))
    assert pact["path"] == "甲"
    assert pact["write_table"] == "hold_staging"
    assert pact["read_table"] == "hold_staging"
    assert not pact.get("shadow_table"), "甲路径禁止 hold_shadow 双源"


def test_drift_from_visibility_pact_rejects_and_audits(monkeypatch):
    """偏离可见册：拒写并记「可见偏了」，无业务行落库。"""
    c, h = _client_auth("admin", "123456")
    drifted = dict(hold_svc.VISIBILITY_PACT)
    drifted["path"] = "乙"
    monkeypatch.setattr(hold_svc, "VISIBILITY_PACT", drifted)

    r = c.post("/api/holds", headers=h, json=_hold_payload("DRIFT-1"))
    assert r.status_code == 409
    audits = c.get("/api/audit", headers=h).json()["items"]
    assert any(a["action"] == "hold_visibility_drift" for a in audits)
    assert c.get("/api/holds", headers=h).json()["items"] == []
