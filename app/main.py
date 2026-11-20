"""Flask 入口：逻辑订阅冲突恢复演示页面与接口。

环境变量：PGHOST/PGPORT/PGUSER/PGPASSWORD/PUB_DBNAME/SUB_DBNAME/
          PG_LOG_DIR/VERIFY_TTL_SECONDS/PORT
"""
import os

from flask import Flask, jsonify, render_template, request
from psycopg2.extras import RealDictCursor

import db
import monitor
from db import (
    ARCHIVE_NOTE,
    CONFLICT_RECEIPT_ID,
    log_action,
    parse_amount,
    receipt_hash,
    require_text,
    reset_scenario,
    sub_conn,
)

app = Flask(__name__)

VERIFY_TTL_SECONDS = int(os.environ.get("VERIFY_TTL_SECONDS", "60"))

PHASE_LABELS = {
    "initializing": "初始化",
    "stuck": "订阅卡住（重号冲突）",
    "waiting_replay": "等待回放",
    "recovered": "已恢复",
}
# 只有冲突未解除前允许更正/核实/确认
MUTABLE_PHASES = {"stuck"}


def _error(msg, code=400):
    return jsonify({"ok": False, "error": msg}), code


def _fetch_state(cur):
    cur.execute("SELECT * FROM scenario_state WHERE id = 1")
    scenario = cur.fetchone()
    cur.execute(
        "SELECT * FROM receipts ORDER BY id, source"
    )
    receipts = cur.fetchall()
    cur.execute("SELECT * FROM receipts_archive ORDER BY id")
    archive = cur.fetchall()
    cur.execute(
        "SELECT * FROM apply_conflicts ORDER BY id DESC"
    )
    conflicts = cur.fetchall()
    cur.execute(
        "SELECT * FROM verifications ORDER BY id DESC"
    )
    verifications = cur.fetchall()
    cur.execute("SELECT * FROM action_log ORDER BY id DESC LIMIT 30")
    actions = cur.fetchall()
    cur.execute(
        """
        SELECT srsubid, srrelid, srsubstate, srsublsn
          FROM pg_subscription_rel
         ORDER BY srrelid
        """
    )
    sub_rel = cur.fetchall()
    cur.execute(
        """
        SELECT subname, subenabled, subslotname,
               (SELECT confirmed_flush_lsn FROM pg_replication_slots s
                 WHERE s.slot_name = sub.subslotname) AS confirmed_flush_lsn
          FROM pg_subscription sub
        """
    )
    subscription = cur.fetchone()
    return {
        "scenario": scenario,
        "receipts": receipts,
        "archive": archive,
        "conflicts": conflicts,
        "verifications": verifications,
        "actions": actions,
        "subscription_rel": sub_rel,
        "subscription": subscription,
    }


def _jsonable(value):
    """把 Decimal/datetime 等转成 JSON 友好类型。"""
    from decimal import Decimal
    from datetime import datetime, date

    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


@app.get("/")
def index():
    return render_template("index.html", ttl=VERIFY_TTL_SECONDS)


@app.get("/api/state")
def get_state():
    with sub_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        state = _fetch_state(cur)
        state["phase_label"] = PHASE_LABELS.get(
            state["scenario"]["phase"], state["scenario"]["phase"]
        )
        state["ttl_seconds"] = VERIFY_TTL_SECONDS
    return jsonify(_jsonable(state))


def _load_manual_receipt(cur, receipt_id):
    cur.execute("SELECT * FROM receipts WHERE id = %s", (receipt_id,))
    row = cur.fetchone()
    if row is None:
        raise ValueError(f"回执 id={receipt_id} 不存在（可能已转存留存表）")
    if row["source"] != "manual":
        raise ValueError(f"回执 id={receipt_id} 不是人工补录行，不能操作")
    return row


def _assert_mutable(cur):
    scenario = db.get_scenario(cur)
    if scenario["phase"] not in MUTABLE_PHASES:
        raise ValueError(
            f"当前阶段为「{PHASE_LABELS.get(scenario['phase'], scenario['phase'])}」，"
            "冲突已解除或尚未形成，不能再更正/核实/确认"
        )
    return scenario


@app.post("/api/receipts/<int:receipt_id>/correct")
def correct_receipt(receipt_id):
    """更正人工补录行。更正会使该回执此前的核实记录失效（需重新核实）。"""
    body = request.get_json(force=True, silent=True) or {}
    try:
        order_no = require_text(body.get("order_no"), "单号")
        customer = require_text(body.get("customer"), "客户")
        amount = parse_amount(body.get("amount"))
        note = require_text(body.get("note"), "备注", max_len=500)
    except ValueError as exc:
        return _error(str(exc))

    try:
        with sub_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            _assert_mutable(cur)
            row = _load_manual_receipt(cur, receipt_id)
            cur.execute(
                """
                UPDATE receipts
                   SET order_no = %s, customer = %s, amount = %s, note = %s
                 WHERE id = %s AND source = 'manual'
                """,
                (order_no, customer, amount, note, receipt_id),
            )
            # 此前对该行的核实一律作废：确认时哈希也对不上，这里再显式标记
            cur.execute(
                """
                UPDATE verifications
                   SET superseded_at = now()
                 WHERE receipt_id = %s AND superseded_at IS NULL AND used_at IS NULL
                """,
                (receipt_id,),
            )
            log_action(
                cur,
                "correct",
                f"更正人工补录回执 id={receipt_id}："
                f"单号 {row['order_no']}→{order_no}，客户 {row['customer']}→{customer}，"
                f"金额 {row['amount']}→{amount}；此前核实记录已作废",
            )
            conn.commit()
    except ValueError as exc:
        return _error(str(exc), 409)
    return jsonify({"ok": True})


@app.post("/api/receipts/<int:receipt_id>/verify")
def verify_receipt(receipt_id):
    """值班员核实当前补录内容，生成带哈希与有效期的核实记录。"""
    body = request.get_json(force=True, silent=True) or {}
    operator = require_text(body.get("operator") or "值班员", "操作人", max_len=50)
    with sub_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            _assert_mutable(cur)
            row = _load_manual_receipt(cur, receipt_id)
        except ValueError as exc:
            return _error(str(exc), 409)
        digest = receipt_hash(row)
        cur.execute(
            """
            INSERT INTO verifications
                (receipt_id, operator, content_hash, ttl_seconds, expires_at)
            VALUES (%s, %s, %s, %s, now() + make_interval(secs => %s))
            RETURNING id, verified_at, expires_at
            """,
            (receipt_id, operator, digest, VERIFY_TTL_SECONDS,
             VERIFY_TTL_SECONDS),
        )
        rec = cur.fetchone()
        log_action(
            cur,
            "verify",
            f"操作人 {operator} 核实回执 id={receipt_id}，"
            f"核实记录 id={rec['id']}，有效期至 {rec['expires_at'].isoformat()}",
        )
        conn.commit()
    return jsonify({"ok": True, "verification_id": rec["id"]})


@app.post("/api/receipts/<int:receipt_id>/confirm")
def confirm_archive(receipt_id):
    """核实通过后确认：留存表转存 + 删除原行，必须在同一事务内一并提交。

    拒绝条件（全部回滚，两张表保留确认前内容）：
      1. 场景不处于 stuck；
      2. 不存在有效的核实记录（已使用 / 已被更正作废）；
      3. 核实已超过 TTL（过期确认）；
      4. 核实之后补录行被更正过（内容哈希不匹配，需重新核实）。
    """
    body = request.get_json(force=True, silent=True) or {}
    verification_id = body.get("verification_id")
    with sub_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        try:
            _assert_mutable(cur)
            row = _load_manual_receipt(cur, receipt_id)
        except ValueError as exc:
            return _error(str(exc), 409)

        cur.execute(
            """
            SELECT * FROM verifications
             WHERE id = %s AND receipt_id = %s
            """,
            (verification_id, receipt_id),
        )
        verification = cur.fetchone()
        if verification is None:
            conn.rollback()
            return _error("核实记录不存在或不属于该回执，拒绝确认", 409)
        if verification["used_at"] is not None:
            conn.rollback()
            return _error("该核实记录已使用过，拒绝确认", 409)
        if verification["superseded_at"] is not None:
            conn.rollback()
            return _error("核实后补录行已被更正，原核实已作废，请重新核实", 409)
        cur.execute("SELECT now() AS now_ts")
        now_ts = cur.fetchone()["now_ts"]
        if now_ts > verification["expires_at"]:
            conn.rollback()
            return _error(
                f"核实已过期（有效期至 {verification['expires_at'].isoformat()}），"
                "拒绝确认，请重新核实",
                409,
            )
        if receipt_hash(row) != verification["content_hash"]:
            conn.rollback()
            return _error(
                "核实之后补录行被更正过，内容与核实时不一致，拒绝本次确认，"
                "请重新核实（两张表保留确认前的内容）",
                409,
            )

        # 行锁 + 锁内复核：与并发的更正请求串行化，消除 TOCTOU 窗口。
        # 更正请求会先被这把锁挡住；若它已先提交，下面复核哈希会直接拦下。
        cur.execute(
            "SELECT * FROM receipts WHERE id = %s AND source = 'manual' FOR UPDATE",
            (receipt_id,),
        )
        locked_row = cur.fetchone()
        if locked_row is None:
            conn.rollback()
            return _error("补录行已不存在，拒绝确认", 409)
        if receipt_hash(locked_row) != verification["content_hash"]:
            conn.rollback()
            return _error(
                "核实之后补录行被更正过，内容与核实时不一致，拒绝本次确认，"
                "请重新核实（两张表保留确认前的内容）",
                409,
            )

        # —— 以下转存与删除在同一事务内一并提交，任一失败整体回滚 ——
        cur.execute(
            """
            INSERT INTO receipts_archive
                (id, order_no, customer, amount, shipped_at, created_at, source, note)
            SELECT id, order_no, customer, amount, shipped_at, created_at, source, %s
              FROM receipts
             WHERE id = %s AND source = 'manual'
            RETURNING id
            """,
            (ARCHIVE_NOTE, receipt_id),
        )
        inserted = cur.fetchall()
        if len(inserted) != 1:
            conn.rollback()
            return _error("转存留存表失败，拒绝确认", 409)
        cur.execute(
            "DELETE FROM receipts WHERE id = %s AND source = 'manual' RETURNING id",
            (receipt_id,),
        )
        deleted = cur.fetchall()
        if len(deleted) != 1:
            conn.rollback()
            return _error("删除原行失败，拒绝确认", 409)

        cur.execute(
            "UPDATE verifications SET used_at = now() WHERE id = %s",
            (verification["id"],),
        )
        cur.execute(
            """
            UPDATE scenario_state
               SET phase = 'waiting_replay', updated_at = now()
             WHERE id = 1
            """,
        )
        log_action(
            cur,
            "confirm_archive",
            f"确认通过：回执 id={receipt_id} 在同一事务内转存留存表并删除原行；"
            "场景进入「等待回放」，等待原订阅重放整笔上游事务",
        )
        conn.commit()
    return jsonify({"ok": True, "phase": "waiting_replay"})


@app.post("/api/reset")
def reset():
    next_no = reset_scenario()
    return jsonify({"ok": True, "scenario_no": next_no})


@app.get("/api/health")
def health():
    return jsonify({"ok": True})


def bootstrap():
    """启动时等待数据库、注入冲突场景、拉起后台监控。"""
    db.wait_for_databases()
    seeded = db.seed_conflict_scenario()
    # 若重启时场景已在进行中（seed 是 no-op），确保订阅处于启用状态，
    # 否则 waiting_replay 的重放会因应用重启而停摆。
    if not seeded:
        with db.sub_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT subenabled FROM pg_subscription WHERE subname = %s",
                ("sub_receipts",),
            )
            row = cur.fetchone()
            if row is not None and not row[0]:
                cur.execute("ALTER SUBSCRIPTION sub_receipts ENABLE")
                conn.commit()
                print("[web] 检测到订阅被停用，已重新启用", flush=True)
    monitor.start_background()
    print(
        f"[web] 启动完成：冲突回执 id={CONFLICT_RECEIPT_ID}，"
        f"核实 TTL={VERIFY_TTL_SECONDS}s，日志目录={monitor.PG_LOG_DIR}",
        flush=True,
    )


if __name__ == "__main__":
    bootstrap()
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8080")),
        threaded=True,
    )
