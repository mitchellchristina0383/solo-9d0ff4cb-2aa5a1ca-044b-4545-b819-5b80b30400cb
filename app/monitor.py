"""订阅冲突监控与恢复探测（后台守护线程）。

两条探测路径：
  1. 解析 db 容器共享过来的 csvlog，抓取 logical replication worker 的
     ERROR（23505 重号等真实订阅写入失败）；
  2. 启发式兜底：日志不可用时，从“场景里应到未到的上游回执 + 下游重号行”推断。

恢复探测：确认转存后场景进入 waiting_replay，原订阅自动重放整笔上游事务；
两张上游回执（expected_ids）全部以 source='upstream' 到达才置为 recovered。
"""
import csv
import glob
import os
import re
import threading
import time

import psycopg2
from psycopg2.extras import RealDictCursor

from db import STUCK_TX_IDS, log_action, sub_conn

PG_LOG_DIR = os.environ.get("PG_LOG_DIR", "/var/log/postgresql")
POLL_INTERVAL_S = float(os.environ.get("MONITOR_INTERVAL_S", "1.0"))

# csvlog 23 列（PostgreSQL 14+），按位置解析
CSV_FIELDS = [
    "log_time", "user_name", "database_name", "process_id",
    "connection_from", "session_id", "session_line_num", "command_tag",
    "session_start_time", "virtual_transaction_id", "transaction_id",
    "error_severity", "sqlstate_code", "message", "detail", "hint",
    "internal_query", "internal_query_pos", "context", "query",
    "query_pos", "location", "application_name",
]

# logical replication worker 的报错识别：
# PG16 的 apply worker 日志行 application_name 为空，但 context 含
# “processing remote data for replication origin ...”；
# 部分版本/场景下 application_name 为订阅名（sub_xxx）。
WORKER_APP_RE = re.compile(r"^sub_", re.IGNORECASE)
REPLICATION_ORIGIN_RE = re.compile(r"replication origin|logical replication", re.IGNORECASE)
SQLSTATE_RE = re.compile(r"\b(23505|23P01|40001|40P01|23503|23502|23514)\b")
RECEIPT_ID_RE = re.compile(r'receipts.*?key \(id\)=\((\d+)\)', re.IGNORECASE)
GENERIC_ID_RE = re.compile(r"\(id\)=\((\d+)\)")


def _list_csv_logs():
    paths = sorted(
        glob.glob(os.path.join(PG_LOG_DIR, "*.csv")),
        key=lambda p: os.path.getmtime(p),
    )
    return paths


def _parse_log_rows(path):
    """读出一个 csvlog 文件里的全部日志行（csv 模块自带续行处理）。"""
    rows = []
    try:
        with open(path, newline="", encoding="utf-8", errors="replace") as fh:
            for raw in csv.reader(fh):
                if len(raw) < len(CSV_FIELDS):
                    continue
                row = dict(zip(CSV_FIELDS, raw[: len(CSV_FIELDS)]))
                rows.append(row)
    except FileNotFoundError:
        return []
    except OSError:
        return []
    return rows


def _extract_receipt_id(row):
    for field in ("message", "detail", "hint"):
        text = row.get(field) or ""
        m = RECEIPT_ID_RE.search(text) or GENERIC_ID_RE.search(text)
        if m:
            return int(m.group(1))
    return None


def _is_worker_error(row):
    if (row.get("error_severity") or "").upper() not in {"ERROR", "FATAL"}:
        return False
    app = row.get("application_name") or ""
    if WORKER_APP_RE.match(app):
        return True
    # PG16 apply worker 的应用名为空，靠 context / message 里的复制来源标记识别
    blob = " ".join(
        filter(None, [row.get("context"), row.get("message"), row.get("detail")])
    )
    return bool(REPLICATION_ORIGIN_RE.search(blob))


def scan_log_once(seen_keys):
    """扫一遍最新 csvlog，返回新发现的冲突事件列表。

    seen_keys: 跨轮次保留的去重集合，元素 (log_time, process_id, session_line_num)
    """
    events = []
    paths = _list_csv_logs()
    if not paths:
        return events
    # 只看最新一个日志文件（演示场景跨度短，轮转后旧文件无需再看）
    for row in _parse_log_rows(paths[-1]):
        if not _is_worker_error(row):
            continue
        key = (
            row.get("log_time"),
            row.get("process_id"),
            row.get("session_line_num"),
        )
        if key in seen_keys:
            continue
        seen_keys.add(key)

        text = " ".join(
            filter(None, [row.get("message"), row.get("detail"), row.get("hint")])
        )
        m = SQLSTATE_RE.search(text)
        sqlstate = m.group(1) if m else (row.get("sqlstate_code") or None)
        # 只关心真正会导致事务回滚重试的报错
        if sqlstate and sqlstate not in {
            "23505", "23P01", "40001", "40P01", "23503", "23502", "23514"
        }:
            continue
        events.append(
            {
                "log_time": row.get("log_time"),
                "sqlstate": sqlstate,
                "message": text,
                "receipt_id": _extract_receipt_id(row),
            }
        )
    return events


def _record_conflict(cur, event, expected_ids, source):
    """落库一条冲突。

    同一类报错（sqlstate + 回执 + 消息指纹）只保留一条：
    apply worker 会每 5 秒重试一次，不去重会让事件表无限增长。
    返回 True 表示新插入，False 表示命中已有事件。
    """
    if event.get("log_time"):
        cur.execute(
            """
            SELECT id FROM apply_conflicts
             WHERE sqlstate IS NOT DISTINCT FROM %s
               AND receipt_id IS NOT DISTINCT FROM %s
               AND left(message, 200) = left(%s, 200)
             LIMIT 1
            """,
            (event.get("sqlstate"), event.get("receipt_id"), event["message"][:500]),
        )
        existing = cur.fetchone()
        if existing:
            return False
    cur.execute(
        """
        INSERT INTO apply_conflicts
            (log_time, sqlstate, message, receipt_id, expected_tx_ids, source)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            event.get("log_time"),
            event.get("sqlstate"),
            event["message"][:1000],
            event.get("receipt_id"),
            expected_ids,
            source,
        ),
    )
    rid = event.get("receipt_id")
    label = "重号(23505)" if event.get("sqlstate") == "23505" else "写入失败"
    log_action(
        cur,
        "conflict_detected",
        f"探测到订阅{label}" + (f"，回执 id={rid}" if rid else "") + f"（来源：{source}）",
    )
    return True


def _heuristic_event(cur, scenario):
    """兜底推断：期望的上游回执缺失 + 下游存在同 ID 人工补录行。"""
    expected = list(scenario["expected_ids"] or [])
    if not expected:
        return None
    cur.execute(
        """
        SELECT r.id, r.source
          FROM unnest(%s::bigint[]) AS e(id)
          JOIN receipts r ON r.id = e.id
         WHERE r.source = 'manual'
        """,
        (expected,),
    )
    manual = cur.fetchall()
    if not manual:
        return None
    cur.execute(
        "SELECT 1 FROM apply_conflicts WHERE source = 'heuristic' LIMIT 1"
    )
    if cur.fetchone():
        return None
    rid = manual[0]["id"]
    return {
        "log_time": None,
        "sqlstate": "23505",
        "message": (
            f"启发式推断：订阅事务 {expected} 回放受阻，"
            f"下游已存在人工补录行 id={rid}，主键重号（csvlog 不可用时的兜底判断）"
        ),
        "receipt_id": rid,
    }


def _check_replay_recovered(cur, scenario):
    """waiting_replay 阶段：两张上游回执都以 upstream 到达才算恢复。"""
    if scenario["phase"] != "waiting_replay":
        return
    expected = list(scenario["expected_ids"] or [])
    if not expected:
        return
    cur.execute(
        """
        SELECT count(*) AS n FROM receipts
         WHERE id = ANY(%s) AND source = 'upstream'
        """,
        (expected,),
    )
    arrived = cur.fetchone()["n"]
    if arrived == len(expected):
        cur.execute(
            """
            UPDATE scenario_state
               SET phase = 'recovered',
                   recovered_at = now(),
                   conflict_id = NULL,
                   updated_at = now()
             WHERE id = 1 AND phase = 'waiting_replay'
            """
        )
        cur.execute(
            "UPDATE apply_conflicts SET resolved_at = now() WHERE resolved_at IS NULL"
        )
        log_action(
            cur,
            "recovered",
            f"原订阅重放成功，上游回执 {expected} 全部到达，场景恢复",
        )


def monitor_once(seen_keys):
    """一轮监控：收集冲突 + 探测回放恢复。返回本轮新增冲突数。"""
    new_count = 0
    with sub_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM scenario_state WHERE id = 1")
        scenario = cur.fetchone()
        if scenario is None:
            return 0
        expected = list(scenario["expected_ids"] or [])

        if scenario["phase"] in {"stuck", "initializing"}:
            for event in scan_log_once(seen_keys):
                if _record_conflict(cur, event, expected or list(STUCK_TX_IDS), "log"):
                    new_count += 1
                    if scenario["phase"] == "initializing":
                        cur.execute(
                            "UPDATE scenario_state SET phase = 'stuck', "
                            "conflict_id = COALESCE(%s, conflict_id), updated_at = now() "
                            "WHERE id = 1",
                            (event.get("receipt_id"),),
                        )
            # 兜底：连续探测不到日志但数据形态明确是重号
            if scenario["phase"] == "stuck":
                cur.execute(
                    "SELECT 1 FROM apply_conflicts LIMIT 1"
                )
                if not cur.fetchone():
                    event = _heuristic_event(cur, scenario)
                    if event and _record_conflict(
                        cur, event, expected or list(STUCK_TX_IDS), "heuristic"
                    ):
                        new_count += 1

        cur.execute("SELECT * FROM scenario_state WHERE id = 1")
        scenario = cur.fetchone()
        _check_replay_recovered(cur, scenario)
        conn.commit()
    return new_count


def _run_forever():
    seen = set()
    # 启动基线：把当前 csvlog 里已有的日志行全部标记为“已读”，
    # 只监控应用本次启动之后的新报错，避免把历史轮次的旧错误成批补录。
    for path in _list_csv_logs():
        for row in _parse_log_rows(path):
            seen.add((
                row.get("log_time"),
                row.get("process_id"),
                row.get("session_line_num"),
            ))
    while True:
        try:
            monitor_once(seen)
        except (psycopg2.Error, OSError) as exc:
            # 监控线程不能因瞬时错误退出
            print(f"[monitor] 本轮监控失败：{exc}", flush=True)
        time.sleep(POLL_INTERVAL_S)


def start_background():
    t = threading.Thread(target=_run_forever, name="rep-monitor", daemon=True)
    t.start()
    return t
