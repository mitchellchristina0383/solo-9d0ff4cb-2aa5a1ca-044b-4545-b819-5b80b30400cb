"""数据库连接与基础操作。

一个 PostgreSQL 实例内两个库：
  pubdb —— 上游发布库
  subdb —— 下游订阅库（订阅、留存、核实、状态等表都在这里）
"""
import os
import time
from decimal import Decimal, InvalidOperation

import psycopg2
from psycopg2.extras import RealDictCursor

PUB_DB = os.environ.get("PUB_DBNAME", "pubdb")
SUB_DB = os.environ.get("SUB_DBNAME", "subdb")

# 人工补录与上游重号的回执 ID；上游事务还会带一张不冲突的回执
STUCK_TX_IDS = (1000, 1001)
CONFLICT_RECEIPT_ID = 1001
ARCHIVE_NOTE = "值班员核实通过，人工补录行转存留存"


def _base_kwargs():
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", ""),
        "connect_timeout": 10,
    }


def pub_conn():
    return psycopg2.connect(dbname=PUB_DB, **_base_kwargs())


def sub_conn():
    return psycopg2.connect(dbname=SUB_DB, **_base_kwargs())


def wait_for_databases(timeout_s=60):
    """等待两个库都可连接（compose 健康检查通过后通常立即可用）。"""
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            with pub_conn() as c, c.cursor() as cur:
                cur.execute("SELECT 1")
            with sub_conn() as c, c.cursor() as cur:
                cur.execute("SELECT 1")
            return
        except psycopg2.Error as exc:
            last_err = exc
            time.sleep(0.5)
    raise RuntimeError(f"数据库在 {timeout_s}s 内未就绪：{last_err}")


def log_action(cur, action, detail):
    cur.execute(
        "INSERT INTO action_log (action, detail) VALUES (%s, %s)",
        (action, detail),
    )


def parse_amount(raw):
    """金额校验：非空、非负、最多两位小数。"""
    if raw is None or str(raw).strip() == "":
        raise ValueError("金额不能为空")
    try:
        amount = Decimal(str(raw).strip())
    except InvalidOperation:
        raise ValueError("金额不是合法数字")
    if amount < 0:
        raise ValueError("金额不能为负")
    if -amount.as_tuple().exponent > 2:
        raise ValueError("金额最多保留两位小数")
    return amount


def require_text(raw, field_name, max_len=200):
    if raw is None or str(raw).strip() == "":
        raise ValueError(f"{field_name}不能为空")
    value = str(raw).strip()
    if len(value) > max_len:
        raise ValueError(f"{field_name}过长（最多 {max_len} 字）")
    return value


def receipt_hash(row):
    """核实内容哈希：覆盖补录行所有可更正字段。

    核实之后只要有人更正过其中任一字段，哈希就不再匹配，确认被拒绝。
    """
    import hashlib

    payload = "|".join(
        [
            str(row["id"]),
            str(row["order_no"]),
            str(row["customer"]),
            str(row["amount"]),
            str(row["shipped_at"]),
            str(row.get("note") or ""),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def get_scenario(cur):
    cur.execute("SELECT * FROM scenario_state WHERE id = 1")
    return cur.fetchone()


def _insert_conflict_locked():
    """无条件注入冲突数据。调用方必须保证订阅已禁用、表已清空。

    顺序：先写下游人工补录行，再写上游一笔两回执事务，
    最后把场景置为 stuck。此时没有 apply worker 在跑，不会被抢放。
    """
    with sub_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO receipts
                (id, order_no, customer, amount, shipped_at, source, note)
            VALUES (%s, %s, %s, %s, now(), 'manual', %s)
            """,
            (
                CONFLICT_RECEIPT_ID,
                "SO-MANUAL-1001",
                "下游临时客户（待核实）",
                Decimal("199.00"),
                "下游未收到上游数据，值班员人工补录",
            ),
        )
        log_action(cur, "manual_insert", f"下游人工补录回执 id={CONFLICT_RECEIPT_ID}")
        conn.commit()

    # 上游一笔事务新增两张回执（其中 1001 与补录行重号）。
    # psycopg2 连接默认自动开启事务，不能再显式发 BEGIN/COMMIT，
    # 否则会出现 25001/25P01 警告；用 conn.commit() 控制提交即可。
    with pub_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO receipts (id, order_no, customer, amount, shipped_at)
            VALUES
                (1000, 'SO-2026-1000', '甲公司', 1200.00, now()),
                (1001, 'SO-2026-1001', '乙公司',  240.50, now())
            """
        )
        conn.commit()

    with sub_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            UPDATE scenario_state
               SET phase = 'stuck',
                   expected_ids = %s,
                   conflict_id = %s,
                   updated_at = now()
             WHERE id = 1
            """,
            (list(STUCK_TX_IDS), CONFLICT_RECEIPT_ID),
        )
        log_action(
            cur,
            "upstream_tx",
            f"上游事务新增回执 id={STUCK_TX_IDS[0]},{STUCK_TX_IDS[1]}，"
            f"订阅在 id={CONFLICT_RECEIPT_ID} 重号处卡住",
        )
        conn.commit()


def seed_conflict_scenario():
    """启动时注入演示场景（带守卫，幂等）。

    仅在场景处于 initializing 且订阅表为空时注入；
    对已在进行中的轮次（stuck/waiting_replay/recovered）是 no-op，
    避免应用重启时往回放中的库里重新插补录行。
    """
    with sub_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 自愈：初始化脚本若曾中途失败，状态行可能缺失；只补缺、不覆盖已有状态
        cur.execute(
            """
            INSERT INTO scenario_state (id, phase)
            VALUES (1, 'initializing')
            ON CONFLICT (id) DO NOTHING
            """
        )
        st = get_scenario(cur)
        if st is None or st["phase"] != "initializing":
            return False

        cur.execute("SELECT count(*) AS n FROM receipts")
        if cur.fetchone()["n"] > 0:
            return False

    _insert_conflict_locked()
    return True


def _wait_apply_worker_gone(cur, timeout_s=15):
    """等待该订阅的 apply worker 进程完全退出，避免重置期间偷跑回放。"""
    import time as _time

    deadline = _time.time() + timeout_s
    while _time.time() < deadline:
        cur.execute(
            """
            SELECT count(*) AS n FROM pg_stat_activity
             WHERE backend_type = 'logical replication worker'
            """
        )
        if cur.fetchone()["n"] == 0:
            return
        _time.sleep(0.2)
    raise RuntimeError("等待 apply worker 退出超时，重置中止")


def reset_scenario():
    """重置到下一轮演示：停 worker、清数据、重建复制槽、重新注入冲突。

    关键顺序（每一步都为避免 apply worker 抢跑）：
      1) 禁用订阅并等 apply worker 完全退出；
      2) 清下游、清上游；
      3) 删除旧复制槽并在发布库重建 —— 新槽的起始位点是当前最新 WAL，
         之后注入的事务对它一定是“未来”的变更，必然被解码回放，
         比 pg_replication_slot_advance 对活跃槽的尽力推进更确定；
      4) 注入冲突（补录行 + 上游事务），此刻没有 worker 在跑；
      5) 最后重新启用订阅，worker 启动后必然撞上重号。
    """
    with sub_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT scenario_no FROM scenario_state WHERE id = 1"
        )
        prev = cur.fetchone()
        next_no = (prev["scenario_no"] if prev else 0) + 1

        cur.execute("ALTER SUBSCRIPTION sub_receipts DISABLE")
        conn.commit()
        _wait_apply_worker_gone(cur)

        cur.execute(
            "TRUNCATE receipts, receipts_archive, apply_conflicts, "
            "verifications, action_log RESTART IDENTITY"
        )
        cur.execute(
            """
            UPDATE scenario_state
               SET phase = 'initializing',
                   expected_ids = '{}',
                   conflict_id = NULL,
                   recovered_at = NULL,
                   scenario_no = %s,
                   updated_at = now()
             WHERE id = 1
            """,
            (next_no,),
        )
        log_action(cur, "reset", f"重置演示环境，进入第 {next_no} 轮")
        conn.commit()

    # 复制槽的删除/重建各自要求干净的事务边界（建槽不能与写操作同事务），
    # 因此每步用独立连接、独立提交。
    with pub_conn() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE receipts RESTART IDENTITY")
        conn.commit()

    with pub_conn() as conn, conn.cursor() as cur:
        # 订阅已禁用、worker 已退出，槽不再活跃：删除旧槽
        cur.execute(
            "SELECT pg_drop_replication_slot('sub_receipts_slot') "
            "WHERE EXISTS (SELECT 1 FROM pg_replication_slots "
            "              WHERE slot_name = 'sub_receipts_slot' AND NOT active)"
        )
        conn.commit()

    with pub_conn() as conn, conn.cursor() as cur:
        # 新槽 confirmed_flush 从当前 WAL 位置开始，只看此后的新事务
        cur.execute(
            "SELECT pg_create_logical_replication_slot('sub_receipts_slot', 'pgoutput')"
        )
        conn.commit()

    # 注入冲突（订阅仍禁用，worker 不会抢跑；内部先补录后写上游事务）
    _insert_conflict_locked()

    # 一切就绪后才启用订阅，worker 启动后必然在 1001 撞重号
    with sub_conn() as conn, conn.cursor() as cur:
        cur.execute("ALTER SUBSCRIPTION sub_receipts ENABLE")
        conn.commit()
    return next_no
