#!/usr/bin/env bash
# 仅在数据卷首次初始化时由 docker-entrypoint-initdb.d 执行一次。
# 同一个 PostgreSQL 实例内建立两个库：
#   pubdb —— 上游发布库（业务系统，发货回执由这里产生）
#   subdb —— 下游订阅库（本地数仓，有逻辑订阅 + 人工补录数据）
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<'SQL'
CREATE DATABASE pubdb;
CREATE DATABASE subdb;
SQL

# ---------- 上游：发布库 ----------
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname pubdb <<'SQL'
CREATE TABLE receipts (
    id           BIGINT PRIMARY KEY,
    order_no     TEXT        NOT NULL,
    customer     TEXT        NOT NULL,
    amount       NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
    shipped_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE PUBLICATION pub_receipts FOR TABLE receipts;
SQL

# ---------- 下游：订阅库 ----------
# 订阅表在前 5 列上与上游表结构完全一致（逻辑复制按列名匹配），
# 额外的 source / note 列是下游本地标记，不会进入发布/订阅数据流。
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname subdb <<'SQL'
CREATE TABLE receipts (
    id           BIGINT PRIMARY KEY,
    order_no     TEXT        NOT NULL,
    customer     TEXT        NOT NULL,
    amount       NUMERIC(12,2) NOT NULL CHECK (amount >= 0),
    shipped_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    source       TEXT        NOT NULL DEFAULT 'upstream',  -- upstream=订阅回放 / manual=人工补录
    note         TEXT
);

-- 留存表：值班员核实通过的人工补录行转存到这里，结构与订阅表一致
CREATE TABLE receipts_archive (LIKE receipts INCLUDING ALL);

-- 演示场景状态（每轮重置只有一行）
CREATE TABLE scenario_state (
    id                  INTEGER PRIMARY KEY DEFAULT 1,
    scenario_no         INTEGER NOT NULL DEFAULT 1,
    phase               TEXT NOT NULL,
    expected_ids        BIGINT[] NOT NULL DEFAULT '{}',
    conflict_id         BIGINT,
    recovered_at        TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT single_row CHECK (id = 1)
);

-- csvlog 解析后落库的订阅写入失败/重号事件（monitor 线程写入）
CREATE TABLE apply_conflicts (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    detected_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    log_time         TIMESTAMPTZ,
    sqlstate         TEXT,
    message          TEXT NOT NULL,
    receipt_id       BIGINT,
    expected_tx_ids  BIGINT[] NOT NULL DEFAULT '{}',
    source           TEXT NOT NULL,          -- log=真实 csvlog / heuristic=启发式兜底
    resolved_at      TIMESTAMPTZ
);

-- 值班员核实记录（带内容哈希与有效期）
CREATE TABLE verifications (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    receipt_id    BIGINT NOT NULL,
    operator      TEXT NOT NULL,
    content_hash  TEXT NOT NULL,
    ttl_seconds   INTEGER NOT NULL,
    verified_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ NOT NULL,
    used_at       TIMESTAMPTZ,
    superseded_at TIMESTAMPTZ
);

-- 网页操作审计日志
CREATE TABLE action_log (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
    action     TEXT NOT NULL,
    detail     TEXT NOT NULL
);

-- 同实例本地连接走 Unix socket（trust），订阅槽与发布在同一实例。
-- 注意：发布库与订阅库在同一个 PostgreSQL 实例时，不能直接
-- CREATE SUBSCRIPTION ... (create_slot=true, enabled=true)——
-- 建槽的 walsender 要等建订阅事务提交，而建订阅事务在等 walsender，形成自锁。
-- 标准两步法：先建“禁用且不建槽”的订阅，再手工建槽，最后启用。
CREATE SUBSCRIPTION sub_receipts
    CONNECTION 'host=/var/run/postgresql dbname=pubdb user=postgres'
    PUBLICATION pub_receipts
    WITH (copy_data = false, create_slot = false, enabled = false,
          slot_name = 'sub_receipts_slot');
SQL

# 到发布库手工创建逻辑槽（插件 pgoutput），再回订阅库启用订阅
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname pubdb <<'SQL'
SELECT pg_create_logical_replication_slot('sub_receipts_slot', 'pgoutput');
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname subdb <<'SQL'
ALTER SUBSCRIPTION sub_receipts ENABLE;

INSERT INTO scenario_state (id, phase) VALUES (1, 'initializing');
SQL
