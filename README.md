# 逻辑订阅重号冲突 · 恢复演练台

给数据库值班员的一个本地演练工具：演示**逻辑订阅遇到主键重号（23505）卡住**后，
如何通过「核实 → 更正 → 确认转存 → 原订阅自动重放」安全恢复。

用一个 PostgreSQL 实例内的两个库模拟上下游：

| 库 | 角色 | 关键对象 |
| --- | --- | --- |
| `pubdb` | 上游发布库 | 表 `receipts`、发布 `pub_receipts`、逻辑槽 `sub_receipts_slot` |
| `subdb` | 下游订阅库 | 订阅 `sub_receipts`、订阅表 `receipts`、留存表 `receipts_archive`、核实/冲突/审计表 |

## 演示的故事线

1. 下游**人工补录**了一张回执 `id=1001`（`source='manual'`）。
2. 上游随后在**一笔事务里新增两张回执** `1000、1001`。
3. apply worker 回放整笔事务时，在 `1001` 撞主键重号报 `23505`，
   **整笔事务回滚并每 ~5 秒重试一次**——订阅卡在该事务上，两张回执都进不了下游。
4. 值班员在页面上**核实**补录内容（生成带内容哈希与有效期的核实记录），
   必要时先**更正**补录行（更正后旧核实自动作废，须重新核实）。
5. 值班员**确认**：补录行在**同一个数据库事务**内转存留存表 `receipts_archive`
   并从订阅表删除；任一失败整体回滚，两张表保留确认前内容。
6. 冲突清除，**原订阅自动重放整笔上游事务**：页面先显示「等待回放」，
   监测到 `1000、1001` 两张回执都以 `source='upstream'` 到达后才报「已恢复」。

## 快速开始

```bash
docker compose up --build
# 打开 http://127.0.0.1:8080
```

`web` 依赖 `db` 的健康检查，两个库就绪后页面即可用。
端口绑定在回环地址（`127.0.0.1:8080`），本工具无鉴权，请勿暴露到公共网络。

页面每 1.5 秒自动刷新，可看到：阶段横幅、订阅/槽位点状态、订阅写入失败事件、
订阅表与留存表内容、核实记录（有效/过期/作废/已使用）、操作审计。
页面右上角「重置到下一轮」可反复演练。

## 核实与确认的安全规则

- **核实**：对补录行的 `id/单号/客户/金额/发货时间/备注` 计算 SHA-256 哈希落库，
  并记录有效期（默认 `VERIFY_TTL_SECONDS=60` 秒）。
- **确认时的拒绝条件**（全部回滚，不改动任何表）：
  1. 场景不处于「订阅卡住」阶段；
  2. 核实记录不存在、已被使用、或已因更正而作废；
  3. **核实已过期**——必须重新核实；
  4. **核实之后补录行被更正过**——重新计算的哈希与核实时不一致，拒绝并提示重新核实。
- 通过校验后，`INSERT INTO receipts_archive ...` 与 `DELETE FROM receipts ...`
  在同一事务内一次性提交（见 `app/main.py` 的 `/api/receipts/<id>/confirm`）。

## 冲突探测：两条路径

1. **csvlog 真实报错（主路径）**：`db` 容器开启 `logging_collector=csvlog`，
   日志目录通过共享卷挂给 `web`；后台线程每秒解析最新 csvlog，
   识别 logical replication worker 的 ERROR（PG16 中该日志行 `application_name` 为空，
   靠 `context` 里的 `replication origin` 标记识别），抓取 SQLSTATE 与冲突回执 ID。
   同类报错按 `sqlstate + 回执 + 消息指纹`去重，避免 worker 重试刷屏。
2. **启发式兜底**：日志不可用时，若「期望的上游回执缺失 + 下游存在同 ID 人工补录行」，
   推断为重号并落一条 `source='heuristic'` 事件，保证页面始终能呈现冲突。

## 自测

栈起来之后在容器里跑端到端自测：

```bash
docker compose up -d --build
docker compose exec web python tests/selftest.py
```

也可对任意可达的演练台运行（脚本只用标准库）：

```bash
BASE_URL=http://127.0.0.1:8080 VERIFY_TTL_SECONDS=60 python tests/selftest.py
```

覆盖（共 40+ 断言）：

- **T1 冲突形成**：阶段 stuck、23505 重号事件、两张上游回执都未进下游、订阅仍启用；
- **T2 更正补录**：合法更正生效、非法/负数/非数字金额被拒；
- **T3 过期确认**：核实后超过 TTL 再确认被拒绝，订阅表/留存表内容不变；
- **T4 核实后更正**：哈希不符拒绝确认、两表保留确认前内容、旧核实标记作废、伪造核实 ID 被拒；
- **T5 正常确认**：单事务转存+删除、进入等待回放、核实记录置已使用、其他阶段拒绝再更正；
- **T6 回放恢复**：两张上游回执都到达才置 recovered、上游原始内容覆盖、留存行保留、冲突标记解除；
- **T7 重置**：可反复演练，新一轮重新形成冲突。

## 实现要点与坑

- **同实例逻辑订阅的自锁**：发布库与订阅库在同一 PG 实例时，
  不能直接 `CREATE SUBSCRIPTION ... (create_slot=true, enabled=true)`——
  建槽的 walsender 等建订阅事务提交，而建订阅事务在等 walsender，会死锁。
  初始化脚本（`db/initdb/01-init.sh`）采用两步法：先建「禁用且不建槽」的订阅，
  再到发布库 `pg_create_logical_replication_slot`，最后 `ALTER SUBSCRIPTION ... ENABLE`。
- **重置的确定性**：重置时先禁用订阅并等 apply worker 完全退出
  （轮询 `pg_stat_activity`），清两库数据，再**删除并重建逻辑槽**
  （新槽从当前最新 WAL 开始，之后注入的事务必然被解码），
  先注入冲突（补录 + 上游事务），最后才启用订阅——避免 worker 在清表窗口抢跑回放。
  建槽不能与写操作在同一事务，故每步独立连接、独立提交。
- 订阅表比上游表多 `source/note` 两列，逻辑复制按列名匹配，本地列不进数据流。
- `pg_subscription` 在 PG16 的槽名列是 `subslotname`（不是 `slot_name`）。

## 文件结构

```
docker-compose.yml          db + web 两个服务，共享 csvlog 命名卷
db/Dockerfile               postgres:16 基镜像，预置 csvlog 目录权限
db/initdb/01-init.sh        首启建库/建表/建发布/两步法建订阅
app/main.py                 Flask 页面与更正/核实/确认/重置接口
app/db.py                   连接、场景注入、重置（停 worker/重建槽/再注入）
app/monitor.py              csvlog 解析、冲突落库、等待回放→恢复探测
app/templates/index.html    单页界面
tests/selftest.py           端到端自测（标准库实现）
```

## 限制与说明

- 单实例两库是为本地演示简化；真实环境上下游通常是不同实例，
  但冲突现象、23505 报错与处置流程完全一致。
- 数据存于 Docker 命名卷；`docker compose down -v` 可彻底清空重来。
- 监控线程启动时把当前 csvlog 既有行作为基线，只记录本次启动之后的新报错。
- 演示用固定回执 ID（1000/1001）与固定补录内容，重置后重新注入。
