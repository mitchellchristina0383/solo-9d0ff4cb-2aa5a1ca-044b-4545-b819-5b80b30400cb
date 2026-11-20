"""端到端自测：逻辑订阅重号冲突恢复演练台。

覆盖场景：
  T1 冲突形成：订阅卡在 23505 重号，两张上游回执都进不了下游，页面数据可见；
  T2 更正补录行：校验非法输入、更正成功后旧核实记录作废；
  T3 过期确认：核实后超过 TTL 再确认 -> 拒绝，订阅表/留存表内容不变；
  T4 核实后更正：核实 -> 更正 -> 确认 -> 哈希不符拒绝，两表内容不变；
  T5 正常确认：核实 -> 确认 -> 单事务转存留存 + 删除原行，进入等待回放；
  T6 回放恢复：轮询到两张上游回执都以 upstream 到达，阶段才变为 recovered；
  T7 重置：可反复演练，新一轮重新形成冲突；
  T8 页面可打开、伪造核实记录的确认被拒绝。

用法（compose 栈起来之后）：
  docker compose exec web python tests/selftest.py
或对任意可达的演练台：
  BASE_URL=http://127.0.0.1:8091 python tests/selftest.py
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request

BASE_URL = os.environ.get("BASE_URL", "http://127.0.0.1:8080").rstrip("/")
# 与服务端 VERIFY_TTL_SECONDS 对齐（本地自测可设短一点，如 4）
TTL = int(os.environ.get("VERIFY_TTL_SECONDS", "60"))
POLL_TIMEOUT_S = 45

FAILURES = []


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE_URL + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode()
            ctype = resp.headers.get("Content-Type", "")
            payload = json.loads(raw) if "application/json" in ctype else raw
            return resp.status, payload
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, raw


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f" -- {detail}" if detail else ""))
    if not cond:
        FAILURES.append(name)


def wait_state(pred, desc):
    deadline = time.time() + POLL_TIMEOUT_S
    last = None
    while time.time() < deadline:
        _, state = call("GET", "/api/state")
        last = state
        if pred(state):
            return state
        time.sleep(1)
    raise AssertionError(f"等待超时：{desc}；最后状态={json.dumps(last, ensure_ascii=False)[:400]}")


def receipt_rows(state):
    return {r["id"]: r for r in state["receipts"]}


def archive_ids(state):
    return {r["id"] for r in state["archive"]}


def fresh_round():
    """重置并等待新一轮冲突形成（阶段切换 + 冲突事件落库）。"""
    code, data = call("POST", "/api/reset")
    check("重置接口返回 200", code == 200 and data.get("ok"), str(data))
    state = wait_state(
        lambda s: s["scenario"]["phase"] == "stuck"
        and any(c["sqlstate"] == "23505" for c in s["conflicts"]),
        "阶段进入 stuck 且探测到 23505 冲突",
    )
    return state


def main():
    # 页面可打开
    code, _ = call("GET", "/")
    check("T8 首页可打开", code == 200)

    # ---------- T1 冲突形成 ----------
    state = fresh_round()
    rows = receipt_rows(state)
    check("T1 阶段=stuck", state["scenario"]["phase"] == "stuck")
    check("T1 期望回放回执为 1000/1001",
          state["scenario"]["expected_ids"] == [1000, 1001])
    check("T1 下游只有人工补录 1001",
          set(rows) == {1001} and rows[1001]["source"] == "manual",
          str([(r["id"], r["source"]) for r in state["receipts"]]))
    check("T1 上游 1000 未进入下游", 1000 not in rows)
    conflicts = state["conflicts"]
    check("T1 探测到冲突事件", len(conflicts) >= 1)
    check("T1 冲突含 23505 重号",
          any(c["sqlstate"] == "23505" for c in conflicts))
    check("T1 冲突关联回执 1001",
          any(c["receipt_id"] == 1001 for c in conflicts))
    check("T1 留存表为空", archive_ids(state) == set())
    check("T1 订阅处于启用状态", state["subscription"]["subenabled"] is True)

    # ---------- T2 更正补录行 ----------
    code, data = call("POST", "/api/receipts/1001/correct",
                      {"order_no": "SO-FIX-1001", "customer": "乙公司（更正）",
                       "amount": "250.00", "note": "值班员更正补录"})
    check("T2 合法更正成功", code == 200 and data.get("ok"), str(data))
    _, state = call("GET", "/api/state")
    rows = receipt_rows(state)
    check("T2 更正后内容已更新",
          rows[1001]["order_no"] == "SO-FIX-1001"
          and rows[1001]["customer"] == "乙公司（更正）"
          and str(rows[1001]["amount"]) == "250.00")
    code, data = call("POST", "/api/receipts/1001/correct",
                      {"order_no": "X", "customer": "Y",
                       "amount": "-1", "note": "n"})
    check("T2 非法金额被拒", code == 400 and not data.get("ok"), str(data))
    code, data = call("POST", "/api/receipts/1001/correct",
                      {"order_no": "X", "customer": "Y",
                       "amount": "abc", "note": "n"})
    check("T2 非数字金额被拒", code == 400 and not data.get("ok"), str(data))

    # ---------- T3 过期确认 ----------
    code, data = call("POST", "/api/receipts/1001/verify",
                      {"operator": "自测-甲"})
    check("T3 核实成功", code == 200 and data.get("ok"), str(data))
    vid = data.get("verification_id")
    _, before = call("GET", "/api/state")
    print(f"    等待核实过期（TTL={TTL}s）…")
    time.sleep(TTL + 2)
    code, data = call("POST", "/api/receipts/1001/confirm",
                      {"verification_id": vid})
    check("T3 过期确认被拒绝", code == 409 and not data.get("ok")
          and "过期" in data.get("error", ""), str(data))
    _, after = call("GET", "/api/state")
    check("T3 拒绝后订阅表内容不变",
          [(r["id"], r["source"], r["order_no"]) for r in before["receipts"]]
          == [(r["id"], r["source"], r["order_no"]) for r in after["receipts"]])
    check("T3 拒绝后留存表仍为空",
          archive_ids(before) == set() and archive_ids(after) == set())

    # ---------- T4 核实后更正 -> 哈希不符 ----------
    code, data = call("POST", "/api/receipts/1001/verify",
                      {"operator": "自测-甲"})
    vid = data.get("verification_id")
    check("T4 重新核实成功", code == 200 and vid is not None)
    code, data = call("POST", "/api/receipts/1001/correct",
                      {"order_no": "SO-FIX-1001-B", "customer": "乙公司（二次更正）",
                       "amount": "251.00", "note": "核实后又被更正"})
    check("T4 更正成功且旧核实时效", code == 200)
    _, before = call("GET", "/api/state")
    code, data = call("POST", "/api/receipts/1001/confirm",
                      {"verification_id": vid})
    check("T4 哈希不符确认被拒绝", code == 409 and not data.get("ok")
          and ("更正" in data.get("error", "") or "不一致" in data.get("error", "")),
          str(data))
    _, after = call("GET", "/api/state")
    check("T4 拒绝后两表保留确认前内容",
          receipt_rows(after)[1001]["order_no"] == "SO-FIX-1001-B"
          and archive_ids(after) == set())
    check("T4 旧核实记录已标记作废",
          any(v["id"] == vid and v["superseded_at"]
              for v in after["verifications"]))

    # 伪造核实记录
    code, data = call("POST", "/api/receipts/1001/confirm",
                      {"verification_id": 999999})
    check("T8 伪造核实记录确认被拒", code == 409 and not data.get("ok"), str(data))

    # ---------- T5 正常确认：单事务转存 + 删除 ----------
    code, data = call("POST", "/api/receipts/1001/verify",
                      {"operator": "自测-甲"})
    vid = data.get("verification_id")
    check("T5 最终核实成功", code == 200 and vid is not None)
    _, before = call("GET", "/api/state")
    code, data = call("POST", "/api/receipts/1001/confirm",
                      {"verification_id": vid})
    check("T5 确认成功", code == 200 and data.get("ok")
          and data.get("phase") == "waiting_replay", str(data))
    state = wait_state(
        lambda s: s["scenario"]["phase"] == "waiting_replay",
        "阶段进入 waiting_replay",
    )
    check("T5 阶段=等待回放", state["scenario"]["phase"] == "waiting_replay")
    rows = receipt_rows(state)
    check("T5 原补录行已从订阅表删除", 1001 not in rows)
    check("T5 留存表含转存行", archive_ids(state) == {1001})
    archived = state["archive"][0]
    check("T5 留存行保留补录内容与来源标记",
          archived["source"] == "manual"
          and archived["order_no"] == "SO-FIX-1001-B"
          and "转存留存" in (archived["note"] or ""))
    check("T5 核实记录已置 used_at",
          any(v["id"] == vid and v["used_at"] for v in state["verifications"]))
    check("T5 确认前留存表为空（确认动作才产生转存）",
          archive_ids(before) == set())
    # 阶段已推进，处置入口关闭
    code, data = call("POST", "/api/receipts/1001/correct",
                      {"order_no": "X", "customer": "Y",
                       "amount": "1", "note": "n"})
    check("T5 等待回放阶段拒绝再更正", code == 409)

    # ---------- T6 回放恢复：两张回执都到达才报恢复 ----------
    state = wait_state(
        lambda s: s["scenario"]["phase"] == "recovered",
        "阶段进入 recovered",
    )
    rows = receipt_rows(state)
    check("T6 阶段=已恢复", state["scenario"]["phase"] == "recovered")
    check("T6 回执 1000 由订阅回放到达",
          1000 in rows and rows[1000]["source"] == "upstream")
    check("T6 回执 1001 由订阅回放到达（上游原始内容）",
          1001 in rows and rows[1001]["source"] == "upstream"
          and rows[1001]["order_no"] == "SO-2026-1001",
          str(rows.get(1001)))
    check("T6 回放后共两张上游回执",
          sorted(rows) == [1000, 1001]
          and all(r["source"] == "upstream" for r in rows.values()))
    check("T6 留存行仍在留存表", archive_ids(state) == {1001})
    check("T6 冲突事件均标记已解除",
          state["conflicts"] and all(c["resolved_at"] for c in state["conflicts"]))
    check("T6 恢复时间已记录", state["scenario"]["recovered_at"] is not None)

    # ---------- T7 再重置一轮，确认可重复演练 ----------
    state = fresh_round()
    check("T7 新一轮冲突形成",
          state["scenario"]["phase"] == "stuck"
          and receipt_rows(state)[1001]["source"] == "manual"
          and archive_ids(state) == set())

    print()
    if FAILURES:
        print(f"共 {len(FAILURES)} 项失败：{FAILURES}")
        sys.exit(1)
    print("全部自测通过。")


if __name__ == "__main__":
    main()
