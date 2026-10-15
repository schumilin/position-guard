import asyncio
import os
import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

os.environ["PG_DATA_DIR"] = tempfile.mkdtemp()
os.environ["PG_NO_OS_ALERTS"] = "1"

from guard.alerts import Alerter  # noqa: E402
from guard.config import ConfigStore, Rule, parse_rules  # noqa: E402
from guard.engine import Engine, Position  # noqa: E402
from guard.okx import NetworkError, OkxError, sign  # noqa: E402
from guard.rules import evaluate, partial_size, pick, split_size  # noqa: E402
from guard.series import PriceSeries  # noqa: E402


def ms(hms: str) -> int:
    return int(datetime.strptime("2026-10-15 " + hms, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)


def rule(window=3, direction="adverse", thr=2.0, action="close_all", pct=50.0, rid="r1"):
    return Rule(rid, window, direction, thr, action, pct, True)


# ---------------------------------------------------------------- 锚点：朋友给的例子
def test_anchor_matches_spec_example():
    s = PriceSeries()
    for t in range(ms("12:00:00"), ms("12:05:21"), 1000):
        s.append(t, float(t))
    # 现在 12:05:20，3 分钟窗口，锚点时刻 12:02:20，取时间 ≥ 它且最近的点
    assert s.anchor(ms("12:05:20") - 180_000) == (ms("12:02:20"), float(ms("12:02:20")))


def test_anchor_takes_next_sample_when_exact_second_missing():
    s = PriceSeries()
    for t in range(ms("12:00:00"), ms("12:05:21"), 1000):
        if t != ms("12:02:20"):
            s.append(t, 1.0)
    assert s.anchor(ms("12:02:20"))[0] == ms("12:02:21")


def test_anchor_none_when_no_sample_after_target():
    s = PriceSeries()
    s.append(1000, 1.0)
    assert s.anchor(2000) is None


def test_prune_and_backfill():
    s = PriceSeries()
    for t in range(10_000, 20_000, 1000):
        s.append(t, 2.0)
    s.prune(15_000)
    assert s.first_ts() == 15_000
    assert s.backfill([(5_000, 1.0), (14_000, 1.5), (16_000, 9.9)]) == 2
    assert s.anchor(0) == (5_000, 1.0)
    assert s.anchor(14_500) == (15_000, 2.0)


# ---------------------------------------------------------------- 方向与阈值
def series_with(anchor_px, start=0, seconds=400):
    s = PriceSeries()
    for i in range(seconds):
        s.append(start + i * 1000, anchor_px)
    return s


@pytest.mark.parametrize("side,direction,cur,hit", [
    ("long", "adverse", 98.0, True),     # 多单跌 2% = 不利
    ("long", "adverse", 98.1, False),
    ("long", "favorable", 102.0, True),  # 多单涨 2% = 有利
    ("short", "adverse", 102.0, True),   # 空单涨 2% = 不利
    ("short", "adverse", 98.0, False),
    ("short", "favorable", 98.0, True),  # 空单跌 2% = 有利
])
def test_direction(side, direction, cur, hit):
    s = series_with(100.0)
    ev = evaluate(rule(direction=direction), side, s, 399_000, cur)
    assert ev.hit is hit
    assert ev.anchor_px == 100.0


def test_price_before_window_is_ignored():
    s = PriceSeries()
    s.append(0, 200.0)               # 很早以前的高价，不在 3 分钟窗口里
    for i in range(1, 400):
        s.append(i * 1000, 100.0)
    ev = evaluate(rule(), "long", s, 399_000, 99.0)
    assert ev.anchor_px == 100.0 and not ev.hit


def test_pick_heaviest_action():
    s = series_with(100.0)
    evs = [evaluate(rule(action=a, pct=p, rid=a + str(p)), "long", s, 399_000, 90.0)
           for a, p in [("alert", 50), ("close_pct", 30), ("close_pct", 60)]]
    assert pick(evs).rule.close_pct == 60
    evs.append(evaluate(rule(action="close_all", rid="all"), "long", s, 399_000, 90.0))
    assert pick(evs).rule.action == "close_all"


@pytest.mark.parametrize("pos,pct,lot,minsz,expect", [
    ("10", 50, "1", "1", "5"),
    ("1", 50, "1", "1", None),          # 凑不出 1 张 → 全平
    ("3", 70, "1", "1", "2"),
    ("0.05", 50, "0.01", "0.01", "0.02"),
    ("2", 99, "1", "1", "1"),
    ("1.0", 95, "0.1", "0.5", None),    # 剩余不够最小张数 → 全平
])
def test_partial_size(pos, pct, lot, minsz, expect):
    got = partial_size(Decimal(pos), pct, Decimal(lot), Decimal(minsz))
    assert (str(got.normalize()) if got is not None else None) == expect


def test_split_size():
    assert split_size(Decimal("70000"), Decimal("35000"), Decimal("0.01")) == [Decimal("35000"), Decimal("35000")]
    assert split_size(Decimal("5"), Decimal("35000"), Decimal("1")) == [Decimal("5")]


def test_rule_validation():
    with pytest.raises(ValueError):
        parse_rules([{"window_min": 0, "direction": "adverse", "threshold_pct": 2, "action": "close_all"}])
    with pytest.raises(ValueError):
        parse_rules([{"window_min": 3, "direction": "up", "threshold_pct": 2, "action": "close_all"}])
    with pytest.raises(ValueError):
        parse_rules([{"window_min": 3, "direction": "adverse", "threshold_pct": 2, "action": "close_pct", "close_pct": 100}])
    rules = parse_rules([{"window_min": "5", "direction": "favorable", "threshold_pct": "1.5", "action": "alert"}])
    assert rules[0].window_min == 5 and rules[0].threshold_pct == 1.5


def test_sign_known_vector():
    # HMAC-SHA256 + Base64，与 OKX 文档描述一致
    assert sign("secret", "2020-12-08T09:08:57.715ZGET/api/v5/account/balance?ccy=BTC") == \
        "wpDvCwYCprcMQsQkxWJiWy+YADoQE4ep+OEKKLimMoY="  # 用 openssl 独立算出


# ---------------------------------------------------------------- 引擎（假交易所）
class FakeRest:
    def __init__(self):
        self.calls = []
        self.time_offset_ms = 0
        self.fail_close = 0
        self.rows = []

    def now_ms(self):
        return 0

    async def close_position(self, inst_id, mgn_mode, pos_side):
        self.calls.append(("close", inst_id, mgn_mode, pos_side))
        if self.fail_close:
            self.fail_close -= 1
            raise NetworkError("timeout")
        self.rows = [r for r in self.rows if r["instId"] != inst_id]
        return [{}]

    async def reduce_market(self, inst_id, td_mode, side, sz, pos_side, net_mode):
        self.calls.append(("reduce", inst_id, side, sz, pos_side, net_mode))
        return [{"sCode": "0"}]

    async def instrument(self, inst_id):
        return {"lotSz": "0.01", "minSz": "0.01", "maxMktSz": "35000"}

    async def positions(self):
        return self.rows

    async def algo_pending(self, t):
        return []

    async def mark_candles(self, inst, limit):
        return []


class Clock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


def okx_pos(inst="BTC-USDT-SWAP", pos="1", pos_side="net", pid="p1"):
    return {"instType": "SWAP", "instId": inst, "posId": pid, "posSide": pos_side, "pos": pos,
            "mgnMode": "cross", "lever": "10", "avgPx": "100", "cTime": "0", "closeOrderAlgo": []}


async def make_engine(rules, rows):
    store = ConfigStore(path=Path(tempfile.mkdtemp()) / "c.json")
    store.cfg.default_rules = rules
    clock = Clock()
    engine = Engine(store, Alerter(), clock=clock)
    engine.rest = FakeRest()
    engine.rest.rows = rows
    engine.creds = {"api_key": "k", "secret_key": "s", "passphrase": "p"}
    engine.ready = True
    engine.first_sync_done = True
    for r in rows:
        p = Position.from_okx(r)
        engine.positions[p.pos_id] = p
    return engine, clock


def feed(engine, clock, inst, prices):
    """每秒一个价格：先推行情再采样，和真实运行一致。"""
    for px in prices:
        clock.t += 1
        engine.on_price(inst, px, clock.t)
        engine.sample(clock.t)


async def settle(engine):
    for _ in range(5):
        await asyncio.sleep(0)
    if engine.side_tasks:
        await asyncio.gather(*engine.side_tasks)


async def test_long_drop_triggers_close_once():
    engine, clock = await make_engine([rule(window=3, thr=2)], [okx_pos(pos="1")])
    feed(engine, clock, "BTC-USDT-SWAP", [100.0] * 200)
    assert engine.rest.calls == []
    feed(engine, clock, "BTC-USDT-SWAP", [97.9])
    await settle(engine)
    assert engine.rest.calls == [("close", "BTC-USDT-SWAP", "cross", "net")]
    assert "p1" not in engine.positions
    feed(engine, clock, "BTC-USDT-SWAP", [97.0] * 5)
    await settle(engine)
    assert len(engine.rest.calls) == 1
    assert any(e["kind"] == "close_ok" for e in engine.events)


async def test_short_not_closed_on_drop_with_adverse_rule():
    engine, clock = await make_engine([rule(window=3, thr=2)], [okx_pos(pos="-1")])
    feed(engine, clock, "BTC-USDT-SWAP", [100.0] * 200 + [97.0])
    await settle(engine)
    assert engine.rest.calls == []


async def test_long_short_mode_passes_pos_side():
    engine, clock = await make_engine([rule(window=1, thr=1)], [okx_pos(pos="2", pos_side="short", pid="s1")])
    engine.net_mode = False
    feed(engine, clock, "BTC-USDT-SWAP", [100.0] * 90 + [101.5])
    await settle(engine)
    assert engine.rest.calls == [("close", "BTC-USDT-SWAP", "cross", "short")]


async def test_paused_does_nothing():
    engine, clock = await make_engine([rule()], [okx_pos()])
    engine.store.cfg.paused = True
    feed(engine, clock, "BTC-USDT-SWAP", [100.0] * 200 + [90.0])
    await settle(engine)
    assert engine.rest.calls == []
    assert engine.evals["p1"][0][0].hit  # 面板上仍显示已达到


async def test_partial_close_then_cooldown():
    rows = [okx_pos(pos="1")]
    engine, clock = await make_engine([rule(window=2, thr=1, action="close_pct", pct=50)], rows)
    feed(engine, clock, "BTC-USDT-SWAP", [100.0] * 150 + [98.5])
    await settle(engine)
    assert engine.rest.calls == [("reduce", "BTC-USDT-SWAP", "sell", "0.5", "net", True)]
    # 冷却 2 分钟内同一条规则不再触发，即使跌得更多
    feed(engine, clock, "BTC-USDT-SWAP", [97.0] * 60)
    await settle(engine)
    assert len(engine.rest.calls) == 1
    # 冷却结束后，新的一轮跌幅再次触发
    feed(engine, clock, "BTC-USDT-SWAP", [97.0] * 125 + [95.0])
    await settle(engine)
    assert len(engine.rest.calls) == 2


async def test_alert_only_raises_alarm_without_trading():
    engine, clock = await make_engine([rule(action="alert")], [okx_pos()])
    feed(engine, clock, "BTC-USDT-SWAP", [100.0] * 200 + [97.0])
    await settle(engine)
    assert engine.rest.calls == []
    assert any(a["kind"] == "event" for a in engine.alarms.values())
    engine.ack()
    assert not engine.alarms


async def test_close_retry_then_success():
    engine, clock = await make_engine([rule()], [okx_pos()])
    engine.rest.fail_close = 2
    feed(engine, clock, "BTC-USDT-SWAP", [100.0] * 200 + [97.0])
    await settle(engine)
    assert len(engine.rest.calls) == 3
    assert any(e["kind"] == "close_ok" for e in engine.events)


async def test_close_failure_raises_alarm():
    engine, clock = await make_engine([rule()], [okx_pos()])
    engine.rest.fail_close = 99
    feed(engine, clock, "BTC-USDT-SWAP", [100.0] * 200 + [97.0])
    await settle(engine)
    assert any(k.startswith("close_fail") for k in engine.alarms)


async def test_stale_price_does_not_trigger():
    engine, clock = await make_engine([rule(window=1)], [okx_pos()])
    feed(engine, clock, "BTC-USDT-SWAP", [100.0] * 100)
    engine.last_px["BTC-USDT-SWAP"] = (90.0, clock.t - 60)  # 最新价已经 60 秒没更新
    engine.evaluate_inst("BTC-USDT-SWAP", clock.t)
    await settle(engine)
    assert engine.rest.calls == []


async def test_override_rules_per_coin():
    rows = [okx_pos(), okx_pos(inst="ETH-USDT-SWAP", pid="p2")]
    engine, clock = await make_engine([rule(thr=2)], rows)
    engine.store.cfg.overrides = {"ETH-USDT-SWAP": []}  # ETH 单独设置为「没有规则」
    for _ in range(200):
        clock.t += 1
        engine.on_price("BTC-USDT-SWAP", 100.0, clock.t)
        engine.on_price("ETH-USDT-SWAP", 100.0, clock.t)
        engine.sample(clock.t)
    clock.t += 1
    engine.on_price("BTC-USDT-SWAP", 97.0, clock.t)
    engine.on_price("ETH-USDT-SWAP", 97.0, clock.t)
    await settle(engine)
    assert [c[1] for c in engine.rest.calls] == ["BTC-USDT-SWAP"]


async def test_withdraw_permission_is_refused():
    engine, _ = await make_engine([], [])

    class R(FakeRest):
        async def account_config(self):
            return {"perm": "read_only,withdraw,trade", "posMode": "net_mode", "label": "x"}

    res = await engine.verify_key("demo", {}, rest=R())
    assert not res["ok"] and "提现" in res["fatal"]


async def test_auth_error_is_explained():
    engine, _ = await make_engine([], [])

    class R(FakeRest):
        async def account_config(self):
            raise OkxError("50119", "API key doesn't exist")

    res = await engine.verify_key("live", {}, rest=R())
    assert "14 天" in res["fatal"]
