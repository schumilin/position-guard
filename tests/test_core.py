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
from guard.config import Config, ConfigStore, Rule, Watch, parse_rules, parse_watch_list  # noqa: E402
from guard.engine import Engine, Position  # noqa: E402
from guard.okx import NetworkError, OkxError, sign  # noqa: E402
from guard.rules import evaluate, partial_size, pick, split_size  # noqa: E402
from guard.series import PriceSeries  # noqa: E402

BTC = "BTC-USDT-SWAP"
ETH = "ETH-USDT-SWAP"


def ms(hms: str) -> int:
    return int(datetime.strptime("2026-10-15 " + hms, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)


def rule(window=3, direction="down", thr=2.0, action="close_all", pct=50.0, rid="r1"):
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


# ---------------------------------------------------------------- 涨跌方向
def series_with(anchor_px, start=0, seconds=400):
    s = PriceSeries()
    for i in range(seconds):
        s.append(start + i * 1000, anchor_px)
    return s


@pytest.mark.parametrize("direction,cur,hit", [
    ("down", 98.0, True), ("down", 98.1, False), ("down", 103.0, False),
    ("up", 102.0, True), ("up", 101.9, False), ("up", 95.0, False),
])
def test_direction_is_price_based(direction, cur, hit):
    ev = evaluate(rule(direction=direction), series_with(100.0), 399_000, cur)
    assert ev.hit is hit and ev.anchor_px == 100.0


def test_price_before_window_is_ignored():
    s = PriceSeries()
    s.append(0, 200.0)
    for i in range(1, 400):
        s.append(i * 1000, 100.0)
    ev = evaluate(rule(), s, 399_000, 99.0)
    assert ev.anchor_px == 100.0 and not ev.hit


def test_pick_heaviest_action():
    s = series_with(100.0)
    evs = [evaluate(rule(action=a, pct=p, rid=a + str(p)), s, 399_000, 90.0)
           for a, p in [("alert", 50), ("close_pct", 30), ("close_pct", 60)]]
    assert pick(evs).rule.close_pct == 60
    evs.append(evaluate(rule(action="close_all", rid="all"), s, 399_000, 90.0))
    assert pick(evs).rule.action == "close_all"


@pytest.mark.parametrize("pos,pct,lot,minsz,expect", [
    ("10", 50, "1", "1", "5"),
    ("1", 50, "1", "1", None),
    ("3", 70, "1", "1", "2"),
    ("0.05", 50, "0.01", "0.01", "0.02"),
    ("2", 99, "1", "1", "1"),
    ("1.0", 95, "0.1", "0.5", None),
])
def test_partial_size(pos, pct, lot, minsz, expect):
    got = partial_size(Decimal(pos), pct, Decimal(lot), Decimal(minsz))
    assert (str(got.normalize()) if got is not None else None) == expect


def test_split_size():
    assert split_size(Decimal("70000"), Decimal("35000"), Decimal("0.01")) == [Decimal("35000"), Decimal("35000")]
    assert split_size(Decimal("5"), Decimal("35000"), Decimal("1")) == [Decimal("5")]


# ---------------------------------------------------------------- 配置校验
def test_watch_requires_price_gate():
    with pytest.raises(ValueError, match="价格条件"):
        parse_watch_list([{"inst_id": BTC, "rules": []}])
    with pytest.raises(ValueError, match="价格"):
        parse_watch_list([{"inst_id": BTC, "gate_side": "below", "gate_px": "", "rules": []}])
    with pytest.raises(ValueError, match="只能添加一次"):
        parse_watch_list([{"inst_id": BTC, "gate_side": "below", "gate_px": 1}] * 2)
    w = parse_watch_list([{"inst_id": "btc-usdt-swap", "gate_side": "above", "gate_px": "90000",
                           "rules": [{"window_min": 5, "direction": "up", "threshold_pct": 1, "action": "alert"}]}])[0]
    assert w.inst_id == BTC and w.gate_ok(90000) and not w.gate_ok(89999)


def test_rule_validation():
    with pytest.raises(ValueError):
        parse_rules([{"window_min": 0, "direction": "down", "threshold_pct": 2, "action": "close_all"}])
    with pytest.raises(ValueError):
        parse_rules([{"window_min": 3, "direction": "adverse", "threshold_pct": 2, "action": "close_all"}])
    with pytest.raises(ValueError):
        parse_rules([{"window_min": 3, "direction": "down", "threshold_pct": 2, "action": "close_pct", "close_pct": 100}])


def test_old_v01_config_is_ignored_safely():
    cfg = Config.from_dict({"env": "demo", "default_rules": [{"window_min": 3}], "overrides": {}, "paused": True})
    assert cfg.watch == [] and cfg.paused is True


def test_sign_known_vector():
    assert sign("secret", "2020-12-08T09:08:57.715ZGET/api/v5/account/balance?ccy=BTC") == \
        "wpDvCwYCprcMQsQkxWJiWy+YADoQE4ep+OEKKLimMoY="  # 用 openssl 独立算出


# ---------------------------------------------------------------- 引擎（假交易所）
class FakeRest:
    """模拟 OKX：追逐委托默认马上成交；fill_chase=False 时永远不成交（考验超时兜底）。"""

    def __init__(self):
        self.calls = []
        self.time_offset_ms = 0
        self.rows = []
        self.fill_chase = True
        self.chase_error = None
        self.chase_state = "live"
        self.fail_close = 0

    def now_ms(self):
        return 0

    def _reduce(self, pos_id, sz):
        for r in self.rows:
            if r["posId"] == pos_id:
                left = abs(Decimal(r["pos"])) - Decimal(sz)
                sign_ = -1 if Decimal(r["pos"]) < 0 else 1
                r["pos"] = str(left * sign_)
        self.rows = [r for r in self.rows if Decimal(r["pos"]) != 0]

    def _pos_id(self, inst_id, side, pos_side):
        for r in self.rows:
            if r["instId"] != inst_id:
                continue
            if pos_side in ("long", "short") and r["posSide"] == pos_side:
                return r["posId"]
            if pos_side == "net":
                long_ = Decimal(r["pos"]) > 0
                if (side == "sell") == long_:
                    return r["posId"]
        return None

    async def place_chase(self, inst_id, td_mode, side, sz, pos_side, net_mode, ratio):
        self.calls.append(("chase", inst_id, side, sz, pos_side, net_mode))
        if self.chase_error:
            raise self.chase_error
        if self.fill_chase:
            self._reduce(self._pos_id(inst_id, side, pos_side), sz)
        return "algo1"

    async def algo_detail(self, algo_id):
        return {"state": self.chase_state}

    async def cancel_algo(self, inst_id, algo_id):
        self.calls.append(("cancel", inst_id))
        return [{}]

    async def close_position(self, inst_id, mgn_mode, pos_side):
        self.calls.append(("market_close", inst_id, mgn_mode, pos_side))
        if self.fail_close:
            self.fail_close -= 1
            raise NetworkError("timeout")
        self.rows = [r for r in self.rows if not (r["instId"] == inst_id and r["posSide"] == pos_side)]
        return [{}]

    async def reduce_market(self, inst_id, td_mode, side, sz, pos_side, net_mode):
        self.calls.append(("market_reduce", inst_id, side, sz))
        self._reduce(self._pos_id(inst_id, side, pos_side), sz)
        return [{"sCode": "0"}]

    async def instrument(self, inst_id):
        return {"lotSz": "0.01", "minSz": "0.01", "maxMktSz": "35000"}

    async def positions(self):
        return [dict(r) for r in self.rows]

    async def algo_pending(self, t):
        return []

    async def mark_candles(self, inst, limit):
        return []


class Clock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


def okx_pos(inst=BTC, pos="1", pos_side="net", pid="p1"):
    return {"instType": "SWAP", "instId": inst, "posId": pid, "posSide": pos_side, "pos": pos,
            "mgnMode": "cross", "lever": "10", "avgPx": "100", "cTime": "0", "closeOrderAlgo": []}


def watch(inst=BTC, side="below", px=1000.0, rules=None):
    return Watch(inst, side, px, rules if rules is not None else [rule()])


async def make_engine(watches, rows):
    store = ConfigStore(path=Path(tempfile.mkdtemp()) / "c.json")
    store.cfg.watch = watches
    store.cfg.chase_timeout_sec = 0.05
    clock = Clock()
    engine = Engine(store, Alerter(), clock=clock)
    engine.chase_poll_sec = 0.01
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
    for px in prices:
        clock.t += 1
        engine.on_price(inst, px, clock.t)
        engine.sample(clock.t)


async def settle(engine):
    for _ in range(5):
        await asyncio.sleep(0)
    while engine.side_tasks:
        await asyncio.gather(*list(engine.side_tasks))


async def test_drop_below_gate_closes_with_chase():
    engine, clock = await make_engine([watch(px=1000)], [okx_pos(pos="1")])
    feed(engine, clock, BTC, [100.0] * 200 + [97.9])
    await settle(engine)
    assert engine.rest.calls == [("chase", BTC, "sell", "1", "net", True)]
    assert "p1" not in engine.positions
    assert any(e["kind"] == "close_ok" and "追逐委托已成交" in e["detail"] for e in engine.events)


async def test_gate_not_met_means_no_close():
    # 价格条件「低于 99」：现价 97.9 在条件内才会触发；这里设成低于 90，97.9 不满足
    engine, clock = await make_engine([watch(px=90)], [okx_pos()])
    feed(engine, clock, BTC, [100.0] * 200 + [97.9])
    await settle(engine)
    assert engine.rest.calls == []
    assert engine.gates[BTC] is False
    assert engine.evals[BTC][0][0].hit  # 规则本身达到了，被价格条件挡住


async def test_gate_above():
    engine, clock = await make_engine([watch(side="above", px=100, rules=[rule(direction="up", thr=1)])],
                                      [okx_pos(pos="-2", pid="s1")])
    feed(engine, clock, BTC, [99.0] * 200 + [100.5])  # 涨 1.5% 且高于 100
    await settle(engine)
    assert engine.rest.calls == [("chase", BTC, "buy", "2", "net", True)]


async def test_unwatched_instrument_is_ignored():
    engine, clock = await make_engine([watch(inst=ETH)], [okx_pos(inst=BTC)])
    feed(engine, clock, BTC, [100.0] * 200 + [50.0])
    await settle(engine)
    assert engine.rest.calls == []


async def test_closes_both_long_and_short_on_instrument():
    rows = [okx_pos(pos="1", pos_side="long", pid="L"), okx_pos(pos="3", pos_side="short", pid="S")]
    engine, clock = await make_engine([watch()], rows)
    engine.net_mode = False
    feed(engine, clock, BTC, [100.0] * 200 + [97.0])
    await settle(engine)
    assert sorted(engine.rest.calls) == sorted([("chase", BTC, "sell", "1", "long", False),
                                                ("chase", BTC, "buy", "3", "short", False)])
    assert not engine.positions


async def test_chase_timeout_falls_back_to_market():
    engine, clock = await make_engine([watch()], [okx_pos(pos="1")])
    engine.rest.fill_chase = False
    feed(engine, clock, BTC, [100.0] * 200 + [97.0])
    await settle(engine)
    kinds = [c[0] for c in engine.rest.calls]
    assert kinds == ["chase", "cancel", "market_close"]
    assert any(e["kind"] == "close_ok" and "已改市价" in e["detail"] for e in engine.events)


async def test_chase_canceled_early_falls_back_immediately():
    engine, clock = await make_engine([watch()], [okx_pos(pos="1")])
    engine.store.cfg.chase_timeout_sec = 30  # 很长，但追逐单被撤后应立刻兜底
    engine.rest.fill_chase = False
    engine.rest.chase_state = "canceled"
    feed(engine, clock, BTC, [100.0] * 200 + [97.0])
    await asyncio.wait_for(settle(engine), 2)
    assert [c[0] for c in engine.rest.calls] == ["chase", "cancel", "market_close"]
    assert any("最大追逐距离" in e["detail"] for e in engine.events)


async def test_chase_order_error_falls_back_to_market():
    engine, clock = await make_engine([watch()], [okx_pos(pos="1")])
    engine.rest.chase_error = OkxError("51000", "Parameter error")
    feed(engine, clock, BTC, [100.0] * 200 + [97.0])
    await settle(engine)
    assert [c[0] for c in engine.rest.calls] == ["chase", "market_close"]
    assert any(e["kind"] == "close_ok" for e in engine.events)


async def test_partial_close_with_chase_then_cooldown():
    engine, clock = await make_engine([watch(rules=[rule(window=2, thr=1, action="close_pct", pct=50)])],
                                      [okx_pos(pos="1")])
    feed(engine, clock, BTC, [100.0] * 150 + [98.5])
    await settle(engine)
    assert engine.rest.calls == [("chase", BTC, "sell", "0.5", "net", True)]
    feed(engine, clock, BTC, [97.0] * 60)
    await settle(engine)
    assert len(engine.rest.calls) == 1
    feed(engine, clock, BTC, [97.0] * 125 + [95.0])
    await settle(engine)
    assert len(engine.rest.calls) == 2


async def test_partial_timeout_market_reduces_only_remaining():
    engine, clock = await make_engine([watch(rules=[rule(action="close_pct", pct=50)])], [okx_pos(pos="1")])
    engine.rest.fill_chase = False
    feed(engine, clock, BTC, [100.0] * 200 + [97.0])
    await settle(engine)
    assert engine.rest.calls[-1] == ("market_reduce", BTC, "sell", "0.5")
    assert engine.rest.rows[0]["pos"] == "0.5"


async def test_alert_rule_fires_without_position():
    engine, clock = await make_engine([watch(rules=[rule(action="alert")])], [])
    feed(engine, clock, BTC, [100.0] * 200 + [97.0])
    await settle(engine)
    assert engine.rest.calls == []
    assert any(a["kind"] == "event" for a in engine.alarms.values())
    engine.ack()
    assert not engine.alarms


async def test_close_rule_without_position_does_nothing():
    engine, clock = await make_engine([watch()], [])
    feed(engine, clock, BTC, [100.0] * 200 + [97.0])
    await settle(engine)
    assert engine.rest.calls == [] and not engine.alarms


async def test_paused_does_nothing():
    engine, clock = await make_engine([watch()], [okx_pos()])
    engine.store.cfg.paused = True
    feed(engine, clock, BTC, [100.0] * 200 + [90.0])
    await settle(engine)
    assert engine.rest.calls == []


async def test_disabled_watch_does_nothing():
    w = watch()
    w.enabled = False
    engine, clock = await make_engine([w], [okx_pos()])
    feed(engine, clock, BTC, [100.0] * 200 + [90.0])
    await settle(engine)
    assert engine.rest.calls == []


async def test_stale_price_does_not_trigger():
    engine, clock = await make_engine([watch(rules=[rule(window=1)])], [okx_pos()])
    feed(engine, clock, BTC, [100.0] * 100)
    engine.last_px[BTC] = (90.0, clock.t - 60)
    engine.evaluate_inst(BTC, clock.t)
    await settle(engine)
    assert engine.rest.calls == []


async def test_market_fallback_failure_raises_alarm():
    engine, clock = await make_engine([watch()], [okx_pos()])
    engine.rest.fill_chase = False
    engine.rest.fail_close = 99
    feed(engine, clock, BTC, [100.0] * 200 + [97.0])
    await settle(engine)
    assert any(k.startswith("close_fail") for k in engine.alarms)


async def test_withdraw_permission_is_refused():
    engine, _ = await make_engine([], [])

    class R(FakeRest):
        async def account_config(self):
            return {"perm": "read_only,withdraw,trade", "posMode": "net_mode", "label": "x"}

    res = await engine.verify_key({}, rest=R())
    assert not res["ok"] and "提现" in res["fatal"]


async def test_auth_error_is_explained():
    engine, _ = await make_engine([], [])

    class R(FakeRest):
        async def account_config(self):
            raise OkxError("50119", "API key doesn't exist")

    res = await engine.verify_key({}, rest=R())
    assert "14 天" in res["fatal"]
