"""监控引擎。

数据流：
  公共 WS mark-price（只订阅盯盘标的）─> last_px ─(每秒采样)─> PriceSeries
                                         └─(每个新价格 + 每秒)─> 价格条件 + 规则判断 ─> 平仓 / 报警
  私有 WS positions + REST 每 15 秒对账 ─> positions

触发 = 标的的价格条件当下满足（标记价高于 / 低于设定价）且 某条规则「N 分钟涨 / 跌 X%」达到。
平仓 = 这个标的上的所有仓位（多空都平），先用追逐委托，超时或被撤单就把剩余部分改市价平。
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections import deque
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

import aiohttp

from . import VERSION
from . import creds as creds_mod
from . import proxy as proxy_mod
from .alerts import SOUND_INFO, Alerter
from .config import ACTIONS, DIRECTIONS, GATE_SIDES, ConfigStore, Rule, Watch
from .okx import NetworkError, OkxError, OkxRest, OkxWs, explain, ssl_context, ws_login_args, ws_urls
from .paths import data_dir
from .rules import Evaluation, evaluate, fmt_size, partial_size, pick, split_size
from .series import PriceSeries

log = logging.getLogger(__name__)

POSITION_GONE = {"51023", "51169"}
CHASE_DONE_STATES = {"canceled", "order_failed", "partially_failed", "effective"}
SIDE_CN = {"long": "多单", "short": "空单"}


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v)) if v not in (None, "") else Decimal(0)
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _int(v) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def fmt_window(m: float) -> str:
    if m < 1:
        return f"{round(m * 60)} 秒"
    return f"{m:g} 分钟"


def fmt_px(p: float) -> str:
    if p >= 1:
        return f"{p:,.4f}".rstrip("0").rstrip(".")
    return f"{p:.8f}".rstrip("0").rstrip(".")


def fmt_clock(ts_ms: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ts_ms / 1000))


def gate_text(w: Watch) -> str:
    return f"标记价{GATE_SIDES[w.gate_side]} {fmt_px(w.gate_px)}"


def rule_text(r: Rule) -> str:
    act = f"平 {r.close_pct:g}%" if r.action == "close_pct" else ACTIONS[r.action]
    return f"{fmt_window(r.window_min)}内{DIRECTIONS[r.direction]} ≥ {r.threshold_pct:g}% → {act}"


@dataclass
class Position:
    pos_id: str
    inst_id: str
    pos_side: str          # long / short / net
    pos: Decimal           # 买卖模式下正数是多、负数是空
    mgn_mode: str
    lever: str
    avg_px: str
    upl: str
    upl_ratio: str
    liq_px: str
    mark_px: str
    c_time: int
    attached_sl: bool

    @classmethod
    def from_okx(cls, d: dict) -> "Position":
        algos = d.get("closeOrderAlgo") or []
        return cls(
            pos_id=d.get("posId") or f"{d['instId']}:{d.get('posSide')}:{d.get('mgnMode')}",
            inst_id=d["instId"],
            pos_side=d.get("posSide") or "net",
            pos=_dec(d.get("pos")),
            mgn_mode=d.get("mgnMode") or "cross",
            lever=str(d.get("lever") or ""),
            avg_px=str(d.get("avgPx") or ""),
            upl=str(d.get("upl") or ""),
            upl_ratio=str(d.get("uplRatio") or ""),
            liq_px=str(d.get("liqPx") or ""),
            mark_px=str(d.get("markPx") or ""),
            c_time=_int(d.get("cTime")),
            attached_sl=any(a.get("slTriggerPx") for a in algos if isinstance(a, dict)),
        )

    @property
    def side(self) -> str:
        if self.pos_side in ("long", "short"):
            return self.pos_side
        return "long" if self.pos > 0 else "short"

    @property
    def size(self) -> Decimal:
        return abs(self.pos)

    @property
    def coin(self) -> str:
        return self.inst_id.split("-")[0]

    @property
    def label(self) -> str:
        return f"{self.coin} {SIDE_CN[self.side]} {self.size.normalize():f} 张"


class Engine:
    def __init__(self, store: ConfigStore, alerter: Alerter, *, clock=time.time) -> None:
        self.store = store
        self.alerter = alerter
        self.clock = clock
        self.chase_poll_sec = 1.0
        self.session: aiohttp.ClientSession | None = None
        self.rest: OkxRest | None = None
        self.pub_ws: OkxWs | None = None
        self.priv_ws: OkxWs | None = None
        self.creds: dict | None = None
        self.tasks: list[asyncio.Task] = []
        self.side_tasks: set[asyncio.Task] = set()
        self.events: deque = deque(maxlen=300)
        self.events_path = data_dir() / "events.jsonl"
        self._proxy_cache: str | None = None
        self._proxy_at = 0.0
        self._load_events()
        self._reset_state()

    # ------------------------------------------------------------------ 状态
    def _reset_state(self) -> None:
        self.positions: dict[str, Position] = {}
        self.series: dict[str, PriceSeries] = {}
        self.last_px: dict[str, tuple[float, float]] = {}
        self.instruments: dict[str, dict] = {}
        self.cooldowns: dict[tuple[str, str], float] = {}
        self.in_flight: set[str] = set()
        self.evals: dict[str, list[tuple[Evaluation, float]]] = {}
        self.gates: dict[str, bool] = {}
        self.algo_sl: dict[str, bool] = {}
        self.checks: dict[str, dict] = {}
        self.alarms: dict[str, dict] = {}
        self.subscribed: dict[str, float] = {}
        self.fatal: str | None = None
        self.ready = False
        self.first_sync_done = False
        self.net_mode = True
        self.acct: dict = {}
        self.rest_fail_since: float | None = None
        self.rest_last_error = ""
        self.started_at = self.clock()
        self.status = "starting"
        self.status_text = "正在启动"
        self.check_done = asyncio.Event() if self._has_loop() else None

    @staticmethod
    def _has_loop() -> bool:
        try:
            asyncio.get_running_loop()
            return True
        except RuntimeError:
            return False

    @property
    def cfg(self):
        return self.store.cfg

    def proxy(self) -> str | None:
        now = time.time()
        if now - self._proxy_at > 5:
            self._proxy_cache = proxy_mod.resolve(self.cfg.proxy_mode, self.cfg.proxy_url)
            self._proxy_at = now
        return self._proxy_cache

    # ------------------------------------------------------------------ 生命周期
    async def start(self) -> None:
        if self.check_done is None:
            self.check_done = asyncio.Event()
        self._proxy_at = 0
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context()), trust_env=False)
        self.creds = creds_mod.load()
        self.rest = OkxRest(self.session, creds=self.creds, proxy_getter=self.proxy)
        self._spawn(self._health_loop())
        if not self.creds:
            self.check_done.set()
            return
        self._spawn(self._bootstrap())

    async def stop(self) -> None:
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks = []
        if self.session is not None:
            await self.session.close()
            self.session = None
        self.pub_ws = self.priv_ws = None

    async def restart(self) -> None:
        await self.stop()
        self.alerter.stop_alarm()
        self._reset_state()
        await self.start()

    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.get_running_loop().create_task(self._guard(coro))
        self.tasks.append(task)
        return task

    def _spawn_side(self, coro) -> None:
        """平仓、推送这类一次性任务，重启时不取消，让它做完。"""
        task = asyncio.get_running_loop().create_task(self._guard(coro))
        self.side_tasks.add(task)
        task.add_done_callback(self.side_tasks.discard)

    @staticmethod
    async def _guard(coro) -> None:
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("background task crashed")

    async def _bootstrap(self) -> None:
        self.event("info", "开始连接 OKX")
        while not await self._self_check():
            self.check_done.set()
            if self.fatal:
                return
            await asyncio.sleep(10)
        self.check_done.set()
        self.ready = True
        self.pub_ws = OkxWs("public", ws_urls("public"), self.session, self.proxy, self._on_public)
        self.priv_ws = OkxWs("private", ws_urls("private"), self.session, self.proxy, self._on_private,
                             login_args=lambda: ws_login_args(self.creds, self.rest.now_ms()))
        await self._reconcile_positions()
        await self.priv_ws.subscribe([{"channel": "positions", "instType": "SWAP"}])
        await self._sync_subscriptions()
        self._spawn(self.pub_ws.run())
        self._spawn(self.priv_ws.run())
        self._spawn(self._sampler_loop())
        self._spawn(self._every(15, self._reconcile_positions))
        self._spawn(self._every(60, self._refresh_algos))
        self._spawn(self._every(600, self._sync_time))
        self.event("info", "监控已启动", self.watch_summary())

    async def _every(self, sec: float, fn) -> None:
        while True:
            await asyncio.sleep(sec)
            try:
                await fn()
            except Exception:
                log.exception("periodic %s failed", fn.__name__)

    # ------------------------------------------------------------------ 自检
    async def _self_check(self) -> bool:
        checks: dict[str, dict] = {}
        self.checks = checks
        px = self.proxy()
        if self.cfg.proxy_mode == "off":
            checks["proxy"] = {"state": "ok", "label": "代理", "msg": "设置为不使用代理"}
        elif self.cfg.proxy_mode == "manual":
            checks["proxy"] = {"state": "ok", "label": "代理", "msg": f"手动代理 {px}"}
        elif px:
            checks["proxy"] = {"state": "ok", "label": "代理", "msg": f"自动读取到系统代理 {px}"}
        else:
            checks["proxy"] = {"state": "warn", "label": "代理",
                               "msg": "没读到系统代理，将直接连接（代理软件用的是 TUN / 增强模式时属于正常）"}
        try:
            offset = await self.rest.sync_time()
        except Exception as e:
            checks["network"] = {"state": "fail", "label": "网络",
                                 "msg": f"连不上 OKX。请确认代理软件开着、节点可用。（{explain(e)}）"}
            return False
        checks["network"] = {"state": "ok", "label": "网络", "msg": "已连通 OKX"}
        sec = abs(offset) / 1000
        checks["time"] = ({"state": "ok", "label": "时间", "msg": f"电脑时间准确（偏差 {sec:.1f} 秒）"} if sec < 5 else
                          {"state": "warn", "label": "时间", "msg": f"电脑时间偏差 {sec:.0f} 秒，程序已自动校正"})
        result = await self.verify_key(self.creds, rest=self.rest)
        checks.update(result["checks"])
        if result["fatal"]:
            self._set_fatal(result["fatal"])
            return False
        if not result["ok"]:
            return False
        self.acct = result["acct"]
        self.net_mode = self.acct.get("posMode") != "long_short_mode"
        return True

    async def verify_key(self, creds: dict, rest: OkxRest | None = None) -> dict:
        """检查一把 Key：能否登录、权限是否恰好是「读取 + 交易」。"""
        own_session = None
        if rest is None:
            own_session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context()), trust_env=False)
            rest = OkxRest(own_session, creds=creds, proxy_getter=self.proxy)
        checks: dict[str, dict] = {}
        out = {"ok": False, "fatal": None, "checks": checks, "acct": {}}
        try:
            if own_session is not None:
                await rest.sync_time()
            acct = await rest.account_config()
        except OkxError as e:
            msg = explain(e)
            checks["key"] = {"state": "fail", "label": "API Key", "msg": msg}
            if e.is_auth:
                out["fatal"] = msg
            return out
        except Exception as e:
            checks["key"] = {"state": "fail", "label": "API Key", "msg": f"验证失败：{explain(e)}"}
            return out
        finally:
            if own_session is not None:
                await own_session.close()
        label = acct.get("label") or "无备注"
        checks["key"] = {"state": "ok", "label": "API Key", "msg": f"Key 有效（备注：{label}）"}
        perms = {p.strip() for p in str(acct.get("perm", "")).split(",") if p.strip()}
        if "withdraw" in perms:
            msg = ("这把 Key 开了「提现」权限，为了安全程序拒绝使用。请在 OKX 删除这把 Key，"
                   "重新创建一把只勾「读取」和「交易」的。")
            checks["perm"] = {"state": "fail", "label": "权限", "msg": msg}
            out["fatal"] = msg
            return out
        if "trade" not in perms:
            msg = "这把 Key 没有「交易」权限，程序无法平仓。请重新创建，勾选「读取」和「交易」。"
            checks["perm"] = {"state": "fail", "label": "权限", "msg": msg}
            out["fatal"] = msg
            return out
        checks["perm"] = {"state": "ok", "label": "权限", "msg": "读取 + 交易，没有提现权限"}
        mode = "开平仓模式（可同时持有多空）" if acct.get("posMode") == "long_short_mode" else "买卖模式（单向持仓）"
        checks["mode"] = {"state": "ok", "label": "持仓模式", "msg": mode}
        if str(acct.get("acctLv")) == "1":
            checks["acct"] = {"state": "warn", "label": "账户模式",
                              "msg": "账户是「现货模式」，不能持有合约。需要在 OKX 里切换账户模式"}
        out.update(ok=True, acct=acct)
        return out

    async def _sync_time(self) -> None:
        try:
            await self.rest.sync_time()
        except Exception as e:
            log.warning("time sync failed: %s", e)

    # ------------------------------------------------------------------ 行情
    def _on_public(self, arg: dict, data: list) -> None:
        if arg.get("channel") != "mark-price":
            return
        now = self.clock()
        for d in data:
            inst = d.get("instId")
            try:
                px = float(d["markPx"])
            except (KeyError, TypeError, ValueError):
                continue
            if inst and px > 0:
                self.on_price(inst, px, now)

    def on_price(self, inst: str, px: float, now: float) -> None:
        self.last_px[inst] = (px, now)
        series = self.series.setdefault(inst, PriceSeries())
        if not len(series):
            series.append(int(now * 1000), px)
        self.evaluate_inst(inst, now)

    def sample(self, now: float) -> None:
        """每秒采样一次：把每个盯盘标的的最新标记价记一个点。"""
        now_ms = int(now * 1000)
        keep_ms = int((max(self.cfg.max_window_min(), 15) + 2) * 60_000)
        for inst, (px, ts) in list(self.last_px.items()):
            series = self.series.setdefault(inst, PriceSeries())
            if now - ts <= self.cfg.stale_sec:
                series.append(now_ms, px)
            series.prune(now_ms - keep_ms)
        for w in self.cfg.watch:
            self.evaluate_inst(w.inst_id, now)

    async def _sampler_loop(self) -> None:
        while True:
            interval = max(0.2, self.cfg.sample_interval_sec)
            await asyncio.sleep(interval - (time.time() % interval))
            try:
                self.sample(self.clock())
            except Exception:
                log.exception("sample failed")

    def on_watch_changed(self) -> None:
        """盯盘列表改了：清掉不再盯的标的的判断结果，新标的开始订阅行情。"""
        watched = {w.inst_id for w in self.cfg.watch}
        for inst in list(self.evals):
            if inst not in watched:
                self.evals.pop(inst, None)
                self.gates.pop(inst, None)
        for key in [k for k in self.alarms if k.startswith("stale:") and k[6:] not in watched]:
            self.alarms.pop(key, None)
        if self._has_loop():
            self._spawn_side(self._sync_subscriptions())

    async def _sync_subscriptions(self) -> None:
        if self.pub_ws is None:
            return
        need = [w.inst_id for w in self.cfg.watch if w.inst_id not in self.subscribed]
        if not need:
            return
        for inst in need:
            self.subscribed[inst] = self.clock()
            self.series.setdefault(inst, PriceSeries())
            self._spawn_side(self._backfill(inst))
        await self.pub_ws.subscribe([{"channel": "mark-price", "instId": i} for i in need])

    async def _backfill(self, inst: str) -> None:
        """刚开始盯一个标的时，用 1 分钟标记价 K 线收盘价补齐过去的数据，让窗口一开始就是满的。"""
        minutes = max(self.cfg.max_window_min(), 15)
        try:
            rows = await self.rest.mark_candles(inst, min(100, int(math.ceil(minutes)) + 2))
        except Exception as e:
            log.warning("backfill %s failed: %s", inst, e)
            return
        points = []
        for row in rows:
            if len(row) >= 6 and str(row[5]) != "1":
                continue  # 还没走完的这一分钟不用
            try:
                end_ms = int(row[0]) + 60_000 - 1 - int(self.rest.time_offset_ms)
                points.append((end_ms, float(row[4])))
            except (TypeError, ValueError, IndexError):
                continue
        n = self.series.setdefault(inst, PriceSeries()).backfill(points)
        log.info("backfill %s: %d points", inst, n)

    # ------------------------------------------------------------------ 判断
    def positions_on(self, inst: str) -> list[Position]:
        return [p for p in self.positions.values() if p.inst_id == inst]

    def evaluate_inst(self, inst: str, now: float) -> None:
        w = self.cfg.watch_for(inst)
        if w is None or not w.enabled:
            self.evals.pop(inst, None)
            self.gates.pop(inst, None)
            return
        last = self.last_px.get(inst)
        series = self.series.get(inst)
        if last is None or series is None:
            return
        px, px_ts = last
        fresh = now - px_ts <= self.cfg.stale_sec
        gate = w.gate_ok(px)
        self.gates[inst] = gate
        now_ms = int(now * 1000)
        results: list[tuple[Evaluation, float]] = []
        hits: list[Evaluation] = []
        for r in w.rules:
            if not r.enabled:
                continue
            ev = evaluate(r, series, now_ms, px)
            if ev is None:
                continue
            cd_until = self.cooldowns.get((inst, r.id), 0.0)
            results.append((ev, cd_until))
            if ev.hit and gate and cd_until <= now:
                hits.append(ev)
        self.evals[inst] = results
        if not hits or not fresh or not self.ready or self.fatal or self.cfg.paused or inst in self.in_flight:
            return
        positions = self.positions_on(inst)
        actionable = [h for h in hits if h.rule.action == "alert" or positions]  # 没持仓时平仓规则无事可做
        if not actionable:
            return
        chosen = pick(actionable)
        for h in actionable:
            self.cooldowns[(inst, h.rule.id)] = now + h.rule.window_min * 60
        self.in_flight.add(inst)
        self._spawn_side(self._execute(w, chosen, positions, now))

    def _describe(self, w: Watch, ev: Evaluation) -> str:
        return (f"{w.coin}：价格条件「{gate_text(w)}」满足；标记价 {fmt_clock(ev.anchor_ts)} 为 {fmt_px(ev.anchor_px)}，"
                f"现在 {fmt_px(ev.cur_px)}，{fmt_window(ev.rule.window_min)}内 {ev.change_pct:+.2f}%。"
                f"触发规则「{rule_text(ev.rule)}」")

    # ------------------------------------------------------------------ 执行
    async def _execute(self, w: Watch, ev: Evaluation, positions: list[Position], now: float) -> None:
        rule = ev.rule
        desc = self._describe(w, ev)
        try:
            if rule.action == "alert":
                self.event("trigger", f"{w.coin} 触发报警规则", desc)
                self._raise_alarm(f"rule:{w.inst_id}:{rule.id}:{int(now)}", f"仓位哨兵：{w.coin} 触发报警", desc, kind="event")
                return
            what = "全平" if rule.action == "close_all" else f"平 {rule.close_pct:g}%"
            names = "、".join(p.label for p in positions)
            self.event("trigger", f"{w.coin} 触发规则，正在用追逐委托{what}：{names}", desc)
            results = await asyncio.gather(*(self._close_one(p, rule) for p in positions))
            lines = [f"{p.label}：{msg}" for p, (_, msg) in zip(positions, results)]
            detail = desc + "\n" + "\n".join(lines)
            if all(ok for ok, _ in results):
                title = f"已自动{what}：{w.coin}"
                self.event("close_ok", title, detail)
                self.alerter.play(SOUND_INFO)
                self.alerter.dialog(f"仓位哨兵：{title}", detail, critical=False)
                self._push(f"{title}\n{detail}")
            else:
                self._raise_alarm(f"close_fail:{w.inst_id}:{int(now)}", f"仓位哨兵：{w.coin} 自动平仓失败，请立刻去 OKX 手动处理",
                                  detail, kind="event")
        except Exception as e:
            log.exception("execute failed")
            self._raise_alarm(f"close_fail:{w.inst_id}:{int(now)}", f"仓位哨兵：处理 {w.coin} 时出错，请去 OKX 检查仓位",
                              f"{desc}\n错误：{explain(e)}", kind="event")
        finally:
            self.in_flight.discard(w.inst_id)
            await self._reconcile_positions()

    async def _close_one(self, p: Position, rule: Rule) -> tuple[bool, str]:
        """平一个仓位：算出要平的张数，交给追逐委托（含市价兜底）。"""
        note = ""
        size = p.size
        if rule.action == "close_pct":
            inst = await self._instrument(p.inst_id)
            part = partial_size(p.size, rule.close_pct, _dec(inst.get("lotSz")), _dec(inst.get("minSz")))
            if part is None:
                note = "按比例算出的张数不够最小下单量（或剩余太少），改为全平；"
            else:
                size = part
        target = p.size - size  # 平完后应剩下的张数，全平就是 0
        ok, msg = await self._chase_close(p, size, target)
        return ok, note + msg

    async def _chase_close(self, p: Position, size: Decimal, target: Decimal) -> tuple[bool, str]:
        cfg = self.cfg
        side = "sell" if p.side == "long" else "buy"
        reason = ""
        algo_id = None
        t0 = time.monotonic()
        try:
            algo_id = await self.rest.place_chase(p.inst_id, p.mgn_mode, side, fmt_size(size), p.pos_side,
                                                  self.net_mode, cfg.chase_max_pct / 100)
        except OkxError as e:
            if e.code in POSITION_GONE:
                return True, "仓位已不存在（可能已经被平掉了）"
            reason = f"追逐委托下单失败（{explain(e)}）"
        except NetworkError as e:
            reason = f"追逐委托下单失败（{explain(e)}）"

        if algo_id:
            reason = f"追逐委托 {cfg.chase_timeout_sec:g} 秒内没有全部成交"
            while time.monotonic() - t0 < cfg.chase_timeout_sec:
                await asyncio.sleep(self.chase_poll_sec)
                now_size = await self._position_size(p.pos_id)
                if now_size is not None and now_size <= target:
                    return True, f"追逐委托已成交（用时 {time.monotonic() - t0:.0f} 秒）"
                try:
                    state = (await self.rest.algo_detail(algo_id)).get("state")
                except Exception:
                    state = None
                if state in CHASE_DONE_STATES:
                    if state != "effective":
                        reason = "追逐委托被撤销（价格偏离超过最大追逐距离）"
                    break
            try:
                await self.rest.cancel_algo(p.inst_id, algo_id)
            except Exception as e:
                log.info("cancel chase %s: %s", algo_id, e)
            await asyncio.sleep(self.chase_poll_sec)

        now_size = await self._position_size(p.pos_id)
        if now_size is None:
            await asyncio.sleep(1)
            now_size = await self._position_size(p.pos_id)
        if now_size is None:
            return False, f"{reason}，并且查不到当前仓位，无法改市价兜底"
        if now_size <= target:
            return True, f"追逐委托已成交（用时 {time.monotonic() - t0:.0f} 秒）"
        remaining = now_size - target
        if target <= 0:
            ok, err = await self._retry(lambda: self.rest.close_position(p.inst_id, p.mgn_mode, p.pos_side))
        else:
            ok, err = await self._market_reduce(p, remaining, target)
        if ok:
            return True, f"{reason}，已改市价平掉剩余 {fmt_size(remaining)} 张"
        return False, f"{reason}，改市价也失败了：{err}"

    async def _market_reduce(self, p: Position, amount: Decimal, final_target: Decimal) -> tuple[bool, str | None]:
        inst = await self._instrument(p.inst_id)
        lot = _dec(inst.get("lotSz"))
        side = "sell" if p.side == "long" else "buy"
        remaining = amount + final_target
        for chunk in split_size(amount, _dec(inst.get("maxMktSz")), lot):
            target = remaining - chunk

            async def done(target=target) -> bool:  # 网络超时后先查一下，避免重复平
                now_size = await self._position_size(p.pos_id)
                return now_size is not None and now_size <= target

            ok, err = await self._retry(
                lambda c=chunk: self.rest.reduce_market(p.inst_id, p.mgn_mode, side, fmt_size(c), p.pos_side, self.net_mode),
                already_done=done)
            if not ok:
                return False, err
            remaining = target
        return True, None

    async def _retry(self, fn, already_done=None, attempts: int = 3) -> tuple[bool, str | None]:
        last = None
        for i in range(attempts):
            try:
                await fn()
                return True, None
            except OkxError as e:
                if e.code in POSITION_GONE:
                    return True, None
                last = explain(e)
                if e.is_auth:
                    break
            except NetworkError as e:
                last = explain(e)
                if already_done is not None:
                    try:
                        if await already_done():
                            return True, None
                    except Exception:
                        pass
            await asyncio.sleep(0.5 * (i + 1))
        return False, last

    async def _position_size(self, pos_id: str) -> Decimal | None:
        try:
            rows = await self.rest.positions()
        except Exception:
            return None
        for d in rows:
            if d.get("posId") == pos_id:
                return abs(_dec(d.get("pos")))
        return Decimal(0)

    async def _instrument(self, inst_id: str) -> dict:
        if inst_id not in self.instruments:
            self.instruments[inst_id] = await self.rest.instrument(inst_id)
        return self.instruments[inst_id]

    # ------------------------------------------------------------------ 仓位
    def _on_private(self, arg: dict, data: list) -> None:
        if arg.get("channel") != "positions":
            return
        for d in data:
            if d.get("instType") != "SWAP" or not d.get("instId"):
                continue
            p = Position.from_okx(d)
            if p.pos == 0:
                self._remove_position(p.pos_id)
            else:
                self._upsert_position(p)

    def _upsert_position(self, p: Position) -> None:
        old = self.positions.get(p.pos_id)
        self.positions[p.pos_id] = p
        if old is None and self.first_sync_done:
            watched = self.cfg.watch_for(p.inst_id) is not None
            self.event("info", f"发现新仓位：{p.label}", "在盯盘列表里，按设置监控" if watched else "不在盯盘列表里，不会自动处理")
            if self._has_loop():
                self._spawn_side(self._refresh_algos())

    def _remove_position(self, pos_id: str) -> None:
        p = self.positions.pop(pos_id, None)
        if p is not None:
            self.event("info", f"仓位已不在：{p.coin} {SIDE_CN[p.side]}", "已平仓或被减到 0")

    async def _reconcile_positions(self) -> bool:
        if self.rest is None or not self.creds:
            return False
        try:
            rows = await self.rest.positions()
        except Exception as e:
            if self.rest_fail_since is None:
                self.rest_fail_since = self.clock()
            self.rest_last_error = explain(e)
            if isinstance(e, OkxError) and e.is_auth:
                self._set_fatal(explain(e))
            return False
        self.rest_fail_since = None
        fresh = {}
        for d in rows:
            if d.get("instType", "SWAP") == "SWAP" and d.get("instId") and _dec(d.get("pos")) != 0:
                p = Position.from_okx(d)
                fresh[p.pos_id] = p
        if not self.first_sync_done:
            self.positions = fresh
            self.first_sync_done = True
            names = "、".join(p.label for p in fresh.values()) or "无"
            self.event("info", f"读取到 {len(fresh)} 个永续合约仓位", names)
            await self._refresh_algos()
        else:
            for pid in [k for k in self.positions if k not in fresh]:
                self._remove_position(pid)
            for p in fresh.values():
                self._upsert_position(p)
        return True

    async def _refresh_algos(self) -> None:
        if self.rest is None or not self.creds:
            return
        try:
            rows = list(await self.rest.algo_pending("conditional,oco"))
            rows += await self.rest.algo_pending("move_order_stop")
        except Exception as e:
            log.warning("algo refresh failed: %s", e)
            return
        result = {}
        for p in self.positions.values():
            close_side = "sell" if p.side == "long" else "buy"
            result[p.pos_id] = any(
                a.get("instId") == p.inst_id and a.get("side") == close_side
                and a.get("posSide") in (p.pos_side, "net", "", None)
                and (a.get("slTriggerPx") or a.get("ordType") == "move_order_stop")
                for a in rows)
        self.algo_sl = result

    # ------------------------------------------------------------------ 健康检查 / 报警
    async def _health_loop(self) -> None:
        while True:
            try:
                self.check_health()
            except Exception:
                log.exception("health check failed")
            await asyncio.sleep(1)

    def check_health(self) -> None:
        now = self.clock()
        stale = self.cfg.stale_sec
        if not self.creds:
            self._set_status("setup", "还没填 API Key，请打开控制面板设置")
            return
        if self.priv_ws is not None and self.priv_ws.auth_error and not self.fatal:
            self._set_fatal(self.priv_ws.auth_error)
        if self.fatal:
            self._set_status("error", self.fatal)
            return
        problems = []
        if not self.ready:
            if now - self.started_at <= stale:
                self._set_status("starting", "正在连接 OKX…")
                return
            problems.append(self.checks.get("network", {}).get("msg") or "一直连不上 OKX")
        else:
            for ws, name in ((self.pub_ws, "行情"), (self.priv_ws, "账户")):
                if ws is not None and not ws.connected and now - ws.changed_at > stale:
                    problems.append(f"{name}连接已中断 {int(now - ws.changed_at)} 秒" + (f"（{ws.last_error}）" if ws.last_error else ""))
            if self.rest_fail_since and now - self.rest_fail_since > 60:
                problems.append(f"查询持仓连续失败 {int(now - self.rest_fail_since)} 秒（{self.rest_last_error}）")
        if problems:
            self._raise_alarm("conn", "仓位哨兵：与 OKX 的连接中断",
                              "\n".join(problems) + "\n断线期间不会自动平仓。请检查代理软件是否开着、网络是否正常。", kind="system")
        else:
            self._resolve_alarm("conn", "与 OKX 的连接已恢复，继续监控")

        insts = {w.inst_id for w in self.cfg.watch if w.enabled}
        if not problems and self.ready:
            for inst in insts:
                last = self.last_px.get(inst)
                since = last[1] if last else self.subscribed.get(inst, now)
                age = now - since
                if age > stale:
                    self._raise_alarm(f"stale:{inst}", f"仓位哨兵：{inst.split('-')[0]} 行情中断",
                                      f"已经 {int(age)} 秒没收到 {inst} 的标记价格，这段时间不会对它做判断。", kind="system")
                else:
                    self._resolve_alarm(f"stale:{inst}", f"{inst.split('-')[0]} 行情已恢复")
        for key in [k for k in self.alarms if k.startswith("stale:")]:
            if key[6:] not in insts or problems:
                self.alarms.pop(key, None)

        if problems:
            self._set_status("down", problems[0])
        elif any(a["kind"] == "system" for a in self.alarms.values()):
            self._set_status("down", next(a["title"] for a in self.alarms.values() if a["kind"] == "system"))
        elif self.cfg.paused:
            self._set_status("paused", "已暂停：只看不动，不会自动平仓")
        elif not self.cfg.watch:
            self._set_status("ok", "已连接 · 还没添加盯盘标的")
        else:
            self._set_status("ok", f"监控中 · 盯 {len(insts)} 个标的")

    def _set_status(self, status: str, text: str) -> None:
        self.status, self.status_text = status, text

    def _set_fatal(self, msg: str) -> None:
        if self.fatal:
            return
        self.fatal = msg
        self._raise_alarm("fatal", "仓位哨兵已停止工作", f"{msg}\n请打开控制面板 → 设置，重新填写 API Key。", kind="system")

    def _raise_alarm(self, key: str, title: str, msg: str, *, kind: str) -> None:
        if key in self.alarms:
            self.alarms[key]["msg"] = msg
            return
        self.alarms[key] = {"key": key, "title": title, "msg": msg, "kind": kind,
                            "since": int(self.clock() * 1000), "acked": False}
        self.event("alarm", title, msg)
        self.alerter.start_alarm()
        self.alerter.dialog(title, msg, critical=True)
        self._push(f"{title}\n{msg}")

    def _resolve_alarm(self, key: str, note: str | None = None) -> None:
        alarm = self.alarms.pop(key, None)
        if alarm is None:
            return
        if note:
            self.event("info", note)
            self.alerter.notify("仓位哨兵", note)
            self._push(note)
        if not any(not a["acked"] for a in self.alarms.values()):
            self.alerter.stop_alarm()

    def set_paused(self, paused: bool) -> None:
        if paused == self.cfg.paused:
            return
        self.store.update(paused=paused)
        self.event("config", "已暂停自动平仓（只看不动）" if paused else "已恢复自动平仓")
        self.check_health()

    def ack(self) -> None:
        """「知道了」：停止响铃；一次性的报警清掉，持续性的（断线等）保留到恢复为止。"""
        for key, alarm in list(self.alarms.items()):
            if alarm["kind"] == "event":
                del self.alarms[key]
            else:
                alarm["acked"] = True
        self.alerter.stop_alarm()

    def _push(self, text: str) -> None:
        url = self.cfg.feishu_webhook
        if url and self._has_loop():
            self._spawn_side(self._push_async(url, text))

    async def _push_async(self, url: str, text: str) -> None:
        body = {"msg_type": "text", "content": {"text": f"【仓位哨兵】{text}"}}
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context())) as s:
                async with s.post(url, json=body, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    await resp.read()
        except Exception as e:
            log.warning("feishu push failed: %s", e)

    # ------------------------------------------------------------------ 事件记录
    def _load_events(self) -> None:
        try:
            lines = self.events_path.read_text("utf-8").splitlines()[-300:]
        except OSError:
            return
        for line in lines:
            try:
                self.events.append(json.loads(line))
            except ValueError:
                pass

    def event(self, kind: str, title: str, detail: str = "") -> None:
        e = {"ts": int(self.clock() * 1000), "kind": kind, "title": title, "detail": detail}
        self.events.append(e)
        log.info("[%s] %s %s", kind, title, detail.replace("\n", " | "))
        try:
            if self.events_path.exists() and self.events_path.stat().st_size > 5_000_000:
                self.events_path.replace(self.events_path.with_suffix(".jsonl.1"))
            with open(self.events_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        except OSError:
            pass

    def watch_summary(self) -> str:
        if not self.cfg.watch:
            return "还没有添加盯盘标的，目前什么都不会触发"
        parts = []
        for w in self.cfg.watch:
            rules = "；".join(rule_text(r) for r in w.rules if r.enabled) or "没有规则"
            parts.append(f"{w.coin}{'' if w.enabled else '（已停用）'}：{gate_text(w)} 时 —— {rules}")
        return "\n".join(parts)

    # ------------------------------------------------------------------ 给面板用
    def _position_view(self, p: Position) -> dict:
        return {
            "pos_id": p.pos_id, "inst_id": p.inst_id, "coin": p.coin, "side": p.side,
            "size": f"{p.size.normalize():f}", "mgn_mode": p.mgn_mode, "lever": p.lever,
            "avg_px": p.avg_px, "upl": p.upl, "upl_ratio": p.upl_ratio, "liq_px": p.liq_px, "mark_px": p.mark_px,
            "has_native_sl": p.attached_sl or self.algo_sl.get(p.pos_id, False),
        }

    def snapshot(self) -> dict:
        now = self.clock()
        now_ms = int(now * 1000)
        cfg = self.cfg
        watch = []
        for w in cfg.watch:
            last = self.last_px.get(w.inst_id)
            series = self.series.get(w.inst_id)
            first = series.first_ts() if series is not None else None
            evals = []
            for ev, cd in self.evals.get(w.inst_id, []):
                evals.append({
                    "rule_id": ev.rule.id, "text": rule_text(ev.rule), "window_min": ev.rule.window_min,
                    "direction": ev.rule.direction, "threshold_pct": ev.rule.threshold_pct,
                    "action": ev.rule.action, "close_pct": ev.rule.close_pct,
                    "target_ts": ev.target_ts, "anchor_ts": ev.anchor_ts, "anchor_px": ev.anchor_px,
                    "change_pct": ev.change_pct, "move_pct": ev.move_pct, "hit": ev.hit,
                    "cooldown_until": int(cd * 1000) if cd > now else 0,
                    "short_window": first is not None and first > ev.target_ts,
                })
            watch.append({
                "inst_id": w.inst_id, "coin": w.coin, "enabled": w.enabled,
                "gate_side": w.gate_side, "gate_px": w.gate_px, "gate_text": gate_text(w),
                "gate_ok": self.gates.get(w.inst_id), "mark_px": last[0] if last else None,
                "price_age": (now - last[1]) if last else None,
                "history_sec": (now_ms - first) / 1000 if first else 0,
                "rules_count": sum(1 for r in w.rules if r.enabled), "in_flight": w.inst_id in self.in_flight,
                "positions": [self._position_view(p) for p in self.positions_on(w.inst_id)], "evals": evals,
            })
        watched = {w.inst_id for w in cfg.watch}
        others = [self._position_view(p) for p in sorted(self.positions.values(), key=lambda x: x.inst_id)
                  if p.inst_id not in watched]
        conn = {}
        for ws, name in ((self.pub_ws, "public"), (self.priv_ws, "private")):
            conn[name] = {"connected": bool(ws and ws.connected), "error": ws.last_error if ws else ""}
        return {
            "version": VERSION, "now": now_ms, "status": self.status, "status_text": self.status_text,
            "paused": cfg.paused, "ready": self.ready, "fatal": self.fatal, "has_creds": bool(self.creds),
            "checks": list(self.checks.values()),
            "alarms": sorted(self.alarms.values(), key=lambda a: a["since"], reverse=True),
            "sounding": self.alerter.sounding, "watch": watch, "others": others, "conn": conn,
            "proxy": {"mode": cfg.proxy_mode, "effective": self.proxy(), "system": proxy_mod.system_proxy()},
            "events": list(self.events)[-200:][::-1],
            "config": {
                "proxy_mode": cfg.proxy_mode, "proxy_url": cfg.proxy_url, "feishu_webhook": cfg.feishu_webhook,
                "autostart": cfg.autostart, "paused": cfg.paused,
                "chase_timeout_sec": cfg.chase_timeout_sec, "chase_max_pct": cfg.chase_max_pct,
                "watch": [{"inst_id": w.inst_id, "gate_side": w.gate_side, "gate_px": w.gate_px, "enabled": w.enabled,
                           "rules": [r.__dict__ for r in w.rules]} for w in cfg.watch],
            },
        }
